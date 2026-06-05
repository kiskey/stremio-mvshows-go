package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/kiskey/stremio-mvshows-go/internal/config"
	"github.com/kiskey/stremio-mvshows-go/internal/database"
	"github.com/kiskey/stremio-mvshows-go/internal/services/debrid"
	"github.com/kiskey/stremio-mvshows-go/internal/services/parser"
	"github.com/kiskey/stremio-mvshows-go/internal/services/tracker"
	"github.com/kiskey/stremio-mvshows-go/internal/utils"
	"gorm.io/gorm/clause"
)

// Global concurrency semaphore: restricts background debrid pollers to maximum 3 concurrent goroutines.
// This completely protects the server IP from ever hammering or throttling the debrid API providers.
var backgroundPollSemaphore = make(chan struct{}, 3)

// ── TMDB Data Structures ──

type tmdbLightData struct {
	Title        string  `json:"title"`
	Name         string  `json:"name"`
	PosterPath   string  `json:"poster_path"`
	Overview     string  `json:"overview"`
	ReleaseDate  string  `json:"release_date"`
	FirstAirDate string  `json:"first_air_date"`
	VoteAverage  float64 `json:"vote_average"`
	Genres       []genre `json:"genres"`
}

type genre struct {
	Name string `json:"name"`
}

// ── Stream Deduplication Key ──

type streamDupKey struct {
	IsRD     string
	Quality  string
	Language string
	Infohash string
}

// ── Quality Sorting ──

// qualityOrder defines stream quality ranking — lower number = higher priority
var qualityOrder = map[string]int{
	"4K":    1,
	"2160p": 1,
	"1080p": 2,
	"720p":  3,
	"480p":  4,
	"SD":    5,
}

// sortStreams replicates Node.js sortStreams: RD before P2P, then by quality, then language.
func sortStreams(streams []gin.H) {
	sort.SliceStable(streams, func(i, j int) bool {
		a, b := streams[i], streams[j]

		// RD streams (have "url" key) come before P2P (have "infoHash" key)
		_, aIsP2P := a["infoHash"].(string)
		_, bIsP2P := b["infoHash"].(string)

		if !aIsP2P && bIsP2P {
			return true // a is RD, b is P2P
		}
		if aIsP2P && !bIsP2P {
			return false // a is P2P, b is RD
		}

		// Compare quality rank
		aQuality, _ := a["_quality"].(string)
		bQuality, _ := b["_quality"].(string)
		if aQuality == "" {
			aQuality = "SD"
		}
		if bQuality == "" {
			bQuality = "SD"
		}
		aQRank := qualityOrder[aQuality]
		bQRank := qualityOrder[bQuality]
		if aQRank == 0 {
			aQRank = 99
		}
		if bQRank == 0 {
			bQRank = 99
		}
		if aQRank != bQRank {
			return aQRank < bQRank
		}

		// Compare language alphabetically
		aLang, _ := a["_language"].(string)
		bLang, _ := b["_language"].(string)
		if aLang == "" {
			aLang = "zz"
		}
		if bLang == "" {
			bLang = "zz"
		}
		return strings.ToLower(aLang) < strings.ToLower(bLang)
	})
}

// dedupeStreams replicates Node.js dedupeStreams — removes duplicate entries
// keyed by (isRD | quality | language | infohash).
func dedupeStreams(streams []gin.H) []gin.H {
	seen := make(map[string]bool)
	out := make([]gin.H, 0, len(streams))
	for _, s := range streams {
		streamType := "p2p"
		if _, hasURL := s["url"]; hasURL {
			streamType = "rd"
		}

		quality, _ := s["_quality"].(string)
		if quality == "" {
			quality = "SD"
		}
		lang, _ := s["_language"].(string)
		if lang == "" {
			lang = "NA"
		}

		hashOrURL := ""
		if h, ok := s["infoHash"].(string); ok {
			hashOrURL = h
		} else if u, ok := s["url"].(string); ok {
			hashOrURL = u
		}

		key := fmt.Sprintf("%s|%s|%s|%s", streamType, quality, strings.ToLower(lang), hashOrURL)
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, s)
	}
	return out
}

// ── Route Registration ──

func RegisterStremioRoutes(r *gin.RouterGroup) {
	r.GET("/manifest.json", manifestHandler)
	r.GET("/catalog/:type/:id/:extra", catalogHandler)
	r.GET("/catalog/:type/:id", catalogHandler)
	r.GET("/meta/:type/:id.json", metaHandler)
	r.GET("/stream/:type/:id.json", streamHandler)
	r.GET("/rd-add/:infohash/:episode", rdAddHandler)
}

// ── Manifest ──

func manifestHandler(c *gin.Context) {
	cfg := config.Load()
	c.JSON(http.StatusOK, gin.H{
		"id":          cfg.AddonID,
		"version":     cfg.AddonVersion,
		"name":        cfg.AddonName,
		"description": cfg.AddonDescription,
		"resources": []interface{}{
			"catalog",
			"meta",
			"stream",
		},
		"types": []string{"series", "movie"},
		"catalogs": []gin.H{
			{
				"type": "series",
				"id":   "tamilmv_series",
				"name": "Tamil WebSeries",
				"extra": []gin.H{
					{"name": "skip", "isRequired": false},
				},
			},
			{
				"type": "movie",
				"id":   "tamilmv_hd_movies",
				"name": "Tamil HD Movies",
				"extra": []gin.H{
					{"name": "skip", "isRequired": false},
				},
			},
			{
				"type": "movie",
				"id":   "tamilmv_dubbed_movies",
				"name": "Tamil HD Dubbed Movies",
				"extra": []gin.H{
					{"name": "skip", "isRequired": false},
				},
			},
		},
		"idPrefixes": []string{"tt"}, // Restored "tt" as the only idPrefix, completely matching the Node.js standard
	})
}

// ── Catalog ──

func catalogHandler(c *gin.Context) {
	mediaType := c.Param("type")
	catalogID := c.Param("id")
	extra := c.Param("extra")

	// Critical Fix: Stremio client automatically appends ".json" to the end of catalog path parameters.
	// We must strip these suffixes to prevent string matching and integer conversion failures.
	catalogID = strings.TrimSuffix(catalogID, ".json")
	extra = strings.TrimSuffix(extra, ".json")

	skip := 0
	// Parse skip parameter from query or path suffix
	if qSkip := c.Query("skip"); qSkip != "" {
		if val, err := strconv.Atoi(qSkip); err == nil {
			skip = val
		}
	} else if strings.Contains(extra, "skip=") {
		parts := strings.Split(extra, "skip=")
		if len(parts) > 1 {
			if val, err := strconv.Atoi(parts[1]); err == nil {
				skip = val
			}
		}
	}

	var threads []database.Thread
	// EXPERT FIX: Retrieve ONLY successfully linked threads that possess a valid IMDb ID (tt...)
	// This completely eliminates custom pending IDs (addonId:pending:...) from catalog pages,
	// preventing layout issues and rendering professional Metahub-aligned cards.
	query := database.DB.Debug().
		Where("status = ? AND type = ? AND tmdb_id IN (SELECT tmdb_id FROM tmdb_metadata WHERE imdb_id IS NOT NULL AND imdb_id != '')", "linked", mediaType)

	// Filter by specific catalogs matching manifest IDs
	if catalogID == "tamilmv_hd_movies" {
		query = query.Where("catalog = ?", "tamil-hd-movies")
	} else if catalogID == "tamilmv_dubbed_movies" {
		query = query.Where("catalog = ?", "tamil-dubbed-movies")
	} else {
		query = query.Where("catalog = ?", "top-series-from-forum")
	}

	err := query.
		Order("CASE status WHEN 'linked' THEN 0 ELSE 1 END ASC, posted_at DESC").
		Offset(skip).
		Limit(100).
		Preload("TmdbMetadata").
		Find(&threads).Error

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database lookup failed"})
		return
	}

	cfg := config.Load()
	metas := make([]gin.H, 0, len(threads))

	for _, t := range threads {
		metaID := ""
		if t.TmdbMetadata != nil && t.TmdbMetadata.ImdbID != nil {
			metaID = *t.TmdbMetadata.ImdbID
		}
		if metaID == "" {
			continue // Defensive: skip any record that doesn't have an IMDb ID
		}

		poster := cfg.PlaceholderPoster
		desc := ""
		title := t.CleanTitle
		if title == "" {
			title = t.RawTitle
		}

		var tmdbData tmdbLightData
		releaseInfo := ""
		var imdbRating *string
		var genresList []string

		if json.Unmarshal([]byte(t.TmdbMetadata.Data), &tmdbData) == nil {
			if tmdbData.PosterPath != "" {
				poster = "https://image.tmdb.org/t/p/w500" + tmdbData.PosterPath
			}
			if tmdbData.Overview != "" {
				desc = tmdbData.Overview
			}

			// Extract release year from TMDB date
			dateStr := tmdbData.ReleaseDate
			if dateStr == "" {
				dateStr = tmdbData.FirstAirDate
			}
			if len(dateStr) >= 4 {
				releaseInfo = dateStr[:4]
			}

			// Extract IMDb rating
			if tmdbData.VoteAverage > 0 {
				rating := fmt.Sprintf("%.1f", tmdbData.VoteAverage)
				imdbRating = &rating
			}

			// Extract genre names
			for _, g := range tmdbData.Genres {
				if g.Name != "" {
					genresList = append(genresList, g.Name)
				}
			}
		}

		if t.CustomPoster != nil && *t.CustomPoster != "" {
			poster = *t.CustomPoster
		}
		if t.CustomDescription != nil && *t.CustomDescription != "" {
			desc = *t.CustomDescription
		}

		// Fallback releaseInfo from Thread.Year
		if releaseInfo == "" && t.Year != nil {
			releaseInfo = strconv.Itoa(*t.Year)
		}

		metaEntry := gin.H{
			"id":          metaID,
			"type":        t.Type,
			"name":        title,
			"poster":      poster,
			"description": desc,
			"releaseInfo": releaseInfo,
		}
		if imdbRating != nil {
			metaEntry["imdbRating"] = *imdbRating
		}
		if len(genresList) > 0 {
			metaEntry["genres"] = genresList
		}

		metas = append(metas, metaEntry)
	}

	c.JSON(http.StatusOK, gin.H{"metas": metas})
}

// ── Meta ──

func metaHandler(c *gin.Context) {
	id := strings.TrimSuffix(c.Param("id"), ".json")
	cfg := config.Load()

	cleanID := id
	if idx := strings.Index(id, ":pending:"); idx != -1 {
		cleanID = id[idx+len(":pending:"):]
	}

	// 1. First attempt to lookup as standard linked metadata by IMDb ID (tt...)
	var meta database.TmdbMetadata
	err := database.DB.Debug().Where("imdb_id = ? OR tmdb_id = ?", cleanID, cleanID).Preload("Threads").First(&meta).Error
	if err == nil {
		var details tmdbLightData
		if errJson := json.Unmarshal([]byte(meta.Data), &details); errJson == nil {
			poster := cfg.PlaceholderPoster
			if details.PosterPath != "" {
				poster = "https://image.tmdb.org/t/p/w500" + details.PosterPath
			}
			overview := details.Overview

			mediaType := "movie"
			if len(meta.Threads) > 0 {
				mediaType = meta.Threads[0].Type
			}

			displayName := details.Title
			if displayName == "" {
				displayName = details.Name
			}

			// Apply custom metadata overrides if defined by the admin
			if len(meta.Threads) > 0 {
				t := meta.Threads[0]
				if t.CustomPoster != nil && *t.CustomPoster != "" {
					poster = *t.CustomPoster
				}
				if t.CustomDescription != nil && *t.CustomDescription != "" {
					overview = *t.CustomDescription
				}
			}

			releaseInfo := ""
			dateStr := details.ReleaseDate
			if dateStr == "" {
				dateStr = details.FirstAirDate
			}
			if len(dateStr) >= 4 {
				releaseInfo = dateStr[:4]
			}
			if releaseInfo == "" && meta.Year != nil {
				releaseInfo = strconv.Itoa(*meta.Year)
			}

			metaObj := gin.H{
				"id":          id,
				"type":        mediaType,
				"name":        displayName,
				"poster":      poster,
				"description": overview,
				"releaseInfo": releaseInfo,
			}

			if details.VoteAverage > 0 {
				metaObj["imdbRating"] = fmt.Sprintf("%.1f", details.VoteAverage)
			}
			if len(details.Genres) > 0 {
				genreNames := make([]string, 0, len(details.Genres))
				for _, g := range details.Genres {
					if g.Name != "" {
						genreNames = append(genreNames, g.Name)
					}
				}
				if len(genreNames) > 0 {
					metaObj["genres"] = genreNames
				}
			}

			if mediaType == "series" {
				// Fetch linked streams to build Stremio series videos navigation
				var streams []database.Stream
				_ = database.DB.Where("tmdb_id = ?", meta.TmdbID).Order("season ASC, episode ASC").Find(&streams)

				videos := make([]gin.H, 0)
				seen := make(map[string]bool)

				for _, s := range streams {
					if s.Season != nil && s.Episode != nil {
						sVal := *s.Season
						eVal := *s.Episode
						endVal := eVal
						if s.EpisodeEnd != nil {
							endVal = *s.EpisodeEnd
						}

						for ep := eVal; ep <= endVal; ep++ {
							vKey := fmt.Sprintf("%d:%d", sVal, ep)
							if seen[vKey] {
								continue
							}
							seen[vKey] = true

							videos = append(videos, gin.H{
								"id":      fmt.Sprintf("%s:%d:%d", id, sVal, ep),
								"season":  sVal,
								"episode": ep,
								"title":   fmt.Sprintf("Season %d - Episode %d", sVal, ep),
							})
						}
					}
				}
				metaObj["videos"] = videos
			}

			c.JSON(http.StatusOK, gin.H{"meta": metaObj})
			return
		}
	}

	// 2. Second attempt: Check if the ID refers to an unlinked, pending ThreadHash (backward-compatibility)
	var t database.Thread
	errThread := database.DB.Debug().Where("thread_hash = ?", cleanID).First(&t).Error
	if errThread == nil {
		metaObj := gin.H{
			"id":          id,
			"type":        t.Type,
			"name":        t.RawTitle,
			"poster":      cfg.PlaceholderPoster,
			"description": "Pending metadata match. You can link this manually in the administration rescue panel.",
		}
		if t.Year != nil {
			metaObj["year"] = *t.Year
		}
		c.JSON(http.StatusOK, gin.H{"meta": metaObj})
		return
	}

	c.JSON(http.StatusNotFound, gin.H{"error": "Metadata not found"})
}

// ── Stream ──

func streamHandler(c *gin.Context) {
	id := strings.TrimSuffix(c.Param("id"), ".json")
	cfg := config.Load()

	// Strip pending prefix robustly if looking up unlinked thread streams
	cleanID := id
	if idx := strings.Index(id, ":pending:"); idx != -1 {
		cleanID = id[idx+len(":pending:"):]
	}

	var baseID string
	season := -1
	episode := -1

	// Smart ID Splitting: Accurately identifies base ID vs Season/Episode parameters.
	parts := strings.Split(cleanID, ":")
	if len(parts) >= 3 {
		s, errS := strconv.Atoi(parts[len(parts)-2])
		e, errE := strconv.Atoi(parts[len(parts)-1])
		if errS == nil && errE == nil {
			season = s
			episode = e
			baseID = strings.Join(parts[:len(parts)-2], ":")
		} else {
			baseID = cleanID
		}
	} else {
		baseID = cleanID
	}

	var meta database.TmdbMetadata
	err := database.DB.Debug().Where("imdb_id = ? OR tmdb_id = ?", baseID, baseID).First(&meta).Error
	if err != nil {
		// If metadata lookup fails, check if the query requested unlinked/pending ThreadHash streams
		var t database.Thread
		errThread := database.DB.Debug().Where("thread_hash = ?", baseID).First(&t).Error
		if errThread == nil {
			// This is an unlinked thread. Return direct P2P fallback streams only (No RD mapping)
			streamList := make([]gin.H, 0)
			seenP2P := make(map[string]bool)

			for _, magnet := range t.MagnetURIs {
				parsedMagnet := parser.ParseMagnet(magnet, t.Type)
				if parsedMagnet == nil {
					continue
				}

				if seenP2P[parsedMagnet.Infohash] {
					continue
				}
				seenP2P[parsedMagnet.Infohash] = true

				label := fmt.Sprintf("[P2P] TamilMV\n%s / %s", parsedMagnet.Quality, parsedMagnet.Language)
				streamList = append(streamList, gin.H{
					"name":     label,
					"title":    t.RawTitle,
					"infoHash": parsedMagnet.Infohash,
					"sources":  tracker.GetTrackers(),
				})
			}
			c.JSON(http.StatusOK, gin.H{"streams": streamList})
			return
		}

		// STREMIO PROTOCOL FIX: Never return 404 on the stream endpoint! 
		c.JSON(http.StatusOK, gin.H{"streams": []interface{}{}})
		return
	}

	var streams []database.Stream
	if season != -1 && episode != -1 {
		// Series path: search direct episode match OR full season packs where episode is NULL
		err = database.DB.Debug().Where("tmdb_id = ? AND ((season = ? AND episode <= ? AND episode_end >= ?) OR (season = ? AND episode IS NULL))",
			meta.TmdbID, season, episode, episode, season).
			Order("quality DESC").
			Find(&streams).Error
	} else {
		// Movie path
		err = database.DB.Debug().Where("tmdb_id = ?", meta.TmdbID).Order("quality DESC").Find(&streams).Error
	}

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Streams lookup failed"})
		return
	}

	p := debrid.GetProvider(cfg)

	// Pre-fetch cache checks for matching infohashes to mark stream availability
	var allHashes []string
	for _, s := range streams {
		allHashes = append(allHashes, s.Infohash)
	}
	cacheMap := debrid.CheckCached(allHashes, database.DB)

	var cachedStreams []gin.H   // Instant ⚡ streams
	var uncachedStreams []gin.H // Downloading ⏳ streams

	seenStreams := make(map[streamDupKey]bool)

	mediaType := "movie"
	if len(meta.Threads) > 0 {
		mediaType = meta.Threads[0].Type
	}

	// Pre-fetch TMDB title for movie streams
	var tmdbTitle string
	if mediaType == "movie" {
		var tmdbData tmdbLightData
		if json.Unmarshal([]byte(meta.Data), &tmdbData) == nil {
			tmdbTitle = tmdbData.Title
			if tmdbTitle == "" {
				tmdbTitle = tmdbData.Name
			}
		}
	}

	for _, s := range streams {
		isCached := cacheMap[s.Infohash]

		// ---- Build title detail based on content type ----
		var titleDetail string
		if mediaType == "series" {
			ep := 0
			if s.Episode != nil {
				ep = *s.Episode
			}
			epEnd := ep
			if s.EpisodeEnd != nil {
				epEnd = *s.EpisodeEnd
			}
			// Season pack: both episode fields are nil
			if s.Episode == nil && s.EpisodeEnd == nil {
				ep = 1
				epEnd = 999
			}
			seasonVal := 0
			if s.Season != nil {
				seasonVal = *s.Season
			}
			titleDetail = buildSeriesTitle(seasonVal, ep, epEnd, s.Quality, s.Language)
		} else {
			titleDetail = buildMovieTitle(tmdbTitle, s.Quality, s.Language)
		}

		// ---- Deduplication check ----
		dupKey := streamDupKey{
			IsRD:     "",
			Quality:  s.Quality,
			Language: s.Language,
			Infohash: s.Infohash,
		}
		if seenStreams[dupKey] {
			continue
		}
		seenStreams[dupKey] = true

		if p.IsEnabled() {
			// ---- Attempt instant playback for already-downloaded torrents ----
			var debridTorrent database.DebridTorrent
			errDebrid := database.DB.Where("infohash = ? AND status = ?", s.Infohash, "downloaded").First(&debridTorrent).Error

			if errDebrid == nil && len(debridTorrent.Files) > 0 && len(debridTorrent.Links) > 0 {
				fileToStream, linkIndex := pickBestDebridFile(debridTorrent.Files, debridTorrent.Links, mediaType, season, episode)
				if fileToStream != nil && linkIndex >= 0 && linkIndex < len(debridTorrent.Links) {
					unrestricted, errUnrestrict := p.UnrestrictLink(c.Request.Context(), debridTorrent.Links[linkIndex])
					if errUnrestrict == nil && unrestricted != nil && unrestricted.Download != "" {
						item := gin.H{
							"name":      fmt.Sprintf("[RD+] %s ⚡", s.Quality),
							"title":     fmt.Sprintf("%s\n%s", titleDetail, fileToStream.Path),
							"url":       unrestricted.Download,
							"_quality":  s.Quality,
							"_language": s.Language,
						}
						cachedStreams = append(cachedStreams, item)
						continue // Skip the /rd-add/ fallback for this stream
					}
				}
			}

			// ---- Standard RD stream (redirects through /rd-add/) ----
			label := fmt.Sprintf("[RD] %s ⏳", s.Quality)
			if isCached {
				label = fmt.Sprintf("[RD+] %s ⚡", s.Quality)
			}

			targetEpStr := "movie"
			if season != -1 && episode != -1 {
				targetEpStr = fmt.Sprintf("%d-%d", season, episode)
			}
			rdUrl := fmt.Sprintf("%s/rd-add/%s/%s", cfg.AppHost, s.Infohash, targetEpStr)

			item := gin.H{
				"name":      label,
				"title":     fmt.Sprintf("%s\nClick to Download", titleDetail),
				"url":       rdUrl,
				"_quality":  s.Quality,
				"_language": s.Language,
			}
			if isCached {
				cachedStreams = append(cachedStreams, item)
			} else {
				uncachedStreams = append(uncachedStreams, item)
			}
		} else {
			// ---- Direct P2P Streams ----
			trackerSources := buildTrackerSources()
			sources := withDhtSource(trackerSources, s.Infohash)
			item := gin.H{
				"name":      fmt.Sprintf("[P2P] %s", s.Quality),
				"title":     titleDetail,
				"infoHash":  s.Infohash,
				"sources":   sources,
				"_quality":  s.Quality,
				"_language": s.Language,
			}
			uncachedStreams = append(uncachedStreams, item)
		}
	}

	// Combine, deduplicate, sort, and strip internal keys
	streamList := append(cachedStreams, uncachedStreams...)
	streamList = dedupeStreams(streamList)
	sortStreams(streamList)

	for _, s := range streamList {
		delete(s, "_quality")
		delete(s, "_language")
	}

	c.JSON(http.StatusOK, gin.H{"streams": streamList})
}

// ── Stream Title Builders ──

func buildSeriesTitle(season, episode, episodeEnd int, quality, language string) string {
	seasonStr := fmt.Sprintf("S%02d", season)

	var epPart string
	if episodeEnd == 0 || episodeEnd == episode {
		epPart = fmt.Sprintf("Episode %02d", episode)
	} else if episode == 1 && episodeEnd == 999 {
		epPart = "Season Pack"
	} else {
		epPart = fmt.Sprintf("Episodes %02d-%02d", episode, episodeEnd)
	}

	langPart := ""
	if language != "" {
		langPart = " | " + language
	}

	q := quality
	if q == "" {
		q = "SD"
	}

	return fmt.Sprintf("%s | %s%s\n%s", seasonStr, epPart, langPart, q)
}

func buildMovieTitle(tmdbTitle, quality, language string) string {
	langPart := ""
	if language != "" {
		langPart = " | " + language
	}
	q := quality
	if q == "" {
		q = "SD"
	}
	return fmt.Sprintf("%s%s\n%s", tmdbTitle, langPart, q)
}

// ── Debrid File Selection ──

func pickBestDebridFile(files database.JSONFileList, links []string, mediaType string, season, episode int) (*database.TorrentFile, int) {
	if len(files) == 0 || len(links) == 0 {
		return nil, -1
	}

	// Collect selected files with their original indices
	selectedFiles := make([]struct {
		file  database.TorrentFile
		index int
	}, 0)
	for i, f := range files {
		if f.Selected == 1 {
			selectedFiles = append(selectedFiles, struct {
				file  database.TorrentFile
				index int
			}{file: f, index: i})
		}
	}
	if len(selectedFiles) == 0 {
		return nil, -1
	}

	isVideo := func(path string) bool {
		p := strings.ToLower(path)
		return strings.HasSuffix(p, ".mkv") ||
			strings.HasSuffix(p, ".mp4") ||
			strings.HasSuffix(p, ".avi") ||
			strings.HasSuffix(p, ".mov") ||
			strings.HasSuffix(p, ".m4v")
	}

	// For series: try to match requested episode in file paths
	if mediaType == "series" && episode > 0 {
		epRegex := regexp.MustCompile(`[Ss]\d{1,2}\s*[Ee]\s*(\d{1,3})`)
		for _, sf := range selectedFiles {
			if !isVideo(sf.file.Path) {
				continue
			}
			matches := epRegex.FindStringSubmatch(sf.file.Path)
			if len(matches) > 1 {
				epNum, _ := strconv.Atoi(matches[1])
				if epNum == episode {
					linkIndex := 0
					for j := 0; j <= sf.index; j++ {
						if files[j].Selected == 1 {
							if j == sf.index {
								return &sf.file, linkIndex
							}
							linkIndex++
						}
					}
				}
			}
		}
	}

	// Fallback: largest video file
	var largest *database.TorrentFile
	largestIdx := -1
	for _, sf := range selectedFiles {
		if !isVideo(sf.file.Path) {
			continue
		}
		if largest == nil || sf.file.Bytes > largest.Bytes {
			largest = &sf.file
			largestIdx = sf.index
		}
	}

	if largest == nil {
		// No video files — pick largest selected file overall
		for _, sf := range selectedFiles {
			if largest == nil || sf.file.Bytes > largest.Bytes {
				largest = &sf.file
				largestIdx = sf.index
			}
		}
	}

	if largest == nil {
		return nil, -1
	}

	// Compute link index (position among selected files)
	linkIndex := 0
	for j := 0; j <= largestIdx; j++ {
		if files[j].Selected == 1 {
			if j == largestIdx {
				return largest, linkIndex
			}
			linkIndex++
		}
	}

	return largest, linkIndex
}

// ── rdAddHandler ──

func rdAddHandler(c *gin.Context) {
	infohash := strings.ToLower(c.Param("infohash"))
	episodeParam := c.Param("episode")

	season := -1
	episode := -1
	isMovie := true

	if episodeParam != "movie" {
		parts := strings.Split(episodeParam, "-")
		if len(parts) == 2 {
			season, _ = strconv.Atoi(parts[0])
			episode, _ = strconv.Atoi(parts[1])
			isMovie = false
		}
	}

	// Resolve the original magnet from cache to submit to debrid
	var cache database.MagnetCache
	err := database.DB.Where("infohash = ?", infohash).First(&cache).Error
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Magnet not found in local cache"})
		return
	}

	cfg := config.Load()
	p := debrid.GetProvider(cfg)
	if !p.IsEnabled() {
		c.JSON(http.StatusBadRequest, gin.H{"error": "No active debrid service configured"})
		return
	}

	reqCtx := c.Request.Context()

	// Check if already completely downloaded and saved locally in DebridTorrent table
	var torrentRecord database.DebridTorrent
	errRecord := database.DB.Where("infohash = ?", infohash).First(&torrentRecord).Error
	if errRecord == nil && torrentRecord.Status == "downloaded" {
		// Update LastChecked timestamp to indicate active access hit (extending cache TTL)
		database.DB.Model(&torrentRecord).Update("last_checked", time.Now())

		dlLink, errCachedLink := getDebridCachedLink(reqCtx, &torrentRecord, season, episode, isMovie)
		if errCachedLink == nil && dlLink != "" {
			c.Redirect(http.StatusFound, dlLink)
			return
		}
	}

	// Add and Select magnet on Debrid Provider
	info, errAdd := p.AddAndSelect(reqCtx, cache.Magnet)
	if errAdd != nil {
		utils.Logger.Error().Err(errAdd).Str("infohash", infohash).Msg("Debrid AddAndSelect failed.")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to add magnet to debrid provider: " + errAdd.Error()})
		return
	}

	// Poll status (max 60 iterations * 3s = 3 minutes) until "downloaded"
	maxPolls := 60
	pollInterval := 3 * time.Second
	downloaded := false

	for i := 0; i < maxPolls; i++ {
		select {
		case <-reqCtx.Done():
			utils.Logger.Info().
				Str("infohash", infohash).
				Str("id", info.ID).
				Msg("Client disconnected during active stream loading. Detaching and delegating debrid caching to background.")
			
			go func(torrentID string, infohash string) {
				defer func() {
					if r := recover(); r != nil {
						utils.Logger.Error().Interface("panic", r).Msg("Recovered from background caching poll panic.")
					}
				}()

				// Check global semaphore to cap background execution
				select {
				case backgroundPollSemaphore <- struct{}{}:
					defer func() { <-backgroundPollSemaphore }()
				default:
					utils.Logger.Warn().Str("infohash", infohash).Msg("Background caching queue full. Skipping background polling to prevent API throttling.")
					return
				}

				bgCtx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
				defer cancel()

				bgProvider := debrid.GetProvider(config.Load())
				bgDownloaded := false
				var bgInfo *debrid.TorrentInfo
				var bgErr error

				bgPollInterval := 15 * time.Second

				for j := 0; j < (180 / 15); j++ {
					select {
					case <-bgCtx.Done():
						return
					default:
					}

					bgInfo, bgErr = bgProvider.GetTorrentInfo(bgCtx, torrentID)
					if bgErr == nil && bgInfo.Status == "downloaded" {
						bgDownloaded = true
						break
					}
					time.Sleep(bgPollInterval)
				}

				if bgDownloaded && bgInfo != nil {
					// Successfully finished downloading in the background. Write to local GORM cache
					var record database.DebridTorrent
					record.Infohash = infohash
					record.TorrentID = torrentID
					record.Provider = cfg.DebridService
					record.Status = "downloaded"
					record.Files = make([]database.TorrentFile, len(bgInfo.Files))
					for idx, f := range bgInfo.Files {
						record.Files[idx] = database.TorrentFile{
							ID:       f.ID,
							Path:     f.Path,
							Bytes:    f.Bytes,
							Selected: f.Selected,
						}
					}
					record.Links = bgInfo.Links
					record.LastChecked = time.Now()

					_ = database.DB.Clauses(clause.OnConflict{
						Columns:   []clause.Column{{Name: "infohash"}},
						UpdateAll: true,
					}).Create(&record).Error
					utils.Logger.Info().Str("infohash", infohash).Msg("Debrid torrent cached successfully in background.")
				}
			}(info.ID, infohash)

			c.JSON(499, gin.H{"error": "Request cancelled by client. Cache polling detached to background."})
			return
		default:
		}

		info, err = p.GetTorrentInfo(reqCtx, info.ID)
		if err != nil {
			utils.Logger.Warn().Err(err).Str("id", info.ID).Msg("Error polling debrid torrent status. Retrying.")
		} else if info.Status == "downloaded" {
			downloaded = true
			break
		}
		time.Sleep(pollInterval)
	}

	if !downloaded {
		c.JSON(http.StatusRequestTimeout, gin.H{"error": "Debrid download timed out. Please try streaming this item again shortly."})
		return
	}

	// Find the matching video link
	finalLink := ""
	if isMovie {
		var selectedFiles []debrid.FileInfo
		for _, f := range info.Files {
			if f.Selected == 1 {
				selectedFiles = append(selectedFiles, f)
			}
		}
		if len(selectedFiles) > 0 {
			var fileToPlay *debrid.FileInfo
			var videoFiles []debrid.FileInfo
			for _, f := range selectedFiles {
				if strings.HasSuffix(strings.ToLower(f.Path), ".mkv") ||
					strings.HasSuffix(strings.ToLower(f.Path), ".mp4") ||
					strings.HasSuffix(strings.ToLower(f.Path), ".avi") ||
					strings.HasSuffix(strings.ToLower(f.Path), ".mov") {
					videoFiles = append(videoFiles, f)
				}
			}
			if len(videoFiles) > 0 {
				largest := &videoFiles[0]
				for i := 1; i < len(videoFiles); i++ {
					if videoFiles[i].Bytes > largest.Bytes {
						largest = &videoFiles[i]
					}
				}
				fileToPlay = largest
			} else {
				largest := &selectedFiles[0]
				for i := 1; i < len(selectedFiles); i++ {
					if selectedFiles[i].Bytes > largest.Bytes {
						largest = &selectedFiles[i]
					}
				}
				fileToPlay = largest
			}

			dl, errDl := getDownloadLinkForFile(reqCtx, p, info, fileToPlay.ID)
			if errDl == nil {
				finalLink = dl
			}
		}
	} else {
		// For series, map files to CandidateFile structures and run FindBestSeriesFile selection
		candidates := make([]parser.CandidateFile, len(info.Files))
		for idx, f := range info.Files {
			candidates[idx] = parser.CandidateFile{
				ID:   f.ID,
				Path: f.Path,
				Size: f.Bytes,
			}
		}

		best, found := parser.FindBestSeriesFile(candidates, season, episode, season)
		if found {
			dl, errDl := getDownloadLinkForFile(reqCtx, p, info, best.ID)
			if errDl == nil {
				finalLink = dl
			}
		}
	}

	if finalLink == "" {
		c.JSON(http.StatusNotFound, gin.H{"error": "Failed to locate target video file inside debrid payload."})
		return
	}

	// Cache the success inside GORM database
	torrentRecord.Infohash = infohash
	torrentRecord.TorrentID = info.ID
	torrentRecord.Provider = cfg.DebridService
	torrentRecord.Status = "downloaded"
	torrentRecord.Files = make([]database.TorrentFile, len(info.Files))
	for idx, f := range info.Files {
		torrentRecord.Files[idx] = database.TorrentFile{
			ID:       f.ID,
			Path:     f.Path,
			Bytes:    f.Bytes,
			Selected: f.Selected,
		}
	}
	torrentRecord.Links = info.Links
	torrentRecord.LastChecked = time.Now()

	_ = database.DB.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "infohash"}},
		UpdateAll: true,
	}).Create(&torrentRecord).Error

	c.Redirect(http.StatusFound, finalLink)
}

// ── Cached Debrid Link Resolver ──

func getDebridCachedLink(ctx context.Context, r *database.DebridTorrent, season, episode int, isMovie bool) (string, error) {
	cfg := config.Load()
	p := debrid.GetProvider(cfg)

	// Map DB record back to TorrentInfo shape to utilize our universal link resolver
	info := &debrid.TorrentInfo{
		ID:    r.TorrentID,
		Links: r.Links,
	}
	info.Files = make([]debrid.FileInfo, len(r.Files))
	for i, f := range r.Files {
		info.Files[i] = debrid.FileInfo{
			ID:       f.ID,
			Path:     f.Path,
			Bytes:    f.Bytes,
			Selected: f.Selected,
		}
	}

	if isMovie {
		var selectedFiles []debrid.FileInfo
		for _, f := range info.Files {
			if f.Selected == 1 {
				selectedFiles = append(selectedFiles, f)
			}
		}
		if len(selectedFiles) == 0 {
			return "", fmt.Errorf("no selected files")
		}
		var fileToPlay *debrid.FileInfo
		var videoFiles []debrid.FileInfo
		for _, f := range selectedFiles {
			if strings.HasSuffix(strings.ToLower(f.Path), ".mkv") ||
				strings.HasSuffix(strings.ToLower(f.Path), ".mp4") ||
				strings.HasSuffix(strings.ToLower(f.Path), ".avi") {
				videoFiles = append(videoFiles, f)
			}
		}
		if len(videoFiles) > 0 {
			largest := &videoFiles[0]
			for i := 1; i < len(videoFiles); i++ {
				if videoFiles[i].Bytes > largest.Bytes {
					largest = &videoFiles[i]
				}
			}
			fileToPlay = largest
		} else {
			largest := &selectedFiles[0]
			for i := 1; i < len(selectedFiles); i++ {
				if selectedFiles[i].Bytes > largest.Bytes {
					largest = &selectedFiles[i]
				}
			}
			fileToPlay = largest
		}

		return getDownloadLinkForFile(ctx, p, info, fileToPlay.ID)
	}

	// Build Series Candidates list
	candidates := make([]parser.CandidateFile, len(info.Files))
	for idx, f := range info.Files {
		candidates[idx] = parser.CandidateFile{
			ID:   f.ID,
			Path: f.Path,
			Size: f.Bytes,
		}
	}

	best, found := parser.FindBestSeriesFile(candidates, season, episode, season)
	if found {
		return getDownloadLinkForFile(ctx, p, info, best.ID)
	}

	return "", fmt.Errorf("best file match not found")
}

// getDownloadLinkForFile dynamically asserts if the provider implements direct link resolution (TorBox) or index-based unrestrict (Real-Debrid)
func getDownloadLinkForFile(ctx context.Context, p debrid.Provider, info *debrid.TorrentInfo, fileID int) (string, error) {
	if dlProvider, ok := p.(interface {
		GetDownloadLinkForFile(context.Context, string, string) (string, error)
	}); ok {
		return dlProvider.GetDownloadLinkForFile(ctx, info.ID, strconv.Itoa(fileID))
	}

	// Real-Debrid / Fallback index-based link unrestriction
	linkIndex := -1
	selectedCount := 0
	for _, f := range info.Files {
		if f.Selected == 1 {
			if f.ID == fileID {
				linkIndex = selectedCount
				break
			}
			selectedCount++
		}
	}

	if linkIndex < 0 || linkIndex >= len(info.Links) {
		return "", fmt.Errorf("file ID %d not selected or links index out of range", fileID)
	}

	unrestricted, err := p.UnrestrictLink(ctx, info.Links[linkIndex])
	if err != nil {
		return "", err
	}
	return unrestricted.Download, nil
}

// ── Tracker Sources ──

// buildTrackerSources returns Stremio-formatted tracker sources.
// Format: "tracker:udp://host:port" — matches Node.js exactly.
func buildTrackerSources() []string {
	trackers := tracker.GetTrackers()
	allowed := make([]string, 0, len(trackers))
	for _, t := range trackers {
		if strings.HasPrefix(t, "udp://") || strings.HasPrefix(t, "http://") || strings.HasPrefix(t, "https://") {
			proto := "http"
			rest := t
			if strings.HasPrefix(t, "udp://") {
				proto = "udp"
				rest = strings.TrimPrefix(t, "udp://")
			} else if strings.HasPrefix(t, "http://") {
				rest = strings.TrimPrefix(t, "http://")
			} else if strings.HasPrefix(t, "https://") {
				rest = strings.TrimPrefix(rest, "https://")
			}
			allowed = append(allowed, "tracker:"+proto+"://"+rest)
		}
	}
	return allowed
}

// withDhtSource appends the DHT source for the given infohash.
// Mirrors Node.js withDhtSource exactly.
func withDhtSource(sources []string, infohash string) []string {
	if infohash == "" {
		return sources
	}
	list := make([]string, len(sources))
	copy(list, sources)
	list = append(list, "dht:"+infohash)
	return list
}
