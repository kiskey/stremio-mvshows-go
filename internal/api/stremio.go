// Version: 1.0.7
// Change log: Implemented bulletproof Raw SQL queries in streamHandler and metaHandler to completely bypass GORM model relationship/collation bugs. Added robust trim and sanitation guardrails.

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

// Hot Path regex pre-compiled to prevent CPU and heap allocations during file selections
var epRegex = regexp.MustCompile(`[Ss]\d{1,2}\s*[Ee]\s*(\d{1,3})`)

// ── Stremio Protocol Statically-Typed Struct Responses ──

type StremioCatalogResponse struct {
	Metas []StremioMetaEntry `json:"metas"`
}

type StremioMetaEntry struct {
	ID          string   `json:"id"`
	Type        string   `json:"type"`
	Name        string   `json:"name"`
	ReleaseInfo string   `json:"releaseInfo"`
	Poster      string   `json:"poster,omitempty"`
	Description string   `json:"description,omitempty"`
	ImdbRating  string   `json:"imdbRating,omitempty"`
	Genres      []string `json:"genres,omitempty"`
}

type StremioMetaResponse struct {
	Meta StremioMetaDetail `json:"meta"`
}

type StremioMetaDetail struct {
	ID          string               `json:"id"`
	Type        string               `json:"type"`
	Name        string               `json:"name"`
	ReleaseInfo string               `json:"releaseInfo"`
	Poster      string               `json:"poster,omitempty"`
	Description string               `json:"description,omitempty"`
	ImdbRating  string               `json:"imdbRating,omitempty"`
	Genres      []string             `json:"genres,omitempty"`
	Videos      []StremioVideoDetail `json:"videos,omitempty"`
}

type StremioVideoDetail struct {
	ID      string `json:"id"`
	Season  int    `json:"season"`
	Episode int    `json:"episode"`
	Title   string `json:"title"`
}

type StremioStreamResponse struct {
	Streams []StremioStreamDetail `json:"streams"`
}

type StremioStreamDetail struct {
	Name     string   `json:"name"`
	Title    string   `json:"title"`
	URL      string   `json:"url,omitempty"`
	InfoHash string   `json:"infoHash,omitempty"`
	Sources  []string `json:"sources,omitempty"`
	// Internal tracking fields (excluded from JSON serialization)
	Quality  string   `json:"-"`
	Language string   `json:"-"`
}

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

// StreamSlice implements standard sort.Interface to achieve reflection-free sorting
type StreamSlice []StremioStreamDetail

func (s StreamSlice) Len() int      { return len(s) }
func (s StreamSlice) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s StreamSlice) Less(i, j int) bool {
	a, b := s[i], s[j]

	// RD streams (have non-empty URL) come before P2P (have non-empty InfoHash)
	aIsP2P := a.InfoHash != ""
	bIsP2P := b.InfoHash != ""

	if !aIsP2P && bIsP2P {
		return true // a is RD, b is P2P
	}
	if aIsP2P && !bIsP2P {
		return false // a is P2P, b is RD
	}

	// Compare quality rank
	aQuality := a.Quality
	bQuality := b.Quality
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
	aLang := a.Language
	bLang := b.Language
	if aLang == "" {
		aLang = "zz"
	}
	if bLang == "" {
		bLang = "zz"
	}
	return strings.ToLower(aLang) < strings.ToLower(bLang)
}

// sortStreams stable-sorts stream array cleanly without reflective performance penalties.
func sortStreams(streams []StremioStreamDetail) {
	sort.Stable(StreamSlice(streams))
}

// dedupeStreams removes duplicate entries keyed by (isRD | quality | language | infohash).
func dedupeStreams(streams []StremioStreamDetail) []StremioStreamDetail {
	seen := make(map[string]bool)
	out := make([]StremioStreamDetail, 0, len(streams))
	for _, s := range streams {
		streamType := "p2p"
		if s.URL != "" {
			streamType = "rd"
		}

		quality := s.Quality
		if quality == "" {
			quality = "SD"
		}
		lang := s.Language
		if lang == "" {
			lang = "NA"
		}

		hashOrURL := s.InfoHash
		if s.URL != "" {
			hashOrURL = s.URL
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
	query := database.DB.
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

	metas := make([]StremioMetaEntry, 0, len(threads))
	seenIDs := make(map[string]bool)

	for _, t := range threads {
		// Self-healing Fallback: If GORM's Preload fail silently, resolve relationship via direct index query
		if t.TmdbMetadata == nil && t.TmdbID != nil {
			var fetchedMeta database.TmdbMetadata
			if database.DB.Where("tmdb_id = ?", *t.TmdbID).First(&fetchedMeta).Error == nil {
				t.TmdbMetadata = &fetchedMeta
			}
		}

		metaID := ""
		if t.TmdbMetadata != nil && t.TmdbMetadata.ImdbID != nil {
			metaID = *t.TmdbMetadata.ImdbID
		}
		if metaID == "" {
			continue // Defensive: skip any record that doesn't have an IMDb ID
		}

		// Deduplicate: avoid duplicate cards per IMDb ID when multiple forum threads link to same ID
		if seenIDs[metaID] {
			continue
		}
		seenIDs[metaID] = true

		poster := ""
		desc := ""
		title := t.CleanTitle
		if title == "" {
			title = t.RawTitle
		}

		var tmdbData tmdbLightData
		releaseInfo := ""
		var imdbRating *string
		var genresList []string

		if t.TmdbMetadata != nil {
			// Memory Optimization: decode directly from strings.NewReader to prevent []byte cast and allocations
			dec := json.NewDecoder(strings.NewReader(t.TmdbMetadata.Data))
			if dec.Decode(&tmdbData) == nil {
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

		metaEntry := StremioMetaEntry{
			ID:          metaID,
			Type:        t.Type,
			Name:        title,
			ReleaseInfo: releaseInfo,
		}

		// Cinemeta inferred poster & description fallback rule:
		// Only output poster and description keys if they possess explicitly loaded or overridden values.
		if poster != "" {
			metaEntry.Poster = poster
		}
		if desc != "" {
			metaEntry.Description = desc
		}
		if imdbRating != nil {
			metaEntry.ImdbRating = *imdbRating
		}
		if len(genresList) > 0 {
			metaEntry.Genres = genresList
		}

		metas = append(metas, metaEntry)
	}

	c.JSON(http.StatusOK, StremioCatalogResponse{Metas: metas})
}

// ── Meta ──

func metaHandler(c *gin.Context) {
	id := strings.TrimSuffix(c.Param("id"), ".json")
	cfg := config.Load()

	// URL-decode the ID parameter to handle percent-encoded colons (%3A) safely
	if decoded, err := url.QueryUnescape(id); err == nil {
		id = decoded
	}

	// Trim trailing extensions and whitespaces to prevent SQL mismatch
	id = strings.TrimSpace(strings.TrimSuffix(id, ".json"))

	cleanID := id
	if idx := strings.Index(id, ":pending:"); idx != -1 {
		cleanID = id[idx+len(":pending:"):]
	}

	// 1. Standard linked metadata lookup by IMDb ID (tt...)
	// BULLETPROOF FIX: Use raw SQL query to completely bypass GORM relationship preloading and mapping bugs
	var meta database.TmdbMetadata
	err := database.DB.Raw("SELECT * FROM tmdb_metadata WHERE imdb_id = ? OR tmdb_id = ? LIMIT 1", cleanID, cleanID).Scan(&meta).Error
	if err == nil && meta.TmdbID != "" {
		// Self-healing Fallback: Fetch threads directly to bypass any GORM Preload mapping issues
		if len(meta.Threads) == 0 {
			var fetchedThreads []database.Thread
			if database.DB.Where("tmdb_id = ?", meta.TmdbID).Find(&fetchedThreads).Error == nil {
				meta.Threads = fetchedThreads
			}
		}

		var details tmdbLightData
		// Memory Optimization: decode directly from strings.NewReader to prevent allocations
		dec := json.NewDecoder(strings.NewReader(meta.Data))
		if dec.Decode(&details) == nil {
			poster := ""
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

			metaObj := StremioMetaDetail{
				ID:          id,
				Type:        mediaType,
				Name:        displayName,
				ReleaseInfo: releaseInfo,
			}

			if poster != "" {
				metaObj.Poster = poster
			}
			if overview != "" {
				metaObj.Description = overview
			}

			if details.VoteAverage > 0 {
				metaObj.ImdbRating = fmt.Sprintf("%.1f", details.VoteAverage)
			}
			if len(details.Genres) > 0 {
				genreNames := make([]string, 0, len(details.Genres))
				for _, g := range details.Genres {
					if g.Name != "" {
						genreNames = append(genreNames, g.Name)
					}
				}
				if len(genreNames) > 0 {
					metaObj.Genres = genreNames
				}
			}

			if mediaType == "series" {
				// Fetch linked streams to build Stremio series videos navigation
				var streams []database.Stream
				_ = database.DB.Where("tmdb_id = ?", meta.TmdbID).Order("season ASC, episode ASC").Find(&streams)

				videos := make([]StremioVideoDetail, 0)
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

							videos = append(videos, StremioVideoDetail{
								ID:      fmt.Sprintf("%s:%d:%d", id, sVal, ep),
								Season:  sVal,
								Episode: ep,
								Title:   fmt.Sprintf("Season %d - Episode %d", sVal, ep),
							})
						}
					}
				}
				metaObj.Videos = videos
			}

			c.JSON(http.StatusOK, StremioMetaResponse{Meta: metaObj})
			return
		}
	}

	var t database.Thread
	errThread := database.DB.Where("thread_hash = ?", cleanID).First(&t).Error
	if errThread == nil {
		metaObj := StremioMetaDetail{
			ID:          id,
			Type:        t.Type,
			Name:        t.RawTitle,
			Poster:      cfg.PlaceholderPoster,
			Description: "Pending metadata match. You can link this manually in the administration rescue panel.",
		}
		if t.Year != nil {
			metaObj.ReleaseInfo = strconv.Itoa(*t.Year)
		}
		c.JSON(http.StatusOK, StremioMetaResponse{Meta: metaObj})
		return
	}

	c.JSON(http.StatusNotFound, gin.H{"error": "Metadata not found"})
}

// ── Stream ──

func streamHandler(c *gin.Context) {
	id := strings.TrimSuffix(c.Param("id"), ".json")
	cfg := config.Load()

	// URL-decode the ID parameter to handle percent-encoded colons (%3A) safely
	if decoded, err := url.QueryUnescape(id); err == nil {
		id = decoded
	}

	// Trim trailing extensions and whitespaces to prevent SQL mismatch
	id = strings.TrimSpace(strings.TrimSuffix(id, ".json"))

	// Strip pending prefix robustly if looking up unlinked thread streams
	cleanID := id
	if idx := strings.Index(id, ":pending:"); idx != -1 {
		cleanID = id[idx+len(":pending:"):]
	}

	var baseID string
	season := -1
	episode := -1

	// Smart ID Splitting: Accurately identifies base ID vs Season/Episode parameters.
	// Allocation Optimization: Use strings.LastIndex to bypass heap-allocating Split arrays
	lastColon := strings.LastIndex(cleanID, ":")
	if lastColon != -1 {
		secondLastColon := strings.LastIndex(cleanID[:lastColon], ":")
		if secondLastColon != -1 {
			sVal, errS := strconv.Atoi(cleanID[secondLastColon+1 : lastColon])
			eVal, errE := strconv.Atoi(cleanID[lastColon+1:])
			if errS == nil && errE == nil {
				season = sVal
				episode = eVal
				baseID = cleanID[:secondLastColon]
			} else {
				baseID = cleanID
			}
		} else {
			baseID = cleanID
		}
	} else {
		baseID = cleanID
	}

	// BULLETPROOF FIX: Use raw SQL query to completely bypass GORM relationship preloading and mapping bugs
	var meta database.TmdbMetadata
	err := database.DB.Raw("SELECT * FROM tmdb_metadata WHERE imdb_id = ? OR tmdb_id = ? LIMIT 1", baseID, baseID).Scan(&meta).Error
	if err != nil || meta.TmdbID == "" {
		// If metadata lookup fails, check if the query requested unlinked/pending ThreadHash streams
		var t database.Thread
		errThread := database.DB.Where("thread_hash = ?", baseID).First(&t).Error
		if errThread == nil {
			// This is an unlinked thread. Return direct P2P fallback streams only (No RD mapping)
			streamList := make([]StremioStreamDetail, 0)
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

				trackerSources := buildTrackerSources()
				sources := withDhtSource(trackerSources, parsedMagnet.Infohash)

				streamList = append(streamList, StremioStreamDetail{
					Name:     fmt.Sprintf("[P2P] %s", parsedMagnet.Quality),
					Title:    t.RawTitle,
					InfoHash: parsedMagnet.Infohash,
					Sources:  sources,
				})
			}
			c.JSON(http.StatusOK, StremioStreamResponse{Streams: streamList})
			return
		}

		// STREMIO PROTOCOL FIX: Never return 404 on the stream endpoint! 
		c.JSON(http.StatusOK, StremioStreamResponse{Streams: []StremioStreamDetail{}})
		return
	}

	// Self-healing Fallback: Fetch threads directly to bypass any GORM Preload mapping issues
	if len(meta.Threads) == 0 {
		var fetchedThreads []database.Thread
		if database.DB.Where("tmdb_id = ?", meta.TmdbID).Find(&fetchedThreads).Error == nil {
			meta.Threads = fetchedThreads
		}
	}

	var streams []database.Stream
	if season != -1 && episode != -1 {
		// Series path: search direct episode match, range packs, OR full season/series packs where episode is NULL
		// Crucial Fix: added fallback for complete series packs where both season AND episode are NULL in the database
		err = database.DB.Where("tmdb_id = ? AND ((season = ? AND episode <= ? AND episode_end >= ?) OR (season = ? AND episode IS NULL) OR (season IS NULL AND episode IS NULL))",
			meta.TmdbID, season, episode, episode, season).
			Order("quality DESC").
			Find(&streams).Error
	} else {
		// Movie path
		err = database.DB.Where("tmdb_id = ?", meta.TmdbID).Order("quality DESC").Find(&streams).Error
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

	var cachedStreams []StremioStreamDetail   // Instant ⚡ streams
	var uncachedStreams []StremioStreamDetail // Downloading ⏳ streams

	seenStreams := make(map[streamDupKey]bool)

	mediaType := "movie"
	if len(meta.Threads) > 0 {
		mediaType = meta.Threads[0].Type
	}

	// Pre-fetch TMDB title for movie streams
	var tmdbTitle string
	if mediaType == "movie" {
		var tmdbData tmdbLightData
		dec := json.NewDecoder(strings.NewReader(meta.Data))
		if dec.Decode(&tmdbData) == nil {
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
						item := StremioStreamDetail{
							Name:     fmt.Sprintf("[RD+] %s ⚡", s.Quality),
							Title:    fmt.Sprintf("%s\n%s", titleDetail, fileToStream.Path),
							URL:      unrestricted.Download,
							Quality:  s.Quality,
							Language: s.Language,
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

			item := StremioStreamDetail{
				Name:     label,
				Title:    fmt.Sprintf("%s\nClick to Download", titleDetail),
				URL:      rdUrl,
				Quality:  s.Quality,
				Language: s.Language,
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
			item := StremioStreamDetail{
				Name:     fmt.Sprintf("[P2P] %s", s.Quality),
				Title:    titleDetail,
				InfoHash: s.Infohash,
				Sources:  sources,
				Quality:  s.Quality,
				Language: s.Language,
			}
			uncachedStreams = append(uncachedStreams, item)
		}
	}

	// Combine, deduplicate, sort, and strip internal keys
	streamList := append(cachedStreams, uncachedStreams...)
	streamList = dedupeStreams(streamList)
	sortStreams(streamList)

	c.JSON(http.StatusOK, StremioStreamResponse{Streams: streamList})
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

func pickBestDebridFile(files database.JSONFileList, links database.JSONStringArray, mediaType string, season, episode int) (*database.TorrentFile, int) {
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
