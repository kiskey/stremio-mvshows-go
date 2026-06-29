// Version: 1.2.1
// Change log: Enhanced catalogHandler with on-the-fly raw title sanitization to prevent uncleaned forum strings from leaking into the Stremio UI when Cinemeta details return empty.

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
	r.GET("/meta/:type/:id", metaHandler)   // Bypasses Gin suffix wildcard match limitation
	r.GET("/stream/:type/:id", streamHandler) // Bypasses Gin suffix wildcard match limitation
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
			"stream", // Removed "meta" to let Cinemeta completely render descriptions and episode selectors
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
	// Upgraded, robust query and path skip parameter parsing (handles queries and inline splits cleanly)
	if qSkip := c.Query("skip"); qSkip != "" {
		if val, err := strconv.Atoi(qSkip); err == nil {
			skip = val
		}
	} else if extra != "" {
		if vals, err := url.ParseQuery(extra); err == nil {
			if parsedSkip := vals.Get("skip"); parsedSkip != "" {
				if val, err := strconv.Atoi(parsedSkip); err == nil {
					skip = val
				}
			}
		} else if strings.Contains(extra, "skip=") {
			parts := strings.Split(extra, "skip=")
			if len(parts) > 1 {
				numStr := parts[1]
				if idx := strings.IndexAny(numStr, "&?"); idx != -1 {
					numStr = numStr[:idx]
				}
				if val, err := strconv.Atoi(numStr); err == nil {
					skip = val
				}
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

	// Architectural Limit Calibration: leverage index-scan by ordering strictly on indexed posted_at DESC.
	// Removing the redundant CASE sorting allows SQLite to return catalog results instantly.
	err := query.
		Order("posted_at DESC").
		Offset(skip).
		Limit(40).
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
			var fetchedMetas []database.TmdbMetadata
			if database.DB.Where("tmdb_id = ?", *t.TmdbID).Limit(1).Find(&fetchedMetas).Error == nil && len(fetchedMetas) > 0 {
				t.TmdbMetadata = &fetchedMetas[0]
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

		title := t.CleanTitle
		if title == "" {
			// Read-Time Sanitization Failsafe:
			// If CleanTitle is empty, parse and clean the RawTitle on-the-fly to prevent raw torrent tag leaks.
			parsed := parser.ParseTitle(t.RawTitle)
			if parsed != nil && parsed.Title != "" {
				title = parsed.Title
			} else {
				title = t.RawTitle
			}
		}

		var tmdbData tmdbLightData
		releaseInfo := ""
		var imdbRating *string
		var genresList []string

		if t.TmdbMetadata != nil {
			// Memory Optimization: decode directly from strings.NewReader to prevent []byte cast and allocations
			dec := json.NewDecoder(strings.NewReader(t.TmdbMetadata.Data))
			if dec.Decode(&tmdbData) == nil {
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

		// Fallback releaseInfo from Thread.Year
		if releaseInfo == "" && t.Year != nil {
			releaseInfo = strconv.Itoa(*t.Year)
		}

		// HIGH-FIDELITY RESOLUTION: Dynamically generate official, CORS-whitelisted, Stremio-native Metahub poster CDN URLs
		// This bypasses raw TMDB image paths entirely, resolving hotlink protections and rendering artwork cleanly.
		poster := "https://images.metahub.space/poster/medium/" + metaID + "/img"

		if t.CustomPoster != nil && *t.CustomPoster != "" {
			poster = *t.CustomPoster
		}

		// Set default description if TMDBoverview failed
		desc := tmdbData.Overview

		if t.CustomDescription != nil && *t.CustomDescription != "" {
			desc = *t.CustomDescription
		}

		metaEntry := StremioMetaEntry{
			ID:          metaID,
			Type:        t.Type,
			Name:        title,
			ReleaseInfo: releaseInfo,
			Poster:      poster,
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
	id := c.Param("id")
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
	// BULLETPROOF FIX: Overhauled query using standard GORM .Find() to avoid SQLite sorting and ORDER BY bugs
	var meta database.TmdbMetadata
	var metas []database.TmdbMetadata
	err := database.DB.Where("imdb_id = ? OR tmdb_id = ?", cleanID, cleanID).Limit(1).Find(&metas).Error
	if err == nil && len(metas) > 0 {
		meta = metas[0]

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
			mediaType := "movie"
			if len(meta.Threads) > 0 {
				mediaType = meta.Threads[0].Type
			}

			displayName := details.Title
			if displayName == "" {
				displayName = details.Name
			}

			poster := "https://images.metahub.space/poster/medium/" + cleanID + "/img"

			// Apply custom metadata overrides if defined by the admin
			overview := details.Overview
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
				Poster:      poster,
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

	var threads []database.Thread
	errThread := database.DB.Where("thread_hash = ?", cleanID).Limit(1).Find(&threads).Error
	if errThread == nil && len(threads) > 0 {
		t := threads[0]
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
	id := c.Param("id")
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

	// BULLETPROOF FIX: Overhauled query using standard GORM .Find() to avoid SQLite sorting and ORDER BY bugs
	var meta database.TmdbMetadata
	var metas []database.TmdbMetadata
	err := database.DB.Where("imdb_id = ? OR tmdb_id = ?", baseID, baseID).Limit(1).Find(&metas).Error
	if err != nil || len(metas) == 0 {
		// If metadata lookup fails, check if the query requested unlinked/pending ThreadHash streams
		var threads []database.Thread
		errThread := database.DB.Where("thread_hash = ?", baseID).Limit(1).Find(&threads).Error
		if errThread == nil && len(threads) > 0 {
			t := threads[0]
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

				var p2pBadgeLine string
				var p2pResTag string
				if u, err := url.Parse(magnet); err == nil {
					if dnQuery := u.Query().Get("dn"); dnQuery != "" {
						if decoded, err := url.QueryUnescape(dnQuery); err == nil {
							p2pBadgeLine = parser.FormatBadges(decoded)
							if p2pSize := parser.ExtractFileSize(decoded); p2pSize != "" {
								p2pBadgeLine += "  |  💾 " + p2pSize
							}

							parser.CompileFilters()
							for _, f := range parser.CompiledFilters {
								if f.GroupID == "gr" && f.Positive.MatchString(decoded) {
									p2pResTag = " [" + f.Name + "]"
									break
								}
							}
						}
					}
				}

				if p2pBadgeLine == "" {
					p2pBadgeLine = formatQualityBadge(parsedMagnet.Quality)
				}
				if p2pResTag == "" && parsedMagnet.Quality != "" && strings.ToLower(parsedMagnet.Quality) != "sd" {
					p2pResTag = " [" + strings.ToUpper(parsedMagnet.Quality) + "]"
				}

				trackerSources := buildTrackerSources()
				sources := withDhtSource(trackerSources, parsedMagnet.Infohash)

				streamList = append(streamList, StremioStreamDetail{
					Name:     "🔌 P2P" + p2pResTag,
					Title:    fmt.Sprintf("🎬 %s\n✨ %s   |   🔊 Peer-to-Peer Stream", t.RawTitle, p2pBadgeLine),
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
	meta = metas[0]

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

	// Short-circuit instantly if 0 target streams are resolved (mitigates R-003)
	if len(allHashes) == 0 {
		c.JSON(http.StatusOK, StremioStreamResponse{Streams: []StremioStreamDetail{}})
		return
	}

	// BULK PRE-FETCH: Load all magnet display names from magnet_cache table
	var magnetCaches []database.MagnetCache
	_ = database.DB.Where("infohash IN ?", allHashes).Find(&magnetCaches)
	magnetMap := make(map[string]string)
	for _, mc := range magnetCaches {
		// Safe fallback parser robustly resolves raw display names on unescaped symbols (R-004)
		if dn := parser.ExtractMagnetDisplayName(mc.Magnet); dn != "" {
			magnetMap[mc.Infohash] = dn
		}
	}

	// BULK PRE-FETCH: Load all debrid torrent records to fetch active downloaded file structures/sizes
	var debridTorrents []database.DebridTorrent
	_ = database.DB.Where("infohash IN ?", allHashes).Find(&debridTorrents)
	debridTorrentMap := make(map[string]database.DebridTorrent)
	for _, dt := range debridTorrents {
		debridTorrentMap[dt.Infohash] = dt
	}

	var cachedStreams []StremioStreamDetail   // Instant ⚡ streams
	var uncachedStreams []StremioStreamDetail // Downloading ⏳ streams

	seenStreams := make(map[streamDupKey]bool)

	mediaType := "movie"
	if len(meta.Threads) > 0 {
		mediaType = meta.Threads[0].Type
	}

	// Pre-fetch TMDB title for movie/series streams
	var tmdbTitle string
	var tmdbData tmdbLightData
	dec := json.NewDecoder(strings.NewReader(meta.Data))
	if dec.Decode(&tmdbData) == nil {
		tmdbTitle = tmdbData.Title
		if tmdbTitle == "" {
			tmdbTitle = tmdbData.Name
		}
	}

	for _, s := range streams {
		isCached := cacheMap[s.Infohash]

		// Resolve high-priority resolution tag dynamically for Stream Card Name display
		var resTag string
		var dn string
		if val, ok := magnetMap[s.Infohash]; ok {
			dn = val
		}

		if dn != "" {
			parser.CompileFilters()
			for _, f := range parser.CompiledFilters {
				if f.GroupID == "gr" && f.Positive.MatchString(dn) {
					resTag = " [" + f.Name + "]"
					break
				}
			}
		}
		if resTag == "" && s.Quality != "" && strings.ToLower(s.Quality) != "sd" {
			resTag = " [" + strings.ToUpper(s.Quality) + "]"
		}

		// Generate clean dynamic badges from dn
		var parsedBadges string
		if dn != "" {
			parsedBadges = parser.FormatBadges(dn)
		}

		// Format final badges string
		badgeLine := ""
		if parsedBadges != "" {
			badgeLine = parsedBadges
		} else if s.Quality != "" {
			badgeLine = "[" + strings.ToUpper(s.Quality) + "]"
		}

		// Append overall file size to the badges line if known, falling back on direct regex name parsing if uncached
		var totalSize int64 = 0
		if dt, ok := debridTorrentMap[s.Infohash]; ok {
			for _, f := range dt.Files {
				totalSize += f.Bytes
			}
		}
		if totalSize > 0 {
			badgeLine += "  |  💾 " + utils.FormatSize(totalSize)
		} else if dn != "" {
			if parsedSize := parser.ExtractFileSize(dn); parsedSize != "" {
				badgeLine += "  |  💾 " + parsedSize
			}
		}

		// Series/Movie main identity header
		var detailsHeader string
		if mediaType == "series" {
			seasonVal := 0
			if s.Season != nil {
				seasonVal = *s.Season
			}
			epVal := 0
			if s.Episode != nil {
				epVal = *s.Episode
			}
			epEndVal := epVal
			if s.EpisodeEnd != nil {
				epEndVal = *s.EpisodeEnd
			}

			seasonStr := fmt.Sprintf("S%02d", seasonVal)
			var epPart string
			if epEndVal == 0 || epEndVal == epVal {
				epPart = fmt.Sprintf("Episode %02d", epVal)
			} else if epVal == 1 && epEndVal == 999 {
				epPart = "Season Pack"
			} else {
				epPart = fmt.Sprintf("Episodes %02d-%02d", epVal, epEndVal)
			}
			detailsHeader = fmt.Sprintf("🎬 %s (%s | %s)", tmdbTitle, seasonStr, epPart)
		} else {
			detailsHeader = fmt.Sprintf("🎬 %s", tmdbTitle)
		}

		langBadge := formatLanguage(s.Language)

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
			if dt, ok := debridTorrentMap[s.Infohash]; ok && dt.Status == "downloaded" && len(dt.Files) > 0 && len(dt.Links) > 0 {
				debridTorrent = dt
				fileToStream, linkIndex := pickBestDebridFile(debridTorrent.Files, debridTorrent.Links, mediaType, season, episode)
				if fileToStream != nil && linkIndex >= 0 && linkIndex < len(debridTorrent.Links) {
					unrestricted, errUnrestrict := p.UnrestrictLink(c.Request.Context(), debridTorrent.Links[linkIndex])
					if errUnrestrict == nil && unrestricted != nil && unrestricted.Download != "" {
						badgeName := "⚡ RD+" + resTag
						if cfg.DebridService == "torbox" {
							badgeName = "⚡ TB+" + resTag
						}

						formattedTitle := fmt.Sprintf("%s\n✨ %s\n🔊 %s\n📦 File: %s (%s)", 
							detailsHeader, 
							badgeLine, 
							langBadge, 
							fileToStream.Path, 
							utils.FormatSize(fileToStream.Bytes),
						)

						item := StremioStreamDetail{
							Name:     badgeName,
							Title:    formattedTitle,
							URL:      unrestricted.Download,
							Quality:  s.Quality,
							Language: s.Language,
						}
						cachedStreams = append(cachedStreams, item)
						continue // Skip the /rd-add/ fallback for this stream
					}
				}
			}

			// ---- Standard RD/TB stream (redirects through /rd-add/) ----
			targetEpStr := "movie"
			if season != -1 && episode != -1 {
				targetEpStr = fmt.Sprintf("%d-%d", season, episode)
			}
			rdUrl := fmt.Sprintf("%s/rd-add/%s/%s", cfg.AppHost, s.Infohash, targetEpStr)

			badgeName := "⏳ RD" + resTag
			if isCached {
				badgeName = "⚡ RD+" + resTag
			}
			if cfg.DebridService == "torbox" {
				badgeName = "⏳ TB" + resTag
				if isCached {
					badgeName = "⚡ TB+" + resTag
				}
			}

			formattedTitle := fmt.Sprintf("%s\n✨ %s\n🔊 %s\n📥 Click to Stream", 
				detailsHeader, 
				badgeLine, 
				langBadge,
			)

			item := StremioStreamDetail{
				Name:     badgeName,
				Title:    formattedTitle,
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
			badgeName := "🔌 P2P" + resTag

			formattedTitle := fmt.Sprintf("%s\n✨ %s\n🔊 %s\n🔗 Peer-to-Peer Stream", 
				detailsHeader, 
				badgeLine, 
				langBadge,
			)

			item := StremioStreamDetail{
				Name:     badgeName,
				Title:    formattedTitle,
				InfoHash: s.Infohash,
				Sources:  sources,
				Quality:  s.Quality,
				Language: s.Language,
			}
			uncachedStreams = append(uncachedStreams, item)
		}
	}

	// Combine, deduplicate, sort, and strip internal keys
	streamList = append(cachedStreams, uncachedStreams...)
	streamList = dedupeStreams(streamList)
	sortStreams(streamList)

	c.JSON(http.StatusOK, StremioStreamResponse{Streams: streamList})
}

// ── Stream Title Builders ──

func buildSeriesTitle(tmdbTitle string, season, episode, episodeEnd int, quality, language string) string {
	seasonStr := fmt.Sprintf("S%02d", season)

	var epPart string
	if episodeEnd == 0 || episodeEnd == episode {
		epPart = fmt.Sprintf("Episode %02d", episode)
	} else if episode == 1 && episodeEnd == 999 {
		epPart = "Season Pack"
	} else {
		epPart = fmt.Sprintf("Episodes %02d-%02d", episode, episodeEnd)
	}

	qBadge := formatQualityBadge(quality)
	langBadge := formatLanguage(language)

	return fmt.Sprintf("🎬 %s (%s | %s)\n✨ %s   |   🔊 %s", tmdbTitle, seasonStr, epPart, qBadge, langBadge)
}

func buildMovieTitle(tmdbTitle, quality, language string) string {
	qBadge := formatQualityBadge(quality)
	langBadge := formatLanguage(language)

	return fmt.Sprintf("🎬 %s\n✨ %s   |   🔊 %s", tmdbTitle, qBadge, langBadge)
}

// ── Presentation Formatters ──

var languageFlags = map[string]string{
	"ta": "Tamil 🇮🇳",
	"te": "Telugu 🇮🇳",
	"ml": "Malayalam 🇮🇳",
	"hi": "Hindi 🇮🇳",
	"kn": "Kannada 🇮🇳",
	"en": "English 🇬🇧",
	"fr": "French 🇫🇷",
	"es": "Spanish 🇪🇸",
	"de": "German 🇩🇪",
	"it": "Italian 🇮🇹",
	"ja": "Japanese 🇯🇵",
	"ko": "Korean 🇰🇷",
	"zh": "Chinese 🇨🇳",
}

func formatLanguage(lang string) string {
	lang = strings.ToLower(strings.TrimSpace(lang))
	if lang == "" {
		return "Unknown 🌐"
	}
	if val, ok := languageFlags[lang]; ok {
		return val
	}
	return strings.ToUpper(lang) + " 🌐"
}

func formatQualityBadge(q string) string {
	q = strings.ToUpper(strings.TrimSpace(q))
	switch q {
	case "4K", "2160P":
		return "🚀 4K UHD"
	case "1080P":
		return "🔥 1080p HD"
	case "720P":
		return "⚡ 720p HD"
	case "480P", "SD", "360P":
		return "📼 SD"
	default:
		return "🎥 " + q
	}
}

// ── Debrid File Selection ──

// pickBestDebridFile maps files list to CandidateFiles, executing identical premium-grade parser matching logic (ranges, absolute bounds, and sequential fallbacks) on the instant path.
func pickBestDebridFile(files database.JSONFileList, links database.JSONStringArray, mediaType string, season, episode int) (*database.TorrentFile, int) {
	if len(files) == 0 || len(links) == 0 {
		return nil, -1
	}

	// Map GORM database files list to parser.CandidateFile slice
	candidates := make([]parser.CandidateFile, 0, len(files))
	for i, f := range files {
		if f.Selected == 1 {
			candidates = append(candidates, parser.CandidateFile{
				ID:   i, // Store original slice index as CandidateID to maintain index reference
				Path: f.Path,
				Size: f.Bytes,
			})
		}
	}

	if len(candidates) == 0 {
		return nil, -1
	}

	var matchedIndex int = -1

	if mediaType == "series" && episode > 0 {
		// Leverage complete, optimized parser matching logic (ranges, absolute segments, alphabetical indexing)
		bestFile, found := parser.FindBestSeriesFile(candidates, season, episode, season)
		if found {
			matchedIndex = bestFile.ID
		}
	} else {
		// For movies: pick largest video file
		isVideo := func(path string) bool {
			p := strings.ToLower(path)
			return strings.HasSuffix(p, ".mkv") ||
				strings.HasSuffix(p, ".mp4") ||
				strings.HasSuffix(p, ".avi") ||
				strings.HasSuffix(p, ".mov") ||
				strings.HasSuffix(p, ".m4v")
		}
		var largestID int = -1
		var largestSize int64 = -1
		for _, c := range candidates {
			if !isVideo(c.Path) {
				continue
			}
			if c.Size > largestSize {
				largestSize = c.Size
				largestID = c.ID
			}
		}
		if largestID == -1 && len(candidates) > 0 {
			// Fallback to largest file overall
			for _, c := range candidates {
				if c.Size > largestSize {
					largestSize = c.Size
					largestID = c.ID
				}
			}
		}
		matchedIndex = largestID
	}

	if matchedIndex == -1 {
		return nil, -1
	}

	// Resolve the linkIndex based on position among selected files
	linkIndex := 0
	for j := 0; j <= matchedIndex; j++ {
		if files[j].Selected == 1 {
			if j == matchedIndex {
				return &files[matchedIndex], linkIndex
			}
			linkIndex++
		}
	}

	return nil, -1
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
	var caches []database.MagnetCache
	err := database.DB.Where("infohash = ?", infohash).Limit(1).Find(&caches).Error
	if err != nil || len(caches) == 0 {
		c.JSON(http.StatusNotFound, gin.H{"error": "Magnet not found in local cache"})
		return
	}
	cache = caches[0]

	cfg := config.Load()
	p := debrid.GetProvider(cfg)
	if !p.IsEnabled() {
		c.JSON(http.StatusBadRequest, gin.H{"error": "No active debrid service configured"})
		return
	}

	reqCtx := c.Request.Context()

	// Check if already completely downloaded and saved locally in DebridTorrent table
	var torrentRecord database.DebridTorrent
	var torrentRecords []database.DebridTorrent
	errRecord := database.DB.Where("infohash = ?", infohash).Limit(1).Find(&torrentRecords).Error
	if errRecord == nil && len(torrentRecords) > 0 && torrentRecords[0].Status == "downloaded" {
		torrentRecord = torrentRecords[0]
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
	torrentID := info.ID // Store ID in a persistent string variable to prevent nil pointer dereference on transient errors

	for i := 0; i < maxPolls; i++ {
		select {
		case <-reqCtx.Done():
			utils.Logger.Info().
				Str("infohash", infohash).
				Str("id", torrentID).
				Msg("Client disconnected during active stream loading. Detaching and delegating debrid caching to background.")
			
			go func(tID string, infohash string) {
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

					bgInfo, bgErr = bgProvider.GetTorrentInfo(bgCtx, tID)
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
					record.TorrentID = tID
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
			}(torrentID, infohash)

			c.JSON(499, gin.H{"error": "Request cancelled by client. Cache polling detached to background."})
			return
		default:
		}

		info, err = p.GetTorrentInfo(reqCtx, torrentID)
		if err != nil {
			utils.Logger.Warn().Err(err).Str("id", torrentID).Msg("Error polling debrid torrent status. Retrying.")
		} else if info != nil && info.Status == "downloaded" {
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
