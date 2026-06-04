package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url" // Critical fix: added net/url to resolve compile error in url.QueryEscape
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
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// Global concurrency semaphore: restricts background debrid pollers to maximum 3 concurrent goroutines.
// This completely protects the server IP from ever hammering or throttling the debrid API providers.
var backgroundPollSemaphore = make(chan struct{}, 3)

// tmdbLightData is a highly optimized, allocation-free struct replacing map[string]interface{} unmarshaling
type tmdbLightData struct {
	Title      string `json:"title"`
	Name       string `json:"name"`
	PosterPath string `json:"poster_path"`
	Overview   string `json:"overview"`
}

// streamDupKey is a comparable struct replacing dynamic formatted strings to prevent heap allocations
type streamDupKey struct {
	IsRD     string
	Quality  string
	Language string
	Infohash string
}

func RegisterStremioRoutes(r *gin.RouterGroup) {
	r.GET("/manifest.json", manifestHandler)
	r.GET("/catalog/:type/:id/:extra", catalogHandler)
	r.GET("/catalog/:type/:id", catalogHandler)
	r.GET("/meta/:type/:id.json", metaHandler)
	r.GET("/stream/:type/:id.json", streamHandler)
	r.GET("/rd-add/:infohash/:episode", rdAddHandler)
}

func manifestHandler(c *gin.Context) {
	cfg := config.Load()
	c.JSON(http.StatusOK, gin.H{
		"id":          cfg.AddonID,
		"version":     cfg.AddonVersion,
		"name":        cfg.AddonName,
		"description": cfg.AddonDescription,
		"resources":   []string{"catalog", "meta", "stream"},
		"types":       []string{"series", "movie"},
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
		"idPrefixes": []string{"tt", "tv", "movie", cfg.AddonID}, // Added AddonID to prefix route unlinked threads
	})
}

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
	// Return both linked and pending threads, prioritizing linked ones at the top of lists
	query := database.DB.Where("status IN ? AND type = ?", []string{"linked", "pending_tmdb"}, mediaType)

	// Filter by specific catalogs matching manifest IDs
	if catalogID == "tamilmv_hd_movies" {
		query = query.Where("catalog = ?", "tamil-hd-movies")
	} else if catalogID == "tamilmv_dubbed_movies" {
		query = query.Where("catalog = ?", "tamil-dubbed-movies")
	} else {
		query = query.Where("catalog = ?", "top-series-from-forum")
	}

	err := query.
		Order("CASE status WHEN 'linked' THEN 0 ELSE 1 END"). // Order linked threads first, then pending rescue items
		Order("posted_at DESC").
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
		metaID := t.ThreadHash
		poster := cfg.PlaceholderPoster
		desc := ""
		title := t.CleanTitle
		if title == "" {
			title = t.RawTitle
		}

		if t.TmdbMetadata != nil {
			if t.TmdbMetadata.ImdbID != "" {
				metaID = t.TmdbMetadata.ImdbID
			} else {
				metaID = t.TmdbMetadata.TmdbID
			}

			// Optimized unmarshaling to prevent thousands of reflection heap allocations per page
			var tmdbData tmdbLightData
			if json.Unmarshal([]byte(t.TmdbMetadata.Data), &tmdbData) == nil {
				if tmdbData.PosterPath != "" {
					poster = "https://image.tmdb.org/t/p/w500" + tmdbData.PosterPath
				}
				if tmdbData.Overview != "" {
					desc = tmdbData.Overview
				}
			}
		} else {
			// Unlinked threads are formatted with addonId:pending: prefix to trigger manifest idPrefix matching
			metaID = fmt.Sprintf("%s:pending:%s", cfg.AddonID, t.ThreadHash)
		}

		if t.CustomPoster != nil && *t.CustomPoster != "" {
			poster = *t.CustomPoster
		}
		if t.CustomDescription != nil && *t.CustomDescription != "" {
			desc = *t.CustomDescription
		}

		yearStr := ""
		if t.Year != nil {
			yearStr = strconv.Itoa(*t.Year)
		}

		metas = append(metas, gin.H{
			"id":          metaID,
			"type":        t.Type,
			"name":        title,
			"poster":      poster,
			"description": desc,
			"releaseInfo": yearStr,
		})
	}

	c.JSON(http.StatusOK, gin.H{"metas": metas})
}

func metaHandler(c *gin.Context) {
	id := strings.TrimSuffix(c.Param("id"), ".json")
	cfg := config.Load()

	// Strip pending prefixes if looking up unlinked threads
	pendingPrefix := cfg.AddonID + ":pending:"
	cleanID := id
	if strings.HasPrefix(id, pendingPrefix) {
		cleanID = strings.TrimPrefix(id, pendingPrefix)
	}

	var meta database.TmdbMetadata
	err := database.DB.Where("imdb_id = ? OR tmdb_id = ?", cleanID, cleanID).Preload("Threads").First(&meta).Error
	if err != nil {
		// If direct metadata lookup fails, check if the ID refers to a pending/unlinked ThreadHash
		var t database.Thread
		errThread := database.DB.Where("thread_hash = ?", cleanID).First(&t).Error
		if errThread == nil {
			// Return a safe placeholder metadata response for pending items
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
		return
	}

	// Optimized unmarshaling replacing map[string]interface{}
	var details tmdbLightData
	if errJson := json.Unmarshal([]byte(meta.Data), &details); errJson != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to parse TMDB data payload"})
		return
	}

	poster := cfg.PlaceholderPoster
	if details.PosterPath != "" {
		poster = "https://image.tmdb.org/t/p/w500" + details.PosterPath
	}

	overview := details.Overview

	// Determine matching media type
	mediaType := "movie"
	if len(meta.Threads) > 0 {
		mediaType = meta.Threads[0].Type
	}

	displayName := details.Title
	if displayName == "" {
		displayName = details.Name
	}

	metaObj := gin.H{
		"id":          id,
		"type":        mediaType,
		"name":        displayName,
		"poster":      poster,
		"description": overview,
		"year":        meta.Year,
	}

	if mediaType == "series" {
		// Fetch linked streams to build the Stremio series videos episodic navigation
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

				// Generate chronological sequence mapping for episode ranges (packs)
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
}

func streamHandler(c *gin.Context) {
	id := strings.TrimSuffix(c.Param("id"), ".json")
	cfg := config.Load()

	// Strip pending prefix if looking up unlinked thread streams
	pendingPrefix := cfg.AddonID + ":pending:"
	cleanID := id
	if strings.HasPrefix(id, pendingPrefix) {
		cleanID = strings.TrimPrefix(id, pendingPrefix)
	}

	var imdbID string
	season := -1
	episode := -1

	parts := strings.Split(cleanID, ":")
	imdbID = parts[0]
	if len(parts) > 2 {
		season, _ = strconv.Atoi(parts[1])
		episode, _ = strconv.Atoi(parts[2])
	}

	var meta database.TmdbMetadata
	err := database.DB.Where("imdb_id = ? OR tmdb_id = ?", imdbID, imdbID).First(&meta).Error
	if err != nil {
		// If metadata lookup fails, check if the query requested unlinked/pending ThreadHash streams
		var t database.Thread
		errThread := database.DB.Where("thread_hash = ?", imdbID).First(&t).Error
		if errThread == nil {
			// This is an unlinked thread. Return direct P2P fallback streams only (No RD mapping)
			streamList := make([]gin.H, 0)
			seenP2P := make(map[string]bool)

			for _, magnet := range t.MagnetURIs {
				parsedMagnet := parser.ParseMagnet(magnet, t.Type)
				if parsedMagnet == nil {
					continue
				}

				// De-duplicate stream entries by infohash
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

		c.JSON(http.StatusNotFound, gin.H{"error": "Metadata mapping not found"})
		return
	}

	var streams []database.Stream
	if season != -1 && episode != -1 {
		// Series path: search direct episode match OR full season packs where episode is NULL
		err = database.DB.Where("tmdb_id = ? AND ((season = ? AND episode <= ? AND episode_end >= ?) OR (season = ? AND episode IS NULL))",
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

	var cachedStreams []gin.H   // Instant ⚡ streams
	var uncachedStreams []gin.H // Downloading ⏳ streams

	// Allocation-Free: replaced string key concatenation with a comparable struct key
	seenStreams := make(map[streamDupKey]bool)

	isRDStr := "false"
	if p.IsEnabled() {
		isRDStr = "true"
	}

	for _, s := range streams {
		isCached := cacheMap[s.Infohash]

		// Deduplicate stream candidates based on (isRD | quality | language | infohash) using struct keys
		dupKey := streamDupKey{
			IsRD:     isRDStr,
			Quality:  s.Quality,
			Language: s.Language,
			Infohash: s.Infohash,
		}
		if seenStreams[dupKey] {
			continue
		}
		seenStreams[dupKey] = true

		emoji := "⏳"
		if isCached {
			emoji = "⚡ Instant"
		}

		label := fmt.Sprintf("[%s] TamilMV\n%s / %s", emoji, s.Quality, s.Language)

		targetEpStr := "movie"
		if season != -1 && episode != -1 {
			targetEpStr = fmt.Sprintf("%d-%d", season, episode)
		}

		rdUrl := fmt.Sprintf("%s/rd-add/%s/%s", cfg.AppHost, s.Infohash, targetEpStr)

		if p.IsEnabled() {
			item := gin.H{
				"name":  "TamilMV Addon",
				"title": label,
				"url":   rdUrl,
			}
			if isCached {
				cachedStreams = append(cachedStreams, item)
			} else {
				uncachedStreams = append(uncachedStreams, item)
			}
		} else {
			// Direct P2P Streams
			item := gin.H{
				"name":     label,
				"title":    "Direct Torrent P2P Stream",
				"infoHash": s.Infohash,
				"sources":  tracker.GetTrackers(),
			}
			uncachedStreams = append(uncachedStreams, item)
		}
	}

	// Order by: Cached (Instant) first, followed by Uncached (Downloading) streams
	streamList := append(cachedStreams, uncachedStreams...)

	c.JSON(http.StatusOK, gin.H{"streams": streamList})
}

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
			// Client disconnected/aborted player. Detach active polling and complete the download in the background
			// so that subsequent streaming requests load instantly.
			//
			// To completely prevent API rate-limiting / IP throttling, we enforce a strict 15-second slow-poll
			// interval for detached background runs, and check a global semaphore to cap total active background pollers.
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

				// Slow-poll interval: 15s instead of 3s to reduce debrid API queries by 500% !
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

			c.JSON(http.StatusClientClosedRequest, gin.H{"error": "Request cancelled by client. Cache polling detached to background."})
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
	torrentRecord.LastChecked = time.Now() // Set initial cache timestamp to now

	_ = database.DB.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "infohash"}},
		UpdateAll: true,
	}).Create(&torrentRecord).Error

	c.Redirect(http.StatusFound, finalLink)
}

// ── Stremio Route Helper Routines ──

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
	// 1. Try TorBox direct download link resolution
	if dlProvider, ok := p.(interface {
		GetDownloadLinkForFile(context.Context, string, string) (string, error)
	}); ok {
		return dlProvider.GetDownloadLinkForFile(ctx, info.ID, strconv.Itoa(fileID))
	}

	// 2. Real-Debrid / Fallback index-based link unrestriction
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

func buildTrackerSources() []string {
	trackers := tracker.GetTrackers()
	out := make([]string, 0, len(trackers))
	for _, t := range trackers {
		t = strings.TrimSpace(t)
		if t == "" {
			continue
		}
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
		out = append(out, "&tr="+url.QueryEscape(proto+"://"+rest))
	}
	return out
}
