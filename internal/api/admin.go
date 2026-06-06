// Version: 1.1.0
// Change log: Fixed c.Scanner typo to c.JSON inside failuresHandler to resolve the compilation failure.

package api

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/kiskey/stremio-mvshows-go/internal/config"
	"github.com/kiskey/stremio-mvshows-go/internal/database"
	"github.com/kiskey/stremio-mvshows-go/internal/services/debrid"
	"github.com/kiskey/stremio-mvshows-go/internal/services/metadata"
	"github.com/kiskey/stremio-mvshows-go/internal/services/orchestrator"
	"github.com/kiskey/stremio-mvshows-go/internal/services/parser"
	"github.com/kiskey/stremio-mvshows-go/internal/services/tracker"
	"github.com/kiskey/stremio-mvshows-go/internal/utils"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func RegisterAdminRoutes(r *gin.RouterGroup) {
	r.GET("/health", healthHandler)
	r.POST("/trigger-crawl", triggerCrawlHandler)
	r.GET("/pending", pendingThreadsHandler)
	r.GET("/pending/:threadId/streams", pendingStreamsHandler)
	r.POST("/custom-meta", customMetaHandler)
	r.POST("/link-official", linkOfficialHandler)
	r.POST("/auto-match", autoMatchHandler) // New endpoint for manual auto-matching
	r.POST("/rd-cache-pending", cachePendingHandler)
	r.POST("/rd-check", rdCheckHandler)
	r.GET("/failures", failuresHandler)
	r.POST("/retry-parse", retryParseHandler)
	r.GET("/recent", recentHandler)
}

func healthHandler(c *gin.Context) {
	cfg := config.Load()
	p := debrid.GetProvider(cfg)

	cacheCheck := "database"
	if p.IsEnabled() {
		// If TorBox is configured, it supports instant API cache checks
		if cfg.DebridService == "torbox" {
			cacheCheck = "instant"
		}
	}

	dbSize := int64(0)
	if stat, err := os.Stat("/data/stremio_addon.db"); err == nil {
		dbSize = stat.Size()
	}

	stats := orchestrator.GetDashboardCache()
	if stats.LastUpdated.IsZero() {
		orchestrator.UpdateDashboardCache()
		stats = orchestrator.GetDashboardCache()
	}

	c.JSON(http.StatusOK, gin.H{
		"isCrawling":         orchestrator.IsCrawling(),
		"lastUpdated":        stats.LastUpdated.Format(time.RFC3339),
		"debridService":      cfg.DebridService,
		"debridCacheCheck":   cacheCheck,
		"realDebridEnabled":  cfg.IsRDEnabled,
		"torboxEnabled":      cfg.IsTorboxEnabled,
		"tmdbConfigured":     cfg.TMDBAPIKey != "",
		"trackerCount":       len(tracker.GetTrackers()),
		"dbSizeBytes":        dbSize,
		"linked":             stats.Linked,
		"pending":            stats.Pending,
		"failed":             stats.Failed,
	})
}

func triggerCrawlHandler(c *gin.Context) {
	cfg := config.Load()
	if orchestrator.IsCrawling() {
		c.JSON(http.StatusConflict, gin.H{"error": "A crawling workflow is already in progress"})
		return
	}

	// Trigger crawl asynchronously
	go orchestrator.RunFullWorkflow(cfg)

	c.JSON(http.StatusAccepted, gin.H{"message": "Manual crawl triggered successfully"})
}

func pendingThreadsHandler(c *gin.Context) {
	threads, err := database.GetPendingThreads()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to retrieve pending threads"})
		return
	}
	c.JSON(http.StatusOK, threads)
}

func pendingStreamsHandler(c *gin.Context) {
	threadIdStr := c.Param("threadId")
	threadId, err := strconv.Atoi(threadIdStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid thread ID"})
		return
	}

	var t database.Thread
	if errDb := database.DB.First(&t, "id = ?", threadId).Error; errDb != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Thread not found"})
		return
	}

	type streamItem struct {
		Label    string `json:"label"`
		Infohash string `json:"infohash"`
		Quality  string `json:"quality"`
		Language string `json:"language"`
	}

	items := make([]streamItem, 0)
	var locked []string

	for _, magnet := range t.MagnetURIs {
		parsedMagnet := parser.ParseMagnet(magnet, t.Type)
		if parsedMagnet == nil {
			continue
		}

		items = append(items, streamItem{
			Label:    parsedMagnet.Infohash,
			Infohash: parsedMagnet.Infohash,
			Quality:  parsedMagnet.Quality,
			Language: parsedMagnet.Language,
		})

		// Check lock status
		var lock database.DebridCacheLock
		if errLock := database.DB.First(&lock, "infohash = ?", parsedMagnet.Infohash).Error; errLock == nil {
			locked = append(locked, parsedMagnet.Infohash)
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"items":  items,
		"locked": locked,
	})
}

func customMetaHandler(c *gin.Context) {
	var body struct {
		ThreadID int     `json:"threadId"`
		Poster   *string `json:"poster"`
		Desc     *string `json:"description"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid payload parameters"})
		return
	}

	var t database.Thread
	if errDb := database.DB.First(&t, "id = ?", body.ThreadID).Error; errDb != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Thread not found"})
		return
	}

	t.CustomPoster = body.Poster
	t.CustomDescription = body.Desc

	if errSave := database.DB.Save(&t).Error; errSave != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update custom metadata"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Custom metadata updated successfully"})
}

func linkOfficialHandler(c *gin.Context) {
	var body struct {
		ThreadID   int    `json:"threadId"`
		OfficialID string `json:"officialId"` // tt... or tv:123 or movie:123
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid payload parameters"})
		return
	}

	var t database.Thread
	if errDb := database.DB.First(&t, "id = ?", body.ThreadID).Error; errDb != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Thread not found"})
		return
	}

	cfg := config.Load()
	tmdbClient := metadata.NewTMDBClient(cfg)

	mediaType := t.Type
	idOnly := body.OfficialID

	// Standardize formats e.g. tv:123 or movie:123
	if strings.Contains(idOnly, ":") {
		parts := strings.Split(idOnly, ":")
		mediaType = parts[0]
		idOnly = parts[1]
	}

	tmdbResult, errTmdb := tmdbClient.GetByID(idOnly, mediaType)
	if errTmdb != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Failed to resolve official ID on TMDB: " + errTmdb.Error()})
		return
	}

	errTx := database.DB.Transaction(func(tx *gorm.DB) error {
		// GORM-safe Collision Pre-check: Verify if this IMDb ID is already registered under an alternative TMDB ID
		if tmdbResult.ImdbID != "" {
			var fetched []database.TmdbMetadata
			if tx.Where("imdb_id = ?", tmdbResult.ImdbID).Limit(1).Find(&fetched).Error == nil && len(fetched) > 0 {
				// Re-route local pointers to use the pre-existing record, completely avoiding UNIQUE constraints issues
				tmdbResult.TmdbID = fetched[0].TmdbID
			}
		}

		// Save TmdbMetadata records
		rawDataBytes, _ := json.Marshal(tmdbResult.RawData)
		
		// CRITICAL FIX: Convert empty IMDb string to explicit NULL pointer for SQLite unique constraint
		var imdbIDPtr *string
		if tmdbResult.ImdbID != "" {
			val := tmdbResult.ImdbID
			imdbIDPtr = &val
		}

		tmdbMetadata := database.TmdbMetadata{
			TmdbID: tmdbResult.TmdbID,
			ImdbID: imdbIDPtr,
			Data:   string(rawDataBytes),
		}
		if tmdbResult.Year > 0 {
			tmdbMetadata.Year = &tmdbResult.Year
		}

		errMeta := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "tmdb_id"}},
			UpdateAll: true,
		}).Create(&tmdbMetadata).Error
		if errMeta != nil {
			return errMeta
		}

		t.TmdbID = &tmdbResult.TmdbID
		t.CleanTitle = tmdbResult.Title // Overwrite old dirty crawled title with the clean TMDB standard
		t.Status = "linked"
		t.Type = mediaType
		if tmdbResult.Year > 0 {
			t.Year = &tmdbResult.Year
		}

		errThr := tx.Save(&t).Error
		if errThr != nil {
			return errThr
		}

		// Create associated streams
		for _, magnet := range t.MagnetURIs {
			parsedMagnet := parser.ParseMagnet(magnet, t.Type)
			if parsedMagnet == nil {
				continue
			}

			// Store cache record
			cacheRecord := database.MagnetCache{
				Infohash: parsedMagnet.Infohash,
				Magnet:   magnet,
			}
			_ = tx.Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "infohash"}},
				UpdateAll: true,
			}).Create(&cacheRecord)

			stream := database.Stream{
				TmdbID:   tmdbResult.TmdbID,
				Infohash: parsedMagnet.Infohash,
				Quality:  parsedMagnet.Quality,
				Language: parsedMagnet.Language,
			}

			if mediaType == "series" {
				seasonVal := parsedMagnet.Season
				if seasonVal == 0 {
					seasonVal = 1
				}
				stream.Season = &seasonVal

				if parsedMagnet.Type == "SINGLE_EPISODE" {
					epVal := parsedMagnet.Episode
					stream.Episode = &epVal
					stream.EpisodeEnd = &epVal
				} else if parsedMagnet.Type == "EPISODE_PACK" {
					startVal := parsedMagnet.EpisodeStart
					endVal := parsedMagnet.EpisodeEnd
					stream.Episode = &startVal
					stream.EpisodeEnd = &endVal
				} else {
					stream.Episode = nil
					stream.EpisodeEnd = nil
				}
			}

			_ = tx.Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "tmdb_id"}, {Name: "season"}, {Name: "episode"}, {Name: "infohash"}},
				UpdateAll: true,
			}).Create(&stream)
		}

		return nil
	})

	if errTx != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed transaction during manual linking: " + errTx.Error()})
		return
	}

	orchestrator.UpdateDashboardCache()
	c.JSON(http.StatusOK, gin.H{"message": "Thread manually linked to official metadata successfully!"})
}

// autoMatchHandler handles manual trigger of auto-matching on selected thread IDs using clean title parsing.
// Overhauled with bounded concurrency to process bulk queues cleanly under proxy timeouts.
func autoMatchHandler(c *gin.Context) {
	var body struct {
		ThreadIDs []int `json:"threadIds"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid payload parameters"})
		return
	}

	if len(body.ThreadIDs) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "No thread IDs provided"})
		return
	}

	cfg := config.Load()
	tmdbClient := metadata.NewTMDBClient(cfg)

	type matchTaskResult struct {
		Thread database.Thread
		Result *metadata.TmdbResult
	}

	var successCount int
	var failCount int
	matchedTitles := make([]string, 0) // Explicitly initialized using make to prevent null JSON serialization on 0 matches
	var results []matchTaskResult
	var mu sync.Mutex

	// Bounded Concurrency: limit parallel API requests to maximum 5 workers to protect TMDB rate limits
	sem := make(chan struct{}, 5)
	var wg sync.WaitGroup

	utils.Logger.Info().Int("total_queued", len(body.ThreadIDs)).Msg("Bulk auto-match request received. Commencing matching sequence...")

	for idx, id := range body.ThreadIDs {
		wg.Add(1)
		go func(index int, threadID int) {
			defer wg.Done()
			
			sem <- struct{}{}
			defer func() { <-sem }()

			var t database.Thread
			if errDb := database.DB.First(&t, "id = ?", threadID).Error; errDb != nil {
				utils.Logger.Warn().Int("thread_id", threadID).Msg("Thread ID not found in database. Skipping.")
				mu.Lock()
				failCount++
				mu.Unlock()
				return
			}

			// Clean the title using our newly optimized parser logic
			parsed := parser.ParseTitle(t.RawTitle)
			if parsed == nil || parsed.Title == "" {
				utils.Logger.Warn().Str("raw_title", t.RawTitle).Msg("Parsing title failed (returned empty). Storing in failure register.")
				mu.Lock()
				failCount++
				mu.Unlock()
				return
			}

			tmdbResult, errTmdb := tmdbClient.Search(parsed.Title, parsed.Year, t.Type)
			if errTmdb != nil {
				utils.Logger.Warn().
					Int("index", index+1).
					Str("clean_title", parsed.Title).
					Int("year", parsed.Year).
					Err(errTmdb).
					Msg("TMDB search returned no confident match.")
				mu.Lock()
				failCount++
				mu.Unlock()
				return
			}

			mu.Lock()
			results = append(results, matchTaskResult{Thread: t, Result: tmdbResult})
			mu.Unlock()

		}(idx, id)
	}

	wg.Wait()

	utils.Logger.Info().Int("matched_queued", len(results)).Msg("Network search completed. Commencing serialized database writes...")

	// Serialize database writes one-by-one to completely prevent SQLite database is locked (SQLITE_BUSY) errors
	for idx, res := range results {
		errTx := database.DB.Transaction(func(tx *gorm.DB) error {
			// GORM-safe Collision Pre-check: Verify if this IMDb ID is already registered under an alternative TMDB ID
			if res.Result.ImdbID != "" {
				var fetched []database.TmdbMetadata
				if tx.Where("imdb_id = ?", res.Result.ImdbID).Limit(1).Find(&fetched).Error == nil && len(fetched) > 0 {
					// Re-route local pointers to use the pre-existing record, completely avoiding UNIQUE constraints issues
					res.Result.TmdbID = fetched[0].TmdbID
				}
			}

			rawDataBytes, _ := json.Marshal(res.Result.RawData)
			
			var imdbIDPtr *string
			if res.Result.ImdbID != "" {
				val := res.Result.ImdbID
				imdbIDPtr = &val
			}

			tmdbMetadata := database.TmdbMetadata{
				TmdbID: res.Result.TmdbID,
				ImdbID: imdbIDPtr,
				Data:   string(rawDataBytes),
			}
			if res.Result.Year > 0 {
				tmdbMetadata.Year = &res.Result.Year
			}

			errMeta := tx.Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "tmdb_id"}},
				UpdateAll: true,
			}).Create(&tmdbMetadata).Error
			if errMeta != nil {
				return errMeta
			}

			res.Thread.TmdbID = &res.Result.TmdbID
			res.Thread.CleanTitle = res.Result.Title // Overwrite old dirty crawled title with the clean TMDB standard
			res.Thread.Status = "linked"
			if res.Result.Year > 0 {
				res.Thread.Year = &res.Result.Year
			}

			errThr := tx.Save(&res.Thread).Error
			if errThr != nil {
				return errThr
			}

			for _, magnet := range res.Thread.MagnetURIs {
				parsedMagnet := parser.ParseMagnet(magnet, res.Thread.Type)
				if parsedMagnet == nil {
					continue
				}

				cacheRecord := database.MagnetCache{
					Infohash: parsedMagnet.Infohash,
					Magnet:   magnet,
				}
				_ = tx.Clauses(clause.OnConflict{
					Columns:   []clause.Column{{Name: "infohash"}},
					UpdateAll: true,
				}).Create(&cacheRecord)

				stream := database.Stream{
					TmdbID:   res.Result.TmdbID,
					Infohash: parsedMagnet.Infohash,
					Quality:  parsedMagnet.Quality,
					Language: parsedMagnet.Language,
				}

				if res.Thread.Type == "series" {
					seasonVal := parsedMagnet.Season
					if seasonVal == 0 {
						seasonVal = 1
					}
					stream.Season = &seasonVal

					if parsedMagnet.Type == "SINGLE_EPISODE" {
						epVal := parsedMagnet.Episode
						stream.Episode = &epVal
						stream.EpisodeEnd = &epVal
					} else if parsedMagnet.Type == "EPISODE_PACK" {
						startVal := parsedMagnet.EpisodeStart
						endVal := parsedMagnet.EpisodeEnd
						stream.Episode = &startVal
						stream.EpisodeEnd = &endVal
					} else {
						stream.Episode = nil
						stream.EpisodeEnd = nil
					}
				}

				_ = tx.Clauses(clause.OnConflict{
					Columns:   []clause.Column{{Name: "tmdb_id"}, {Name: "season"}, {Name: "episode"}, {Name: "infohash"}},
					UpdateAll: true,
				}).Create(&stream)
			}

			_ = database.DeleteFailedThread(res.Thread.ThreadHash, tx)
			return nil
		})

		if errTx == nil {
			utils.Logger.Info().
				Int("index", idx+1).
				Str("raw_title", res.Thread.RawTitle).
				Str("matched_as", res.Result.Title).
				Str("imdb_id", res.Result.ImdbID).
				Msg("Successfully linked thread and saved stream references.")
			successCount++
			matchedTitles = append(matchedTitles, res.Result.Title)
		} else {
			utils.Logger.Error().
				Int("index", idx+1).
				Str("raw_title", res.Thread.RawTitle).
				Err(errTx).
				Msg("Transaction failed while saving metadata to tables.")
			failCount++
		}
	}

	// Real-time progress end log
	utils.Logger.Info().
		Int("success_count", successCount).
		Int("fail_count", failCount).
		Msg("Bulk auto-match sequence completed.")

	orchestrator.UpdateDashboardCache()

	c.JSON(http.StatusOK, gin.H{
		"successCount":  successCount,
		"failCount":     failCount,
		"matchedTitles": matchedTitles,
	})
}

func cachePendingHandler(c *gin.Context) {
	var body struct {
		ThreadID int    `json:"threadId"`
		Infohash string `json:"infohash"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid payload parameters"})
		return
	}

	var lock database.DebridCacheLock
	errLock := database.DB.First(&lock, "infohash = ?", body.Infohash).Error
	if errLock == nil {
		c.JSON(http.StatusConflict, gin.H{"message": "Cache operation already initiated / locked for this infohash."})
		return
	}

	// Create duplicate lock mapping
	_ = database.DB.Create(&database.DebridCacheLock{Infohash: body.Infohash})

	// Retrieve original magnet to add to debrid asynchronously
	var cache database.MagnetCache
	errCache := database.DB.Where("infohash = ?", body.Infohash).First(&cache).Error
	if errCache != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Original magnet not found in cache database"})
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
	torrentID := info.ID // Store ID in a persistent string variable to prevent nil pointer reference on transient errors

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

	// Real-Debrid / Fallback index-based link unrestrict
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
