// Version: 1.0.5
// Change log: Added "sync" to the imports block to resolve compiler errors for the bounded concurrency autoMatchHandler.

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

	var successCount int
	var failCount int
	var matchedTitles []string
	var mu sync.Mutex

	// Bounded Concurrency: limit parallel requests to maximum 5 workers to protect TMDB rate limits
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

			errTx := database.DB.Transaction(func(tx *gorm.DB) error {
				// GORM-safe Collision Pre-check: Verify if this IMDb ID is already registered under an alternative TMDB ID
				if tmdbResult.ImdbID != "" {
					var fetched []database.TmdbMetadata
					if tx.Where("imdb_id = ?", tmdbResult.ImdbID).Limit(1).Find(&fetched).Error == nil && len(fetched) > 0 {
						// Re-route local pointers to use the pre-existing record, completely avoiding UNIQUE constraints issues
						tmdbResult.TmdbID = fetched[0].TmdbID
					}
				}

				rawDataBytes, _ := json.Marshal(tmdbResult.RawData)
				
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
				t.Status = "linked"
				if tmdbResult.Year > 0 {
					t.Year = &tmdbResult.Year
				}

				errThr := tx.Save(&t).Error
				if errThr != nil {
					return errThr
				}

				for _, magnet := range t.MagnetURIs {
					parsedMagnet := parser.ParseMagnet(magnet, t.Type)
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
						TmdbID:   tmdbResult.TmdbID,
						Infohash: parsedMagnet.Infohash,
						Quality:  parsedMagnet.Quality,
						Language: parsedMagnet.Language,
					}

					if t.Type == "series" {
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

				_ = database.DeleteFailedThread(t.ThreadHash, tx)
				return nil
			})

			mu.Lock()
			if errTx == nil {
				utils.Logger.Info().
					Int("index", index+1).
					Str("raw_title", t.RawTitle).
					Str("matched_as", tmdbResult.Title).
					Str("imdb_id", tmdbResult.ImdbID).
					Msg("Successfully linked thread and saved stream references.")
				successCount++
				matchedTitles = append(matchedTitles, tmdbResult.Title)
			} else {
				utils.Logger.Error().
					Int("index", index+1).
					Str("raw_title", t.RawTitle).
					Err(errTx).
					Msg("Transaction failed while saving metadata to tables.")
				failCount++
			}
			mu.Unlock()

		}(idx, id)
	}

	wg.Wait()

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
		c.JSON(http.StatusBadRequest, gin.H{"error": "Debrid provider is currently disabled"})
		return
	}

	// Secure the asynchronous background caching goroutine with recovery handling to prevent server crashes.
	// Uses background context as the original request context will be canceled when the HTTP request terminates!
	go func() {
		defer func() {
			if r := recover(); r != nil {
				utils.Logger.Error().
					Interface("panic", r).
					Str("infohash", body.Infohash).
					Msg("Unhandled panic rescued inside asynchronous cachePendingHandler worker goroutine.")
				_ = database.DB.Where("infohash = ?", body.Infohash).Delete(&database.DebridCacheLock{})
			}
		}()

		utils.Logger.Info().Str("infohash", body.Infohash).Msg("Asynchronously caching pending magnet in debrid...")
		_, errAdd := p.AddAndSelect(context.Background(), cache.Magnet)
		if errAdd != nil {
			utils.Logger.Error().Err(errAdd).Str("infohash", body.Infohash).Msg("Asynchronous debrid cache-add failed.")
			// Delete lock so admin can try again
			_ = database.DB.Where("infohash = ?", body.Infohash).Delete(&database.DebridCacheLock{})
		} else {
			utils.Logger.Info().Str("infohash", body.Infohash).Msg("Magnet submitted to debrid successfully.")
		}
	}()

	c.JSON(http.StatusOK, gin.H{"message": "Cache operation triggered in background successfully!"})
}

// rdCheckHandler queries the local GORM database only to check the download cache status of torrent files.
func rdCheckHandler(c *gin.Context) {
	var body struct {
		Hashes []string `json:"hashes"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid payload format. Expected an array of hashes."})
		return
	}

	result := make(map[string]bool)
	for _, h := range body.Hashes {
		result[strings.ToLower(h)] = false
	}

	if len(body.Hashes) > 0 {
		var records []database.DebridTorrent
		err := database.DB.Where("infohash IN ? AND status = ?", body.Hashes, "downloaded").Find(&records).Error
		if err == nil {
			for _, r := range records {
				result[r.Infohash] = true
			}
		}
	}

	c.JSON(http.StatusOK, gin.H{"cached": result})
}

func failuresHandler(c *gin.Context) {
	failures, err := database.GetFailedThreads()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to retrieve parse failures"})
		return
	}
	c.JSON(http.StatusOK, failures)
}

func retryParseHandler(c *gin.Context) {
	var body struct {
		ThreadHash string `json:"threadHash"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid thread hash parameter"})
		return
	}

	// Remove from failures list
	_ = database.DeleteFailedThread(body.ThreadHash, nil)

	orchestrator.UpdateDashboardCache()
	c.JSON(http.StatusOK, gin.H{"message": "Thread deleted from parse failures list. It will be re-processed on next crawl."})
}

func recentHandler(c *gin.Context) {
	// Paginated linked threads and parse failures list
	var linked []database.Thread
	_ = database.DB.Where("status = ?", "linked").Order("updated_at DESC").Limit(15).Find(&linked)

	var failures []database.FailedThread
	_ = database.DB.Order("last_attempt DESC").Limit(15).Find(&failures)

	type activity struct {
		Title     string `json:"title"`
		UpdatedAt string `json:"updatedAt"`
	}

	linkedAct := make([]activity, len(linked))
	for idx, val := range linked {
		linkedAct[idx] = activity{
			Title:     val.CleanTitle,
			UpdatedAt: val.UpdatedAt.Format(time.RFC3339),
		}
	}

	failAct := make([]activity, len(failures))
	for idx, val := range failures {
		failAct[idx] = activity{
			Title:     val.RawTitle,
			UpdatedAt: val.LastAttempt.Format(time.RFC3339),
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"linked":   linkedAct,
		"failures": failAct,
	})
}
