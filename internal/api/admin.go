
// Version: 2.0.1
// Change log: Fixed undefined bolt namespace compiler error by explicitly aliasing go.etcd.io/bbolt import as bolt.

package api

import (
	"context"
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
	bolt "go.etcd.io/bbolt"
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
	r.GET("/cinemeta-search", cinemetaSearchHandler) // Parallel Cinemeta search catalog resolver
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
	if stat, err := os.Stat("/data/stremio_addon.db.bolt"); err == nil {
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

	t, errDb := database.FindThreadByID(uint(threadId))
	if errDb != nil || t == nil {
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
		if database.IsDebridCacheLocked(parsedMagnet.Infohash) {
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

	t, errDb := database.FindThreadByID(uint(body.ThreadID))
	if errDb != nil || t == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Thread not found"})
		return
	}

	t.CustomPoster = body.Poster
	t.CustomDescription = body.Desc

	if errSave := database.CreateOrUpdateThread(nil, t); errSave != nil {
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

	t, errDb := database.FindThreadByID(uint(body.ThreadID))
	if errDb != nil || t == nil {
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

	// This now triggers Cinemeta-First logic inside metadata.go if the ID begins with "tt"
	tmdbResult, errTmdb := tmdbClient.GetByID(idOnly, mediaType)
	if errTmdb != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Failed to resolve official ID on Cinemeta/TMDB: " + errTmdb.Error()})
		return
	}

	errTx := database.DB.Update(func(tx *bolt.Tx) error {
		metaBucket := tx.Bucket([]byte("tmdb_metadata"))
		threadBucket := tx.Bucket([]byte("threads"))
		magnetBucket := tx.Bucket([]byte("magnet_cache"))

		// Check if this IMDb ID is already registered under an alternative TMDB ID
		c := metaBucket.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var metadataRecord database.TmdbMetadata
			if errDec := database.DecodeGob(v, &metadataRecord); errDec == nil {
				if metadataRecord.ImdbID != nil && *metadataRecord.ImdbID == tmdbResult.ImdbID {
					tmdbResult.TmdbID = metadataRecord.TmdbID
					break
				}
			}
		}

		// Save tmdbMetadata
		rawDataBytes := []byte("{}")
		
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

		metaBytes, err := database.EncodeGob(tmdbMetadata)
		if err != nil {
			return err
		}
		_ = metaBucket.Put([]byte(tmdbResult.TmdbID), metaBytes)

		t.TmdbID = &tmdbResult.TmdbID
		
		// Write-Time Sanitation Failsafe:
		cleanTitle := tmdbResult.Title
		if cleanTitle == "" {
			parsed := parser.ParseTitle(t.RawTitle, t.Type)
			if parsed != nil && parsed.Title != "" {
				cleanTitle = parsed.Title
			} else {
				cleanTitle = t.RawTitle
			}
		}
		t.CleanTitle = cleanTitle
		t.Status = "linked"
		t.Type = mediaType
		if tmdbResult.Year > 0 {
			t.Year = &tmdbResult.Year
		}

		// Save updated thread
		tBytes, err := database.EncodeGob(t)
		if err != nil {
			return err
		}
		_ = threadBucket.Put([]byte(t.ThreadHash), tBytes)

		// Create chronological indexes
		idxB := tx.Bucket([]byte("catalog_index"))
		postedTime := time.Now()
		if t.PostedAt != nil {
			postedTime = *t.PostedAt
		}
		inverseTime := 9999999999 - postedTime.Unix()
		indexKey := fmt.Sprintf("cat:%s:%010d:%s", t.Catalog, inverseTime, t.ThreadHash)
		_ = idxB.Put([]byte(indexKey), []byte(t.ThreadHash))

		// Create associated streams
		var newStreams []database.Stream
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
			cacheBytes, _ := database.EncodeGob(cacheRecord)
			_ = magnetBucket.Put([]byte(parsedMagnet.Infohash), cacheBytes)

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
				}
			}

			newStreams = append(newStreams, stream)
		}

		if len(newStreams) > 0 {
			_ = database.CreateStreams(tx, newStreams)
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
	matchedTitles := make([]string, 0)
	var results []matchTaskResult
	var mu sync.Mutex

	sem := make(chan struct{}, 5)
	var wg sync.WaitGroup

	utils.Logger.Info().Int("total_queued", len(body.ThreadIDs)).Msg("Bulk auto-match request received. Commencing matching sequence...")

	for idx, id := range body.ThreadIDs {
		wg.Add(1)
		go func(index int, threadID int) {
			defer wg.Done()
			
			sem <- struct{}{}
			defer func() { <-sem }()

			t, errDb := database.FindThreadByID(uint(threadID))
			if errDb != nil || t == nil {
				utils.Logger.Warn().Int("thread_id", threadID).Msg("Thread ID not found in database. Skipping.")
				mu.Lock()
				failCount++
				mu.Unlock()
				return
			}

			// Clean the title using our newly optimized parser logic
			parsed := parser.ParseTitle(t.RawTitle, t.Type)
			if parsed == nil || parsed.Title == "" {
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
			results = append(results, matchTaskResult{Thread: *t, Result: tmdbResult})
			mu.Unlock()

		}(idx, id)
	}

	wg.Wait()

	utils.Logger.Info().Int("matched_queued", len(results)).Msg("Network search completed. Commencing serialized database writes...")

	// Serialize database writes one-by-one to completely prevent database locks
	for idx, res := range results {
		errTx := database.DB.Update(func(tx *bolt.Tx) error {
			metaBucket := tx.Bucket([]byte("tmdb_metadata"))
			threadBucket := tx.Bucket([]byte("threads"))
			magnetBucket := tx.Bucket([]byte("magnet_cache"))

			// GORM-safe Collision Pre-check: Verify if this IMDb ID is already registered under an alternative TMDB ID
			c := metaBucket.Cursor()
			for k, v := c.First(); k != nil; k, v = c.Next() {
				var fetched database.TmdbMetadata
				if errDec := database.DecodeGob(v, &fetched); errDec == nil {
					if fetched.ImdbID != nil && *fetched.ImdbID == res.Result.ImdbID {
						res.Result.TmdbID = fetched.TmdbID
						break
					}
				}
			}

			rawDataBytes := []byte("{}")
			
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

			metaBytes, err := database.EncodeGob(tmdbMetadata)
			if err != nil {
				return err
			}
			_ = metaBucket.Put([]byte(res.Result.TmdbID), metaBytes)

			res.Thread.TmdbID = &res.Result.TmdbID
			
			// Write-Time Sanitation Failsafe:
			cleanTitle := res.Result.Title
			if cleanTitle == "" {
				parsed := parser.ParseTitle(res.Thread.RawTitle, res.Thread.Type)
				if parsed != nil && parsed.Title != "" {
					cleanTitle = parsed.Title
				} else {
					cleanTitle = res.Thread.RawTitle
				}
			}
			res.Thread.CleanTitle = cleanTitle
			res.Thread.Status = "linked"
			if res.Result.Year > 0 {
				res.Thread.Year = &res.Result.Year
			}

			tBytes, err := database.EncodeGob(res.Thread)
			if err != nil {
				return err
			}
			_ = threadBucket.Put([]byte(res.Thread.ThreadHash), tBytes)

			// Maintain catalog indexes
			idxB := tx.Bucket([]byte("catalog_index"))
			postedTime := time.Now()
			if res.Thread.PostedAt != nil {
				postedTime = *res.Thread.PostedAt
			}
			inverseTime := 9999999999 - postedTime.Unix()
			indexKey := fmt.Sprintf("cat:%s:%010d:%s", res.Thread.Catalog, inverseTime, res.Thread.ThreadHash)
			_ = idxB.Put([]byte(indexKey), []byte(res.Thread.ThreadHash))

			var newStreams []database.Stream
			for _, magnet := range res.Thread.MagnetURIs {
				parsedMagnet := parser.ParseMagnet(magnet, res.Thread.Type)
				if parsedMagnet == nil {
					continue
				}

				cacheRecord := database.MagnetCache{
					Infohash: parsedMagnet.Infohash,
					Magnet:   magnet,
				}
				cacheBytes, _ := database.EncodeGob(cacheRecord)
				_ = magnetBucket.Put([]byte(parsedMagnet.Infohash), cacheBytes)

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
					}
				}

				newStreams = append(newStreams, stream)
			}

			if len(newStreams) > 0 {
				_ = database.CreateStreams(tx, newStreams)
			}

			_ = database.DeleteFailedThread(tx, res.Thread.ThreadHash)
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

func cinemetaSearchHandler(c *gin.Context) {
	query := c.Query("query")
	if query == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Query parameter is required"})
		return
	}

	cfg := config.Load()
	tmdbClient := metadata.NewTMDBClient(cfg)

	items, err := tmdbClient.SearchCinemeta(query)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Cinemeta lookup failed: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, items)
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

	if database.IsDebridCacheLocked(body.Infohash) {
		c.JSON(http.StatusConflict, gin.H{"message": "Cache operation already initiated / locked for this infohash."})
		return
	}

	_ = database.CreateDebridCacheLock(body.Infohash)

	// Retrieve original magnet from local cache database
	var cache database.MagnetCache
	errCache := database.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("magnet_cache"))
		data := b.Get([]byte(body.Infohash))
		if data == nil {
			return bolt.ErrBucketNotFound
		}
		return database.DecodeGob(data, &cache)
	})
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

	go func() {
		defer func() {
			if r := recover(); r != nil {
				utils.Logger.Error().
					Interface("panic", r).
					Str("infohash", body.Infohash).
					Msg("Unhandled panic rescued inside asynchronous cachePendingHandler worker goroutine.")
				_ = database.DeleteDebridCacheLock(body.Infohash)
			}
		}()

		utils.Logger.Info().Str("infohash", body.Infohash).Msg("Asynchronously caching pending magnet in debrid...")
		_, errAdd := p.AddAndSelect(context.Background(), cache.Magnet)
		if errAdd != nil {
			utils.Logger.Error().Err(errAdd).Str("infohash", body.Infohash).Msg("Asynchronous debrid cache-add failed.")
			_ = database.DeleteDebridCacheLock(body.Infohash)
		} else {
			utils.Logger.Info().Str("infohash", body.Infohash).Msg("Magnet submitted to debrid successfully.")
		}
	}()

	c.JSON(http.StatusOK, gin.H{"message": "Cache operation triggered in background successfully!"})
}

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

	_ = database.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("debrid_torrents"))
		for _, h := range body.Hashes {
			hLower := strings.ToLower(h)
			data := b.Get([]byte(hLower))
			if data != nil {
				var dt database.DebridTorrent
				if errDec := database.DecodeGob(data, &dt); errDec == nil {
					if dt.Status == "downloaded" {
						result[hLower] = true
					}
				}
			}
		}
		return nil
	})

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

	_ = database.DeleteFailedThread(nil, body.ThreadHash)

	orchestrator.UpdateDashboardCache()
	c.JSON(http.StatusOK, gin.H{"message": "Thread deleted from parse failures list. It will be re-processed on next crawl."})
}

func recentHandler(c *gin.Context) {
	linked, _ := database.GetRecentLinkedThreads()
	failures, _ := database.GetFailedThreads()

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
