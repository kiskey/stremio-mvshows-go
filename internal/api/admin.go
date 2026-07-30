// Version: 3.0.2
// Change log: Fixed Go compiler error (undefined err) in linkOfficialHandler and autoMatchHandler by properly declaring err with :=.

package api

import (
    "context"
    "fmt"
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
    r.POST("/auto-match", autoMatchHandler)
    r.POST("/rd-cache-pending", cachePendingHandler)
    r.POST("/rd-check", rdCheckHandler)
    r.GET("/failures", failuresHandler)
    r.POST("/retry-parse", retryParseHandler)
    r.GET("/recent", recentHandler)
    r.GET("/cinemeta-search", cinemetaSearchHandler)
    r.POST("/parse-preview", parsePreviewHandler)

    r.GET("/purge-lookup", purgeLookupHandler)
    r.POST("/purge-confirm", purgeConfirmHandler)
    r.POST("/trigger-targeted-crawl", triggerTargetedCrawlHandler)

    // Panel F Monitored Series Watchlist Routes
    r.GET("/monitored-series", monitoredSeriesHandler)
    r.POST("/monitored-series/toggle", toggleMonitoredSeriesHandler)
    r.POST("/monitored-series/bulk-toggle", bulkToggleMonitoredSeriesHandler)
    r.POST("/monitored-series/add", addMonitoredSeriesHandler)
    r.GET("/series-search", seriesSearchHandler)

    // Panel G Entity Relation Visualizer Route
    r.GET("/visualize-tree", visualizeTreeHandler)
}

func healthHandler(c *gin.Context) {
    cfg := config.Load()
    p := debrid.GetProvider(cfg)

    cacheCheck := "database"
    if p.IsEnabled() {
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
        "isCrawling":        orchestrator.IsCrawling(),
        "lastUpdated":       stats.LastUpdated.Format(time.RFC3339),
        "debridService":     cfg.DebridService,
        "debridCacheCheck":  cacheCheck,
        "realDebridEnabled": cfg.IsRDEnabled,
        "torboxEnabled":     cfg.IsTorboxEnabled,
        "tmdbConfigured":    cfg.TMDBAPIKey != "",
        "trackerCount":      len(tracker.GetTrackers()),
        "dbSizeBytes":       dbSize,
        "linked":            stats.Linked,
        "pending":           stats.Pending,
        "failed":            stats.Failed,
    })
}

func triggerCrawlHandler(c *gin.Context) {
    cfg := config.Load()
    if orchestrator.IsCrawling() {
        c.JSON(http.StatusConflict, gin.H{"error": "A crawling workflow is already in progress"})
        return
    }
    go orchestrator.RunFullWorkflow(cfg)
    c.JSON(http.StatusAccepted, gin.H{"message": "Manual crawl triggered successfully"})
}

func setAntiCacheHeaders(c *gin.Context) {
    c.Header("Cache-Control", "no-cache, no-store, must-revalidate")
    c.Header("Pragma", "no-cache")
    c.Header("Expires", "0")
}

func pendingThreadsHandler(c *gin.Context) {
    setAntiCacheHeaders(c)
    threads, err := database.GetPendingThreads()
    if err != nil {
        c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to retrieve pending threads"})
        return
    }
    c.JSON(http.StatusOK, threads)
}

func pendingStreamsHandler(c *gin.Context) {
    setAntiCacheHeaders(c)
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

        displayLabel := t.CleanTitle
        if displayLabel == "" {
            parsed := parser.ParseTitle(t.RawTitle, t.Type)
            if parsed != nil && parsed.Title != "" {
                displayLabel = parsed.Title
            } else {
                displayLabel = t.RawTitle
            }
        }

        items = append(items, streamItem{
            Label:    displayLabel,
            Infohash: parsedMagnet.Infohash,
            Quality:  parsedMagnet.Quality,
            Language: parsedMagnet.Language,
        })

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

    if body.Poster != nil && strings.TrimSpace(*body.Poster) == "" {
        t.CustomPoster = nil
    } else {
        t.CustomPoster = body.Poster
    }

    if body.Desc != nil && strings.TrimSpace(*body.Desc) == "" {
        t.CustomDescription = nil
    } else {
        t.CustomDescription = body.Desc
    }

    if errSave := database.CreateOrUpdateThread(nil, t); errSave != nil {
        c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update custom metadata"})
        return
    }

    c.JSON(http.StatusOK, gin.H{"message": "Custom metadata updated successfully"})
}

func linkOfficialHandler(c *gin.Context) {
    var body struct {
        ThreadID   int    `json:"threadId"`
        OfficialID string `json:"officialId"` // e.g. "tt13464516" or "movie:550" or "series:282258"
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

    // Admin Classification Override Direction: Default to thread's current type or respect explicit prefix
    targetType := metadata.NormalizeMediaType(t.Type)
    idOnly := body.OfficialID

    if strings.Contains(idOnly, ":") {
        parts := strings.Split(idOnly, ":")
        targetType = metadata.NormalizeMediaType(parts[0]) // Apply explicit admin type override
        idOnly = parts[1]
    }

    tmdbResult, errTmdb := tmdbClient.GetByID(idOnly, targetType)
    if errTmdb != nil {
        c.JSON(http.StatusBadRequest, gin.H{"error": "Failed to resolve official ID on Cinemeta/TMDB: " + errTmdb.Error()})
        return
    }

    // Verify resolved metadata type matches expected target type
    if tmdbResult.Type != "" && !metadata.IsTypeMatch(tmdbResult.Type, targetType) {
        c.JSON(http.StatusBadRequest, gin.H{
            "error": fmt.Sprintf("Media type mismatch: requested '%s', but resolved ID %s is a '%s'", targetType, body.OfficialID, tmdbResult.Type),
        })
        return
    }

    errTx := database.DB.Update(func(tx *bolt.Tx) error {
        streamsBucket := tx.Bucket([]byte("streams"))

        primaryTmdbID := tmdbResult.TmdbID
        secondaryImdbID := tmdbResult.ImdbID

        if primaryTmdbID == "" && secondaryImdbID != "" {
            primaryTmdbID = secondaryImdbID
        }

        // SURGICAL STREAM PURGE: Remove this specific thread's streams from old TmdbID to prevent ghost streams
        if t.TmdbID != nil && *t.TmdbID != "" && *t.TmdbID != primaryTmdbID {
            oldTmdbID := *t.TmdbID
            if streamsBucket != nil {
                for _, magnet := range t.MagnetURIs {
                    parsedMagnet := parser.ParseMagnet(magnet, t.Type)
                    if parsedMagnet != nil {
                        oldCompositeKey := fmt.Sprintf("%s:%s", oldTmdbID, strings.ToLower(parsedMagnet.Infohash))
                        _ = streamsBucket.Delete([]byte(oldCompositeKey))
                    }
                }
            }
        }

        rawDataBytes := []byte("{}")
        
        var imdbIDPtr *string
        if secondaryImdbID != "" {
            imdbIDPtr = &secondaryImdbID
        }

        tmdbMetadata := database.TmdbMetadata{
            TmdbID:    primaryTmdbID,
            ImdbID:    imdbIDPtr,
            Data:      string(rawDataBytes),
            CreatedAt: time.Now(),
            UpdatedAt: time.Now(),
        }
        if tmdbResult.Year > 0 {
            tmdbMetadata.Year = &tmdbResult.Year
        }

        // Idempotent metadata write
        _ = database.UpsertTmdbMetadata(tx, tmdbMetadata)

        t.TmdbID = &primaryTmdbID
        t.Type = targetType // Apply admin type override
        t.Status = "linked"

        cleanTitle := tmdbResult.Title
        if cleanTitle == "" || strings.Contains(cleanTitle, "[") || strings.Contains(cleanTitle, "]") || strings.Contains(strings.ToLower(cleanTitle), "1080p") || strings.Contains(strings.ToLower(cleanTitle), "720p") || strings.Contains(strings.ToLower(cleanTitle), "s0") {
            parsed := parser.ParseTitle(t.RawTitle, t.Type)
            if parsed != nil && parsed.Title != "" {
                cleanTitle = parsed.Title
            } else {
                cleanTitle = t.RawTitle
            }
        }
        t.CleanTitle = cleanTitle
        if tmdbResult.Year > 0 {
            t.Year = &tmdbResult.Year
        }

        var cleanedMagnets []string
        for _, m := range t.MagnetURIs {
            cleanM := parser.StripTrackersFromMagnet(m)
            dn := parser.ExtractMagnetDisplayName(m)
            if dn != "" {
                pmr := parser.ParseRelease(dn, t.Type)
                if pmr != nil && pmr.IsValid && pmr.CleanTitle != "" {
                    overlapClean := metadata.OverlapCoefficient(cleanTitle, pmr.CleanTitle)
                    if overlapClean < 0.20 {
                        utils.Logger.Warn().
                            Str("clean_title", cleanTitle).
                            Str("magnet_title", pmr.CleanTitle).
                            Msg("In-thread title divergence detected during manual link. Dropping rogue magnet link.")
                        continue
                    }
                }
            }
            cleanedMagnets = append(cleanedMagnets, cleanM)
        }
        t.MagnetURIs = cleanedMagnets
        t.MagnetSetHash = parser.ComputeMagnetSetHash(cleanedMagnets)

        err := database.CreateOrUpdateThread(tx, t)
        if err != nil {
            return err
        }

        var newStreams []database.Stream
        for _, magnet := range t.MagnetURIs {
            parsedMagnet := parser.ParseMagnet(magnet, t.Type)
            if parsedMagnet == nil {
                continue
            }

            // Idempotent magnet cache write
            _ = database.UpsertMagnetCache(tx, parsedMagnet.Infohash, magnet)

            stream := database.Stream{
                TmdbID:    primaryTmdbID,
                Infohash:  parsedMagnet.Infohash,
                Quality:   parsedMagnet.Quality,
                Language:  parsedMagnet.Language,
                CreatedAt: time.Now(),
                UpdatedAt: time.Now(),
            }

            if targetType == "series" {
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
            _ = database.CreateStreams(tx, newStreams) // Idempotent internally
        }

        _ = database.DeleteFailedThread(tx, t.ThreadHash)
        return nil
    })

    if errTx != nil {
        c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed transaction during manual linking: " + errTx.Error()})
        return
    }

    orchestrator.UpdateDashboardCache()
    c.JSON(http.StatusOK, gin.H{"message": "Thread manually linked successfully!"})
}

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

            threadType := metadata.NormalizeMediaType(t.Type)

            parsed := parser.ParseTitle(t.RawTitle, threadType)
            if parsed == nil || parsed.Title == "" {
                mu.Lock()
                failCount++
                mu.Unlock()
                return
            }

            cleanTitle := strings.TrimSpace(parsed.Title)
            if cleanTitle == "" || len(cleanTitle) < 1 {
                mu.Lock()
                failCount++
                mu.Unlock()
                return
            }

            words := strings.Fields(strings.ToLower(cleanTitle))
            metadataCount := 0
            for _, w := range words {
                if parser.IsMetadataWord(w) {
                    metadataCount++
                }
            }
            if len(words) > 0 && float64(metadataCount)/float64(len(words)) > 0.5 {
                mu.Lock()
                failCount++
                mu.Unlock()
                return
            }

            utils.Logger.Info().
                Int("index", index+1).
                Str("raw_title", t.RawTitle).
                Str("clean_title", parsed.Title).
                Int("year", parsed.Year).
                Msg("Processing thread for auto-match")

            tmdbResult, errTmdb := tmdbClient.SearchWithAliases(parsed.Title, parsed.Year, threadType)
            if errTmdb != nil || tmdbResult == nil {
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

            // Strict media type assertion
            if tmdbResult.Type != "" && !metadata.IsTypeMatch(tmdbResult.Type, threadType) {
                utils.Logger.Warn().
                    Int("index", index+1).
                    Str("raw_title", t.RawTitle).
                    Str("thread_type", threadType).
                    Str("matched_type", tmdbResult.Type).
                    Msg("Auto-match candidate rejected due to media type mismatch.")
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

    utils.Logger.Info().Int("matched_queued", len(results)).Msg("Network search completed. Commencing transactional database writes...")

    for idx, res := range results {
        errTx := database.DB.Update(func(tx *bolt.Tx) error {
            streamsBucket := tx.Bucket([]byte("streams"))

            threadType := metadata.NormalizeMediaType(res.Thread.Type)

            primaryTmdbID := res.Result.TmdbID
            secondaryImdbID := res.Result.ImdbID

            if primaryTmdbID == "" && secondaryImdbID != "" {
                primaryTmdbID = secondaryImdbID
            }

            // SURGICAL STREAM PURGE: Remove this specific thread's streams from old TmdbID before relinking
            if res.Thread.TmdbID != nil && *res.Thread.TmdbID != "" && *res.Thread.TmdbID != primaryTmdbID {
                oldTmdbID := *res.Thread.TmdbID
                if streamsBucket != nil {
                    for _, magnet := range res.Thread.MagnetURIs {
                        parsedMagnet := parser.ParseMagnet(magnet, res.Thread.Type)
                        if parsedMagnet != nil {
                            oldCompositeKey := fmt.Sprintf("%s:%s", oldTmdbID, strings.ToLower(parsedMagnet.Infohash))
                            _ = streamsBucket.Delete([]byte(oldCompositeKey))
                        }
                    }
                }
            }

            rawDataBytes := []byte("{}")
            var imdbIDPtr *string
            if secondaryImdbID != "" {
                imdbIDPtr = &secondaryImdbID
            }

            tmdbMetadata := database.TmdbMetadata{
                TmdbID:    primaryTmdbID,
                ImdbID:    imdbIDPtr,
                Data:      string(rawDataBytes),
                CreatedAt: time.Now(),
                UpdatedAt: time.Now(),
            }
            if res.Result.Year > 0 {
                tmdbMetadata.Year = &res.Result.Year
            }

            // Idempotent metadata write
            _ = database.UpsertTmdbMetadata(tx, tmdbMetadata)

            res.Thread.TmdbID = &primaryTmdbID
            res.Thread.Type = threadType
            
            cleanTitle := res.Result.Title
            if cleanTitle == "" || strings.Contains(cleanTitle, "[") || strings.Contains(cleanTitle, "]") || strings.Contains(strings.ToLower(cleanTitle), "1080p") || strings.Contains(strings.ToLower(cleanTitle), "720p") || strings.Contains(strings.ToLower(cleanTitle), "s0") {
                parsed := parser.ParseTitle(res.Thread.RawTitle, threadType)
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

            var cleanedMagnets []string
            for _, m := range res.Thread.MagnetURIs {
                cleanM := parser.StripTrackersFromMagnet(m)
                dn := parser.ExtractMagnetDisplayName(m)
                if dn != "" {
                    pmr := parser.ParseRelease(dn, threadType)
                    if pmr != nil && pmr.IsValid && pmr.CleanTitle != "" {
                        overlapClean := metadata.OverlapCoefficient(cleanTitle, pmr.CleanTitle)
                        if overlapClean < 0.20 {
                            utils.Logger.Warn().
                                Str("clean_title", cleanTitle).
                                Str("magnet_title", pmr.CleanTitle).
                                Msg("In-thread title divergence detected during auto-match. Dropping rogue magnet link.")
                            continue
                        }
                    }
                }
                cleanedMagnets = append(cleanedMagnets, cleanM)
            }
            res.Thread.MagnetURIs = cleanedMagnets
            res.Thread.MagnetSetHash = parser.ComputeMagnetSetHash(cleanedMagnets)

            err := database.CreateOrUpdateThread(tx, &res.Thread)
            if err != nil {
                return err
            }

            var newStreams []database.Stream
            for _, magnet := range res.Thread.MagnetURIs {
                parsedMagnet := parser.ParseMagnet(magnet, threadType)
                if parsedMagnet == nil {
                    continue
                }

                // Idempotent magnet cache write
                _ = database.UpsertMagnetCache(tx, parsedMagnet.Infohash, magnet)

                stream := database.Stream{
                    TmdbID:    primaryTmdbID,
                    Infohash:  parsedMagnet.Infohash,
                    Quality:   parsedMagnet.Quality,
                    Language:  parsedMagnet.Language,
                    CreatedAt: time.Now(),
                    UpdatedAt: time.Now(),
                }

                if threadType == "series" {
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
                _ = database.CreateStreams(tx, newStreams) // Idempotent internally
            }

            _ = database.DeleteFailedThread(tx, res.Thread.ThreadHash)
            return nil
        })

        if errTx == nil {
            utils.Logger.Info().
                Int("index", idx+1).
                Str("raw_title", res.Thread.RawTitle).
                Str("clean_title", res.Thread.CleanTitle).
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

func parsePreviewHandler(c *gin.Context) {
    var body struct {
        Title       string `json:"title"`
        ContentType string `json:"contentType"`
    }
    if err := c.ShouldBindJSON(&body); err != nil {
        c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid preview payload parameters"})
        return
    }

    targetType := metadata.NormalizeMediaType(body.ContentType)
    parsed := parser.ParseRelease(body.Title, targetType)

    c.JSON(http.StatusOK, gin.H{
        "rawTitle":        parsed.ReleaseTitle,
        "cleanTitle":      parsed.CleanTitle,
        "year":            parsed.Year,
        "season":          parsed.SeasonNumber,
        "episodes":        parsed.EpisodeNumbers,
        "isSeasonPack":    parsed.IsSeasonPack,
        "quality":         parsed.Quality.FullString,
        "source":          parsed.Source,
        "resolution":      parsed.Resolution,
        "languages":       parsed.Languages,
        "releaseGroup":    parsed.ReleaseGroup,
        "edition":         parsed.Edition.EditionString,
        "specialTags":     parsed.SpecialTags,
        "videoCodec":      parsed.VideoCodec,
        "audioCodec":      parsed.AudioCodec,
        "audioChannels":   parsed.AudioChannels,
        "isValid":         parsed.IsValid,
        "validationError": parsed.ValidationError,
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

    normalizedInfohash := strings.ToLower(body.Infohash)

    if database.IsDebridCacheLocked(normalizedInfohash) {
        c.JSON(http.StatusConflict, gin.H{"message": "Cache operation already initiated / locked for this infohash."})
        return
    }

    _ = database.CreateDebridCacheLock(normalizedInfohash)

    var cache database.MagnetCache
    errCache := database.DB.View(func(tx *bolt.Tx) error {
        b := tx.Bucket([]byte("magnet_cache"))
        data := b.Get([]byte(normalizedInfohash))
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
                    Str("infohash", normalizedInfohash).
                    Msg("Unhandled panic rescued inside asynchronous cachePendingHandler worker goroutine.")
                _ = database.DeleteDebridCacheLock(normalizedInfohash)
            }
        }()

        utils.Logger.Info().Str("infohash", normalizedInfohash).Msg("Asynchronously caching pending magnet in debrid...")
        _, errAdd := p.AddAndSelect(context.Background(), cache.Magnet)
        if errAdd != nil {
            utils.Logger.Error().Err(errAdd).Str("infohash", normalizedInfohash).Msg("Asynchronous debrid cache-add failed.")
            _ = database.DeleteDebridCacheLock(normalizedInfohash)
        } else {
            utils.Logger.Info().Str("infohash", normalizedInfohash).Msg("Magnet submitted to debrid successfully.")
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
    setAntiCacheHeaders(c)
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
    setAntiCacheHeaders(c)
    pageStr := c.DefaultQuery("page", "1")
    limitStr := c.DefaultQuery("limit", "15")

    page, errP := strconv.Atoi(pageStr)
    limit, errL := strconv.Atoi(limitStr)
    if errP != nil || page < 1 {
        page = 1
    }
    if errL != nil || limit < 1 {
        limit = 15
    }

    offset := (page - 1) * limit

    linked, _ := database.GetRecentLinkedThreadsPaginated(offset, limit)
    failures, _ := database.GetFailedThreadsPaginated(offset, limit)

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

func purgeLookupHandler(c *gin.Context) {
    imdbID := c.Query("imdbId")
    if imdbID == "" {
        c.JSON(http.StatusBadRequest, gin.H{"error": "imdbId parameter is required"})
        return
    }

    var meta database.TmdbMetadata
    var foundMeta bool

    _ = database.DB.View(func(tx *bolt.Tx) error {
        metaB := tx.Bucket([]byte("tmdb_metadata"))
        data := metaB.Get([]byte(imdbID))
        if data != nil {
            if errDec := database.DecodeGob(data, &meta); errDec == nil {
                foundMeta = true
            }
        }
        return nil
    })

    if !foundMeta {
        c.JSON(http.StatusNotFound, gin.H{"found": false, "message": "No database records currently linked to this IMDb ID."})
        return
    }

    type threadInfo struct {
        ID       uint   `json:"id"`
        RawTitle string `json:"rawTitle"`
        Status   string `json:"status"`
        Hash     string `json:"hash"`
    }
    var threads []threadInfo
    var title string

    _ = database.DB.View(func(tx *bolt.Tx) error {
        tb := tx.Bucket([]byte("threads"))
        c := tb.Cursor()
        for k, v := c.First(); k != nil; k, v = c.Next() {
            var t database.Thread
            if err := database.DecodeGob(v, &t); err == nil {
                if t.TmdbID != nil && (*t.TmdbID == meta.TmdbID || (meta.ImdbID != nil && *t.TmdbID == *meta.ImdbID)) {
                    threads = append(threads, threadInfo{
                        ID:       t.ID,
                        RawTitle: t.RawTitle,
                        Status:   t.Status,
                        Hash:     t.ThreadHash,
                    })
                    if title == "" {
                        title = t.CleanTitle
                    }
                }
            }
        }
        return nil
    })

    if title == "" {
        title = "Unknown Linked Title"
    }

    streams, _ := database.FindMovieStreams(nil, meta.TmdbID)

    c.JSON(http.StatusOK, gin.H{
        "found":        true,
        "imdbId":       imdbID,
        "tmdbId":       meta.TmdbID,
        "title":        title,
        "threads":      threads,
        "streamsCount": len(streams),
    })
}

func purgeConfirmHandler(c *gin.Context) {
    var body struct {
        ImdbID string `json:"imdbId"`
    }
    if err := c.ShouldBindJSON(&body); err != nil {
        c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid payload parameters"})
        return
    }

    if body.ImdbID == "" {
        c.JSON(http.StatusBadRequest, gin.H{"error": "imdbId parameter is required"})
        return
    }

    var meta database.TmdbMetadata
    var foundMeta bool

    _ = database.DB.View(func(tx *bolt.Tx) error {
        metaB := tx.Bucket([]byte("tmdb_metadata"))
        data := metaB.Get([]byte(body.ImdbID))
        if data != nil {
            if errDec := database.DecodeGob(data, &meta); errDec == nil {
                foundMeta = true
            }
        }
        return nil
    })

    if !foundMeta {
        c.JSON(http.StatusNotFound, gin.H{"error": "No metadata record found for this IMDb ID."})
        return
    }

    var threadsToDelete []database.Thread
    _ = database.DB.View(func(tx *bolt.Tx) error {
        tb := tx.Bucket([]byte("threads"))
        c := tb.Cursor()
        for k, v := c.First(); k != nil; k, v = c.Next() {
            var t database.Thread
            if err := database.DecodeGob(v, &t); err == nil {
                if t.TmdbID != nil && (*t.TmdbID == meta.TmdbID || (meta.ImdbID != nil && *t.TmdbID == *meta.ImdbID)) {
                    threadsToDelete = append(threadsToDelete, t)
                }
            }
        }
        return nil
    })

    var deleteCount int
    for _, t := range threadsToDelete {
        err := database.DeleteThread(nil, &t)
        if err == nil {
            deleteCount++
        }
    }

    _ = database.DB.Update(func(tx *bolt.Tx) error {
        _ = database.DeleteStreamsByTmdbID(tx, meta.TmdbID)
        if meta.ImdbID != nil {
            _ = database.DeleteStreamsByTmdbID(tx, *meta.ImdbID)
        }
        
        _ = tx.Bucket([]byte("tmdb_thread_index")).Delete([]byte(meta.TmdbID))
        if meta.ImdbID != nil {
            _ = tx.Bucket([]byte("tmdb_thread_index")).Delete([]byte(*meta.ImdbID))
        }
        
        metaB := tx.Bucket([]byte("tmdb_metadata"))
        _ = metaB.Delete([]byte(meta.TmdbID))
        if meta.ImdbID != nil && *meta.ImdbID != "" {
            _ = metaB.Delete([]byte(*meta.ImdbID))
        }
        return nil
    })

    orchestrator.UpdateDashboardCache()

    c.JSON(http.StatusOK, gin.H{
        "message": fmt.Sprintf("Successfully purged %d thread(s) and all associated streams/metadata for IMDb ID %s.", deleteCount, body.ImdbID),
    })
}

func triggerTargetedCrawlHandler(c *gin.Context) {
    var body struct {
        ThreadURL   string `json:"threadUrl"`
        ContentType string `json:"contentType"`
        CatalogID   string `json:"catalogId"`
    }
    if err := c.ShouldBindJSON(&body); err != nil {
        c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid payload parameters"})
        return
    }

    if body.ThreadURL == "" {
        c.JSON(http.StatusBadRequest, gin.H{"error": "threadUrl parameter is required"})
        return
    }
    if body.ContentType == "" {
        c.JSON(http.StatusBadRequest, gin.H{"error": "contentType parameter is required"})
        return
    }
    if body.CatalogID == "" {
        c.JSON(http.StatusBadRequest, gin.H{"error": "catalogId parameter is required"})
        return
    }

    cfg := config.Load()
    targetType := metadata.NormalizeMediaType(body.ContentType)
    canonicalURL := parser.FormatCanonicalTopicURL(body.ThreadURL)

    err := orchestrator.RunTargetedWorkflow(cfg, canonicalURL, targetType, body.CatalogID)
    if err != nil {
        c.JSON(http.StatusInternalServerError, gin.H{"error": "Targeted crawl failed: " + err.Error()})
        return
    }

    c.JSON(http.StatusOK, gin.H{"message": "Thread crawled, metadata mapped, and streams indexed successfully!"})
}

// ── Panel F Monitored Series Handlers ──

func monitoredSeriesHandler(c *gin.Context) {
    setAntiCacheHeaders(c)
    list, err := database.GetMonitoredSeriesList()
    if err != nil {
        c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to retrieve monitored series list"})
        return
    }
    c.JSON(http.StatusOK, list)
}

func toggleMonitoredSeriesHandler(c *gin.Context) {
    var body struct {
        ThreadHash string `json:"threadHash"`
        Status     string `json:"status"` // "active", "paused", "archived", "delete"
    }
    if err := c.ShouldBindJSON(&body); err != nil || body.ThreadHash == "" {
        c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid thread hash parameter"})
        return
    }

    if body.Status == "delete" {
        _ = database.DeleteMonitoredSeries(nil, body.ThreadHash)
        c.JSON(http.StatusOK, gin.H{"message": "Series removed from monitored watchlist."})
        return
    }

    ms, errMs := database.GetMonitoredSeriesByHash(nil, body.ThreadHash)
    if errMs != nil || ms == nil {
        c.JSON(http.StatusNotFound, gin.H{"error": "Monitored series record not found"})
        return
    }

    ms.Status = body.Status
    ms.LastUpdated = time.Now()
    if errSave := database.SetMonitoredSeries(nil, ms); errSave != nil {
        c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update series status"})
        return
    }

    c.JSON(http.StatusOK, gin.H{"message": "Series status updated to " + body.Status})
}

func bulkToggleMonitoredSeriesHandler(c *gin.Context) {
    var body struct {
        ThreadHashes []string `json:"threadHashes"`
        Status       string   `json:"status"` // "active", "paused", "archived", "delete"
    }
    if err := c.ShouldBindJSON(&body); err != nil || len(body.ThreadHashes) == 0 || body.Status == "" {
        c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid payload parameters. Expected threadHashes array and target status."})
        return
    }

    validStatuses := map[string]bool{"active": true, "paused": true, "archived": true, "delete": true}
    if !validStatuses[body.Status] {
        c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid target status parameter."})
        return
    }

    count, err := database.BulkSetMonitoredSeriesStatus(nil, body.ThreadHashes, body.Status)
    if err != nil {
        c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed bulk operation: " + err.Error()})
        return
    }

    c.JSON(http.StatusOK, gin.H{
        "message": fmt.Sprintf("Successfully updated %d series to status '%s'.", count, body.Status),
        "count":   count,
    })
}

func addMonitoredSeriesHandler(c *gin.Context) {
    var body struct {
        ThreadID   int    `json:"threadId"`
        ThreadURL  string `json:"threadUrl"`
        ThreadHash string `json:"threadHash"`
    }
    if err := c.ShouldBindJSON(&body); err != nil {
        c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid payload parameters"})
        return
    }

    canonicalURL := parser.FormatCanonicalTopicURL(body.ThreadURL)

    var targetThread *database.Thread

    if body.ThreadHash != "" {
        t, err := database.FindThreadByHash(nil, body.ThreadHash)
        if err == nil && t != nil {
            targetThread = t
        }
    } else if body.ThreadID > 0 {
        t, err := database.FindThreadByID(uint(body.ThreadID))
        if err == nil && t != nil {
            targetThread = t
        }
    }

    if targetThread != nil {
        if canonicalURL != "" {
            targetThread.URL = canonicalURL
            _ = database.CreateOrUpdateThread(nil, targetThread)
        }
        _ = database.AutoEnrollSeries(nil, targetThread)

        ms, errMs := database.GetMonitoredSeriesByHash(nil, targetThread.ThreadHash)
        if errMs == nil && ms != nil {
            ms.Status = "active"
            ms.LastUpdated = time.Now()
            _ = database.SetMonitoredSeries(nil, ms)
        }

        c.JSON(http.StatusOK, gin.H{"message": "Series enrolled into monitored watchlist successfully!"})
        return
    }

    if canonicalURL != "" {
        cfg := config.Load()
        errCrawl := orchestrator.RunTargetedWorkflow(cfg, canonicalURL, "series", "top-series-from-forum")
        if errCrawl != nil {
            c.JSON(http.StatusInternalServerError, gin.H{"error": "Targeted crawl failed for URL: " + errCrawl.Error()})
            return
        }

        scrapedThread, errTr := database.FindThreadByRawTitle(nil, canonicalURL)
        if errTr == nil && scrapedThread != nil {
            _ = database.AutoEnrollSeries(nil, scrapedThread)
            ms, errMs := database.GetMonitoredSeriesByHash(nil, scrapedThread.ThreadHash)
            if errMs == nil && ms != nil {
                ms.Status = "active"
                ms.LastUpdated = time.Now()
                _ = database.SetMonitoredSeries(nil, ms)
            }
        }

        c.JSON(http.StatusOK, gin.H{"message": "Thread crawled and enrolled into monitored watchlist successfully!"})
        return
    }

    c.JSON(http.StatusBadRequest, gin.H{"error": "Provide either a valid Thread ID, Thread Hash, or Thread URL."})
}

func seriesSearchHandler(c *gin.Context) {
    setAntiCacheHeaders(c)
    query := strings.ToLower(strings.TrimSpace(c.Query("q")))
    if query == "" {
        c.JSON(http.StatusOK, []gin.H{})
        return
    }

    type searchMatch struct {
        ID         uint   `json:"id"`
        ThreadHash string `json:"thread_hash"`
        RawTitle   string `json:"raw_title"`
        CleanTitle string `json:"clean_title"`
        Status     string `json:"status"`
        URL        string `json:"url"`
        Monitored  string `json:"monitored_status"`
    }

    monitoredList, _ := database.GetMonitoredSeriesList()
    monitoredMap := make(map[string]string)
    for _, ms := range monitoredList {
        monitoredMap[ms.ThreadHash] = ms.Status
    }

    var matches []searchMatch

    _ = database.DB.View(func(tx *bolt.Tx) error {
        tb := tx.Bucket([]byte("threads"))
        if tb == nil {
            return nil
        }
        c := tb.Cursor()
        for k, v := c.First(); k != nil; k, v = c.Next() {
            var t database.Thread
            if err := database.DecodeGob(v, &t); err == nil {
                if metadata.NormalizeMediaType(t.Type) == "series" {
                    cleanLower := strings.ToLower(t.CleanTitle)
                    rawLower := strings.ToLower(t.RawTitle)
                    if strings.Contains(cleanLower, query) || strings.Contains(rawLower, query) {
                        mStatus := "none"
                        if val, ok := monitoredMap[t.ThreadHash]; ok {
                            mStatus = val
                        }
                        matches = append(matches, searchMatch{
                            ID:         t.ID,
                            ThreadHash: t.ThreadHash,
                            RawTitle:   t.RawTitle,
                            CleanTitle: t.CleanTitle,
                            Status:     t.Status,
                            URL:        t.URL,
                            Monitored:  mStatus,
                        })
                        if len(matches) >= 15 {
                            break
                        }
                    }
                }
            }
        }
        return nil
    })

    c.JSON(http.StatusOK, matches)
}

// ── Panel G: Entity Relation Visualizer Handler ──

func visualizeTreeHandler(c *gin.Context) {
    setAntiCacheHeaders(c)
    query := strings.TrimSpace(c.Query("q"))
    contentType := strings.TrimSpace(c.Query("type"))

    if query == "" {
        c.JSON(http.StatusBadRequest, gin.H{"error": "Query parameter 'q' is required (Title, IMDb ID, or TMDB ID)."})
        return
    }

    targetType := metadata.NormalizeMediaType(contentType)
    graph, err := database.GetEntityGraphData(nil, query, targetType)
    if err != nil {
        c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to traverse entity graph: " + err.Error()})
        return
    }

    c.JSON(http.StatusOK, graph)
}
