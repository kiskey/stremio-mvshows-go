// Version: 2.0.0
// Change log: Replaced fragile string-based bypass with deterministic O(1) MagnetSetHash comparison. Extended zero-write bypass to all thread statuses. Isolated metadata-only updates from new-magnet stream writes. Integrated UpsertMagnetCache and UpsertTmdbMetadata to prevent TTL resets and disk wear.

package orchestrator

import (
    "fmt"
    "strings"
    "sync"
    "time"

    "github.com/kiskey/stremio-mvshows-go/internal/config"
    "github.com/kiskey/stremio-mvshows-go/internal/database"
    "github.com/kiskey/stremio-mvshows-go/internal/services/crawler"
    "github.com/kiskey/stremio-mvshows-go/internal/services/metadata"
    "github.com/kiskey/stremio-mvshows-go/internal/services/parser"
    "github.com/kiskey/stremio-mvshows-go/internal/utils"
    bolt "go.etcd.io/bbolt"
)

var (
    isCrawling     bool
    crawlMu        sync.Mutex
    dashboardCache DashboardStats
    cacheMu        sync.RWMutex
)

type DashboardStats struct {
    Linked      int64     `json:"linked"`
    Pending     int64     `json:"pending"`
    Failed      int64     `json:"failed"`
    LastUpdated time.Time `json:"lastUpdated"`
}

func IsCrawling() bool {
    crawlMu.Lock()
    defer crawlMu.Unlock()
    return isCrawling
}

func GetDashboardCache() DashboardStats {
    cacheMu.RLock()
    defer cacheMu.RUnlock()
    return dashboardCache
}

func UpdateDashboardCache() {
    var linked, pending, failed int64

    if database.DB != nil {
        _ = database.DB.View(func(tx *bolt.Tx) error {
            tb := tx.Bucket([]byte("threads"))
            if tb != nil {
                c := tb.Cursor()
                for k, v := c.First(); k != nil; k, v = c.Next() {
                    var t database.Thread
                    if err := database.DecodeGob(v, &t); err == nil {
                        if t.Status == "linked" {
                            linked++
                        } else if t.Status == "pending_tmdb" {
                            pending++
                        }
                    }
                }
            }

            ftb := tx.Bucket([]byte("failed_threads"))
            if ftb != nil {
                failed = int64(ftb.Stats().KeyN)
            }
            return nil
        })
    }

    cacheMu.Lock()
    dashboardCache = DashboardStats{
        Linked:      linked,
        Pending:     pending,
        Failed:      failed,
        LastUpdated: time.Now(),
    }
    cacheMu.Unlock()
}

func RunFullWorkflow(cfg *config.Config) {
    defer func() {
        if r := recover(); r != nil {
            utils.Logger.Error().Interface("panic", r).Msg("Recovered from panic inside RunFullWorkflow background thread.")
            crawlMu.Lock()
            isCrawling = false
            crawlMu.Unlock()
            UpdateDashboardCache()
        }
    }()

    crawlMu.Lock()
    if isCrawling {
        crawlMu.Unlock()
        utils.Logger.Warn().Msg("Workflow crawl already in progress. Skipping duplicate execution.")
        return
    }
    isCrawling = true
    crawlMu.Unlock()

    defer func() {
        crawlMu.Lock()
        isCrawling = false
        crawlMu.Unlock()
        UpdateDashboardCache()
        utils.Logger.Info().Msg("Full workflow execution cycle finished successfully.")
    }()

    utils.Logger.Info().Msg("Starting full crawling and processing workflow...")

    incremental := false
    if database.DB != nil {
        var count int64
        _ = database.DB.View(func(tx *bolt.Tx) error {
            b := tx.Bucket([]byte("catalog_index"))
            count = int64(b.Stats().KeyN)
            return nil
        })
        if count > 50 && !cfg.ForceFullScrape {
            incremental = true
        }
    }

    if incremental {
        utils.Logger.Info().
            Int("scrape_incremental_pages", cfg.IncrementalEndPage).
            Str("incremental_sort_order", cfg.IncrementalSortQuery).
            Msg("Database is already seeded. Running in Incremental Mode (quick scan of recent posts).")
    } else {
        utils.Logger.Info().
            Int("scrape_full_pages", cfg.ScrapeEndPage).
            Str("full_sort_order", cfg.ForumSortQuery).
            Msg("Database is empty or force-override is active. Running in Full Sync Mode.")
    }

    // 1. Standard Forum Index Crawl
    scraped, err := crawler.RunCrawler(cfg, incremental)
    if err != nil {
        utils.Logger.Error().Err(err).Msg("Crawler execution failed catastrophically.")
        return
    }

    // 2. Poll Active Monitored Series URLs directly to catch un-bumped thread edits
    activeMonitored, errMon := database.GetActiveMonitoredSeries()
    if errMon == nil && len(activeMonitored) > 0 {
        utils.Logger.Info().
            Int("active_monitored_count", len(activeMonitored)).
            Msg("Polling active watched webseries URLs directly to catch un-bumped episode updates...")

        for _, ms := range activeMonitored {
            if ms.URL == "" {
                continue
            }
            targetedScraped, errT := crawler.RunTargetedCrawler(cfg, ms.URL, "series", "top-series-from-forum")
            if errT == nil && len(targetedScraped) > 0 {
                scraped = append(scraped, targetedScraped...)
            }
        }
    }

    utils.Logger.Info().
        Int("count", len(scraped)).
        Msg("Forum crawl complete. Starting concurrent worker pool for thread metadata match processing...")

    tmdbClient := metadata.NewTMDBClient(cfg)

    // Worker Pool Concurrency Engine (5 parallel workers bounded by channel semaphore)
    workerConcurrency := 5
    sem := make(chan struct{}, workerConcurrency)
    var wg sync.WaitGroup

    for idx, thread := range scraped {
        wg.Add(1)
        sem <- struct{}{}

        go func(index int, item crawler.CrawledThread) {
            defer func() {
                <-sem
                wg.Done()
            }()

            processThread(item, tmdbClient, incremental)
        }(idx, thread)
    }

    wg.Wait()

    // 3. Auto-Archive dormant monitored series older than 30 days without updates
    if archivedCount, errArch := database.AutoArchiveInactiveSeries(nil, 30); errArch == nil && archivedCount > 0 {
        utils.Logger.Info().Int("archived_count", archivedCount).Msg("Dormant webseries auto-archived after 30 days of inactivity.")
    }

    utils.Logger.Info().Int("total_scraped", len(scraped)).Msg("Workflow thread processing complete.")
}

func isValidParsedTitle(parsed *parser.ParseResult) bool {
    if parsed == nil {
        return false
    }
    title := strings.TrimSpace(parsed.Title)
    if title == "" || len(title) < 1 {
        return false
    }

    words := strings.Fields(strings.ToLower(title))
    metadataCount := 0
    for _, w := range words {
        if parser.IsMetadataWord(w) {
            metadataCount++
        }
    }
    if len(words) > 0 && float64(metadataCount)/float64(len(words)) > 0.5 {
        return false
    }

    return true
}

// isThreadContentChanged uses the deterministic MagnetSetHash for O(1) comparison.
// It is immune to magnet URI formatting, tracker drift, and legacy uncleaned data.
func isThreadContentChanged(existing *database.Thread, incomingMagnets []string, rawTitle, threadURL string) bool {
    if existing == nil {
        return true
    }

    if existing.RawTitle != rawTitle {
        return true
    }

    if threadURL != "" && existing.URL != "" && existing.URL != threadURL {
        return true
    }

    incomingHash := parser.ComputeMagnetSetHash(incomingMagnets)
    storedHash := existing.MagnetSetHash

    // Backward compatibility: compute hash on the fly for legacy records missing MagnetSetHash
    if storedHash == "" && len(existing.MagnetURIs) > 0 {
        storedHash = parser.ComputeMagnetSetHash(existing.MagnetURIs)
    }

    return incomingHash != storedHash
}

func processThread(thread crawler.CrawledThread, tmdbClient *metadata.TMDBClient, incremental bool) {
    defer func() {
        if r := recover(); r != nil {
            utils.Logger.Error().
                Interface("panic", r).
                Str("title", thread.RawTitle).
                Msg("Recovered from panic during processThread processing.")
        }
    }()

    threadType := metadata.NormalizeMediaType(thread.Type)

    existing, errEx := database.FindThreadByHash(nil, thread.ThreadHash)
    hasExisting := (errEx == nil && existing != nil)

    prTitle := parser.ParseRelease(thread.RawTitle, threadType)
    parsed := &parser.ParseResult{
        Title:        prTitle.CleanTitle,
        Year:         prTitle.Year,
        Season:       prTitle.SeasonNumber,
        IsPack:       prTitle.IsSeasonPack,
        EpisodeStart: prTitle.EpisodeStart,
        EpisodeEnd:   prTitle.EpisodeEnd,
    }

    if parsed == nil || parsed.Title == "" {
        _ = database.LogFailedThread(nil, thread.ThreadHash, thread.RawTitle, "Title parsing failed critically")
        return
    }

    if !isValidParsedTitle(parsed) {
        _ = database.LogFailedThread(nil, thread.ThreadHash, thread.RawTitle,
            fmt.Sprintf("Parsed title invalid: %s", parsed.Title))
        return
    }

    // Clean incoming magnets and filter out rogue title divergence
    var cleanedMagnets []string
    for _, m := range thread.MagnetURIs {
        cleanM := parser.StripTrackersFromMagnet(m)
        dn := parser.ExtractMagnetDisplayName(m)
        if dn != "" {
            pmr := parser.ParseRelease(dn, threadType)
            if pmr != nil && pmr.IsValid && pmr.CleanTitle != "" {
                overlapParsed := metadata.OverlapCoefficient(parsed.Title, pmr.CleanTitle)
                existingCleanTitle := ""
                if existing != nil {
                    existingCleanTitle = existing.CleanTitle
                }
                if existingCleanTitle == "" {
                    existingCleanTitle = parsed.Title
                }
                overlapClean := metadata.OverlapCoefficient(existingCleanTitle, pmr.CleanTitle)
                if overlapParsed < 0.20 && overlapClean < 0.20 {
                    utils.Logger.Warn().
                        Str("thread_title", parsed.Title).
                        Str("magnet_title", pmr.CleanTitle).
                        Msg("In-thread title divergence detected. Dropping rogue magnet.")
                    continue
                }
            }
        }
        cleanedMagnets = append(cleanedMagnets, cleanM)
    }

    // ─── UNIFIED BYPASS LOGIC ───
    // Applies to ALL statuses (linked, pending_tmdb) to prevent unnecessary disk I/O
    if hasExisting {
        if !isThreadContentChanged(existing, cleanedMagnets, thread.RawTitle, thread.URL) {
            utils.Logger.Debug().
                Str("hash", thread.ThreadHash).
                Str("title", thread.RawTitle).
                Msg("Thread content fingerprint matches stored record. Bypassing all writes (0 disk I/O).")
            _ = database.DeleteFailedThread(nil, thread.ThreadHash)
            return
        }

        // Content has changed. Determine if magnets changed or just metadata (title/URL)
        incomingHash := parser.ComputeMagnetSetHash(cleanedMagnets)
        existingHash := existing.MagnetSetHash
        if existingHash == "" {
            existingHash = parser.ComputeMagnetSetHash(existing.MagnetURIs)
        }
        hasNewMagnets := incomingHash != existingHash

        // If thread is linked and only metadata changed (no new magnets),
        // update ONLY the thread record — don't touch streams/metadata/magnet_cache
        if existing.Status == "linked" && existing.TmdbID != nil && !hasNewMagnets {
            errTx := database.DB.Update(func(tx *bolt.Tx) error {
                existing.RawTitle = thread.RawTitle
                if thread.URL != "" {
                    existing.URL = thread.URL
                }
                existing.MagnetURIs = cleanedMagnets
                existing.MagnetSetHash = incomingHash
                existing.LastSeen = time.Now()
                existing.UpdatedAt = time.Now()
                return database.CreateOrUpdateThread(tx, existing)
            })
            if errTx == nil {
                utils.Logger.Info().
                    Str("hash", thread.ThreadHash).
                    Msg("Thread metadata updated (title/URL). Streams and metadata untouched — no new magnets detected.")
                _ = database.DeleteFailedThread(nil, thread.ThreadHash)
                return
            }
        }

        // New magnets detected OR thread not yet linked — proceed to full write path
        if existing.Status == "linked" && existing.TmdbID != nil {
            utils.Logger.Info().
                Str("hash", thread.ThreadHash).
                Str("raw_title", thread.RawTitle).
                Str("tmdb_id", *existing.TmdbID).
                Msg("Topic ID invariant match identified with updated magnets. Executing in-place thread update & stream merging...")

            errTx := database.DB.Update(func(tx *bolt.Tx) error {
                existing.MagnetURIs = cleanedMagnets
                existing.MagnetSetHash = incomingHash
                existing.RawTitle = thread.RawTitle
                existing.Type = threadType
                if thread.URL != "" {
                    existing.URL = thread.URL
                }
                existing.LastSeen = time.Now()
                existing.UpdatedAt = time.Now()

                errSave := database.CreateOrUpdateThread(tx, existing)
                if errSave != nil {
                    return errSave
                }

                isSeries := threadType == "series"
                var newStreams []database.Stream

                for _, magnet := range cleanedMagnets {
                    parsedMagnet := parser.ParseMagnet(magnet, threadType)
                    if parsedMagnet == nil {
                        continue
                    }

                    _ = database.UpsertMagnetCache(tx, parsedMagnet.Infohash, magnet)

                    stream := database.Stream{
                        TmdbID:    *existing.TmdbID,
                        Infohash:  parsedMagnet.Infohash,
                        Quality:   parsedMagnet.Quality,
                        Language:  parsedMagnet.Language,
                        CreatedAt: time.Now(),
                        UpdatedAt: time.Now(),
                    }

                    if isSeries {
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

                _ = database.DeleteFailedThread(tx, thread.ThreadHash)
                return nil
            })

            if errTx == nil {
                utils.Logger.Info().Str("title", thread.RawTitle).Msg("In-place thread update & stream merging completed successfully.")
                return
            }
        }
    }

    // ─── NEW LINK / PENDING_TMDB PATH ───
    var tmdbResult *metadata.TmdbResult
    var errTmdb error

    // Smart Local-First DB Lookup
    localMatch, errLocal := database.FindLinkedThreadByTitleYearType(nil, parsed.Title, parsed.Year, threadType)
    if errLocal == nil && localMatch != nil && localMatch.TmdbID != nil && *localMatch.TmdbID != "" {
        utils.Logger.Info().
            Str("title", parsed.Title).
            Int("year", parsed.Year).
            Str("type", threadType).
            Str("tmdb_id", *localMatch.TmdbID).
            Msg("Smart Local-First Match: Found existing linked DB record. Bypassing external TMDB HTTP call (0 API calls consumed).")

        tmdbResult = &metadata.TmdbResult{
            TmdbID: *localMatch.TmdbID,
            Title:  localMatch.CleanTitle,
            Year:   parsed.Year,
            Type:   threadType,
        }

        if localMatch.TmdbMetadata != nil && localMatch.TmdbMetadata.ImdbID != nil {
            tmdbResult.ImdbID = *localMatch.TmdbMetadata.ImdbID
        } else {
            _ = database.DB.View(func(tx *bolt.Tx) error {
                metaB := tx.Bucket([]byte("tmdb_metadata"))
                if metaB != nil {
                    data := metaB.Get([]byte(*localMatch.TmdbID))
                    if data != nil {
                        var m database.TmdbMetadata
                        if errDec := database.DecodeGob(data, &m); errDec == nil {
                            if m.ImdbID != nil {
                                tmdbResult.ImdbID = *m.ImdbID
                            }
                        }
                    }
                }
                return nil
            })
        }
    } else {
        // Fallback to external TMDB API search
        tmdbResult, errTmdb = tmdbClient.SearchWithAliases(parsed.Title, parsed.Year, threadType)
    }

    // AUTOMATED SCRAPER GUARDRAIL: Reject cross-type matches automatically
    if errTmdb == nil && tmdbResult != nil {
        if tmdbResult.Type != "" && !metadata.IsTypeMatch(tmdbResult.Type, threadType) {
            utils.Logger.Warn().
                Str("title", parsed.Title).
                Str("thread_type", threadType).
                Str("tmdb_type", tmdbResult.Type).
                Msg("Automated Scraper: TMDB result media type differs from forum thread type. Rejection enforced -> pending_tmdb.")
            errTmdb = fmt.Errorf("type mismatch: thread is %s but TMDB is %s", threadType, tmdbResult.Type)
            tmdbResult = nil
        }
    }

    if errTmdb != nil || tmdbResult == nil {
        utils.Logger.Warn().Err(errTmdb).Str("title", parsed.Title).Msg("TMDB lookup failed, low score, or type mismatch. Storing as pending_tmdb.")

        incomingHash := parser.ComputeMagnetSetHash(cleanedMagnets)
        pending := &database.Thread{
            ThreadHash:        thread.ThreadHash,
            RawTitle:          thread.RawTitle,
            CleanTitle:        parsed.Title,
            Status:            "pending_tmdb",
            Type:              threadType,
            PostedAt:          thread.PostedAt,
            Catalog:           thread.CatalogID,
            MagnetURIs:        cleanedMagnets,
            MagnetSetHash:     incomingHash,
            URL:               thread.URL,
            CustomDescription: nil,
            CustomPoster:      nil,
        }
        if parsed.Year > 0 {
            pending.Year = &parsed.Year
        }

        _ = database.DB.Update(func(tx *bolt.Tx) error {
            _ = database.DeleteFailedThread(tx, thread.ThreadHash)
            return database.CreateOrUpdateThread(tx, pending)
        })
        return
    }

    errTx := database.DB.Update(func(tx *bolt.Tx) error {
        metaBucket := tx.Bucket([]byte("tmdb_metadata"))

        if tmdbResult.ImdbID != "" {
            c := metaBucket.Cursor()
            for k, v := c.First(); k != nil; k, v = c.Next() {
                var fetched database.TmdbMetadata
                if errDec := database.DecodeGob(v, &fetched); errDec == nil {
                    if fetched.ImdbID != nil && *fetched.ImdbID == tmdbResult.ImdbID {
                        tmdbResult.TmdbID = fetched.TmdbID
                        break
                    }
                }
            }
        }

        rawDataBytes := []byte("{}")
        
        var imdbIDPtr *string
        if tmdbResult.ImdbID != "" {
            val := tmdbResult.ImdbID
            imdbIDPtr = &val
        }

        tmdbMetadata := database.TmdbMetadata{
            TmdbID:    tmdbResult.TmdbID,
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

        cleanTitle := tmdbResult.Title
        if cleanTitle == "" || strings.Contains(cleanTitle, "[") || strings.Contains(cleanTitle, "]") || strings.Contains(strings.ToLower(cleanTitle), "1080p") || strings.Contains(strings.ToLower(cleanTitle), "720p") || strings.Contains(strings.ToLower(cleanTitle), "s0") {
            parsedTitleFallback := parser.ParseTitle(thread.RawTitle, threadType)
            if parsedTitleFallback != nil && parsedTitleFallback.Title != "" {
                cleanTitle = parsedTitleFallback.Title
            } else {
                cleanTitle = thread.RawTitle
            }
        }

        incomingHash := parser.ComputeMagnetSetHash(cleanedMagnets)
        linkedThread := &database.Thread{
            ThreadHash:    thread.ThreadHash,
            RawTitle:      thread.RawTitle,
            CleanTitle:    cleanTitle,
            TmdbID:        &tmdbResult.TmdbID,
            Status:        "linked",
            Type:          threadType,
            PostedAt:      thread.PostedAt,
            Catalog:       thread.CatalogID,
            MagnetURIs:    cleanedMagnets,
            MagnetSetHash: incomingHash,
            URL:           thread.URL,
            CreatedAt:     time.Now(),
            UpdatedAt:     time.Now(),
        }
        if tmdbResult.Year > 0 {
            linkedThread.Year = &tmdbResult.Year
        }

        err := database.CreateOrUpdateThread(tx, linkedThread)
        if err != nil {
            return err
        }

        isSeries := threadType == "series"
        var streams []database.Stream

        for _, magnet := range cleanedMagnets {
            parsedMagnet := parser.ParseMagnet(magnet, threadType)
            if parsedMagnet == nil {
                continue
            }

            _ = database.UpsertMagnetCache(tx, parsedMagnet.Infohash, magnet)

            stream := database.Stream{
                TmdbID:    tmdbResult.TmdbID,
                Infohash:  parsedMagnet.Infohash,
                Quality:   parsedMagnet.Quality,
                Language:  parsedMagnet.Language,
                CreatedAt: time.Now(),
                UpdatedAt: time.Now(),
            }

            if isSeries {
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

            streams = append(streams, stream)
        }

        if len(streams) > 0 {
            err = database.CreateStreams(tx, streams) // Idempotent internally
            if err != nil {
                return err
            }
        }

        _ = database.DeleteFailedThread(tx, thread.ThreadHash)
        return nil
    })

    if errTx != nil {
        utils.Logger.Error().Err(errTx).Str("title", thread.RawTitle).Msg("Transaction failed while saving linked metadata.")
        _ = database.LogFailedThread(nil, thread.ThreadHash, thread.RawTitle, fmt.Sprintf("Tx Save Error: %s", errTx.Error()))
    } else {
        utils.Logger.Info().Str("title", thread.RawTitle).Msg("Successfully linked thread and saved stream references.")
    }
}

func RunTargetedWorkflow(cfg *config.Config, threadURL, contentType, catalogID string) error {
    targetType := metadata.NormalizeMediaType(contentType)
    utils.Logger.Info().Str("url", threadURL).Str("type", targetType).Msg("Initiating targeted thread recoup workflow...")

    scraped, err := crawler.RunTargetedCrawler(cfg, threadURL, targetType, catalogID)
    if err != nil {
        utils.Logger.Error().Err(err).Str("url", threadURL).Msg("Targeted crawler failed.")
        return err
    }

    if len(scraped) == 0 {
        return fmt.Errorf("no valid magnets detected on the targeted thread page")
    }

    tmdbClient := metadata.NewTMDBClient(cfg)
    for _, thread := range scraped {
        processThread(thread, tmdbClient, false)
    }

    utils.Logger.Info().Str("url", threadURL).Msg("Targeted thread recoup and indexing workflow completed successfully.")
    return nil
}
