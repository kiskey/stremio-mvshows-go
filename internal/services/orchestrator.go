// Version: 1.2.0
// Change log: Integrated standard parse validator metrics check prior to querying metadata lookup indexes (Fixes Problem 11 / Q10).

package orchestrator

import (
	"fmt"
	"strings"
	"sync"
	"time"
	"unicode"

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

	scraped, err := crawler.RunCrawler(cfg, incremental)
	if err != nil {
		utils.Logger.Error().Err(err).Msg("Crawler execution failed catastrophically.")
		return
	}

	utils.Logger.Info().Int("count", len(scraped)).Msg("Forum crawl complete. Starting sequential thread metadata match processing.")
	tmdbClient := metadata.NewTMDBClient(cfg)

	for _, thread := range scraped {
		processThread(thread, tmdbClient, incremental)
	}

	utils.Logger.Info().Int("total_scraped", len(scraped)).Msg("Workflow thread processing complete.")
}

func isValidParsedTitle(parsed *parser.ParseResult) bool {
	if parsed == nil {
		return false
	}
	if strings.TrimSpace(parsed.Title) == "" {
		return false
	}
	if len(parsed.Title) <= 1 {
		return false
	}
	if isAllNumbers(parsed.Title) {
		return false
	}
	if isAllUppercase(parsed.Title) && len(parsed.Title) > 3 {
		return false
	}

	words := strings.Fields(strings.ToLower(parsed.Title))
	metadataCount := 0
	for _, w := range words {
		if isMetadataWord(w) {
			metadataCount++
		}
	}
	if len(words) > 0 && float64(metadataCount)/float64(len(words)) > 0.5 {
		return false
	}

	return true
}

func isAllNumbers(s string) bool {
	for _, r := range s {
		if !unicode.IsDigit(r) && !unicode.IsSpace(r) {
			return false
		}
	}
	return true
}

func isAllUppercase(s string) bool {
	hasLetter := false
	for _, r := range s {
		if unicode.IsLetter(r) {
			hasLetter = true
			if unicode.IsLower(r) {
				return false
			}
		}
	}
	return hasLetter
}

func isMetadataWord(s string) bool {
	metadataWords := []string{
		"proper", "repack", "extended", "unrated", "remastered",
		"x264", "x265", "hevc", "avc", "aac", "ac3", "dts",
		"720p", "1080p", "2160p", "480p", "4k", "uhd",
		"webdl", "webrip", "bluray", "hdtv", "dvdrip",
		"gb", "mb", "kb", "esub", "sub", "subs",
	}
	s = strings.ToLower(s)
	for _, w := range metadataWords {
		if s == w {
			return true
		}
	}
	return false
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

	var existing database.Thread
	var hasExisting bool
	_ = database.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("threads"))
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var t database.Thread
			if err := database.DecodeGob(v, &t); err == nil {
				if t.RawTitle == thread.RawTitle {
					existing = t
					hasExisting = true
					break
				}
			}
		}
		return nil
	})

	if hasExisting {
		if existing.ThreadHash == thread.ThreadHash {
			if existing.Status == "pending_tmdb" && !incremental {
				// Retry
			} else {
				return
			}
		}

		if existing.ThreadHash != thread.ThreadHash {
			_ = database.DB.Update(func(tx *bolt.Tx) error {
				_ = tx.Bucket([]byte("threads")).Delete([]byte(existing.ThreadHash))

				if existing.TmdbID != nil {
					_ = tx.Bucket([]byte("streams")).Delete([]byte(*existing.TmdbID))
					_ = tx.Bucket([]byte("tmdb_thread_index")).Delete([]byte(*existing.TmdbID))
					
					idxB := tx.Bucket([]byte("catalog_index"))
					if existing.Catalog != "" {
						oldPosted := time.Now()
						if existing.PostedAt != nil {
							oldPosted = *existing.PostedAt
						}
						oldInverse := 9999999999 - oldPosted.Unix()
						oldIndexKey := fmt.Sprintf("cat:%s:%s:%010d:%s", existing.Catalog, existing.Type, oldInverse, existing.ThreadHash)
						_ = idxB.Delete([]byte(oldIndexKey))
					}
				}
				return nil
			})
		}
	}

	parsed := parser.ParseTitle(thread.RawTitle, thread.Type)
	if parsed == nil || parsed.Title == "" {
		_ = database.LogFailedThread(nil, thread.ThreadHash, thread.RawTitle, "Title parsing failed critically")
		return
	}

	if !isValidParsedTitle(parsed) {
		_ = database.LogFailedThread(nil, thread.ThreadHash, thread.RawTitle,
			fmt.Sprintf("Parsed title invalid: %s", parsed.Title))
		return
	}

	utils.Logger.Debug().
		Str("raw", thread.RawTitle).
		Str("clean", parsed.Title).
		Int("year", parsed.Year).
		Msg("Title parsed successfully")

	tmdbResult, errTmdb := tmdbClient.SearchWithAliases(parsed.Title, parsed.Year, thread.Type)
	if errTmdb != nil {
		utils.Logger.Warn().Err(errTmdb).Str("title", parsed.Title).Msg("TMDB lookup failed or score below threshold. Storing as pending_tmdb.")

		pending := &database.Thread{
			ThreadHash:        thread.ThreadHash,
			RawTitle:          thread.RawTitle,
			CleanTitle:        parsed.Title,
			Status:            "pending_tmdb",
			Type:              thread.Type,
			PostedAt:          thread.PostedAt,
			Catalog:           thread.CatalogID,
			MagnetURIs:        thread.MagnetURIs,
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
		magnetBucket := tx.Bucket([]byte("magnet_cache"))

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

		metaBytes, err := database.EncodeGob(tmdbMetadata)
		if err != nil {
			return err
		}
		err = metaBucket.Put([]byte(tmdbResult.TmdbID), metaBytes)
		if err != nil {
			return err
		}

		if tmdbMetadata.ImdbID != nil && *tmdbMetadata.ImdbID != "" {
			_ = metaBucket.Put([]byte(*tmdbMetadata.ImdbID), metaBytes)
		}

		cleanTitle := tmdbResult.Title
		if cleanTitle == "" || strings.Contains(cleanTitle, "[") || strings.Contains(cleanTitle, "]") || strings.Contains(strings.ToLower(cleanTitle), "1080p") || strings.Contains(strings.ToLower(cleanTitle), "720p") || strings.Contains(strings.ToLower(cleanTitle), "s0") {
			parsed := parser.ParseTitle(thread.RawTitle, thread.Type)
			if parsed != nil && parsed.Title != "" {
				cleanTitle = parsed.Title
			} else {
				cleanTitle = thread.RawTitle
			}
		}

		var cleanedMagnets []string
		for _, m := range thread.MagnetURIs {
			cleanedMagnets = append(cleanedMagnets, parser.StripTrackersFromMagnet(m))
		}

		linkedThread := &database.Thread{
			ThreadHash: thread.ThreadHash,
			RawTitle:   thread.RawTitle,
			CleanTitle: cleanTitle,
			TmdbID:     &tmdbResult.TmdbID,
			Status:     "linked",
			Type:       thread.Type,
			PostedAt:   thread.PostedAt,
			Catalog:    thread.CatalogID,
			MagnetURIs: cleanedMagnets,
			CreatedAt:  time.Now(),
			UpdatedAt:  time.Now(),
		}
		if tmdbResult.Year > 0 {
			linkedThread.Year = &tmdbResult.Year
		}

		err = database.CreateOrUpdateThread(tx, linkedThread)
		if err != nil {
			return err
		}

		isSeries := strings.ToLower(thread.Type) == "series"
		var streams []database.Stream

		for _, magnet := range cleanedMagnets {
			parsedMagnet := parser.ParseMagnet(magnet, thread.Type)
			if parsedMagnet == nil {
				continue
			}

			cacheRecord := database.MagnetCache{
				Infohash:  parsedMagnet.Infohash,
				Magnet:    magnet,
				CreatedAt: time.Now(),
			}
			cacheBytes, _ := database.EncodeGob(cacheRecord)
			_ = magnetBucket.Put([]byte(parsedMagnet.Infohash), cacheBytes)

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
			err = database.CreateStreams(tx, streams)
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
