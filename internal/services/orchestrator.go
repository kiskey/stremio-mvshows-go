
// Version: 1.1.2
// Change log: Refactored database logic from GORM to high-performance BoltDB transactional bucket queries to resolve build errors and ensure functional parity.

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

// IsCrawling safely reads the active crawling execution flag.
func IsCrawling() bool {
	crawlMu.Lock()
	defer crawlMu.Unlock()
	return isCrawling
}

// GetDashboardCache safely reads currently cached dashboard statistics.
func GetDashboardCache() DashboardStats {
	cacheMu.RLock()
	defer cacheMu.RUnlock()
	return dashboardCache
}

// UpdateDashboardCache recalculates aggregate statistics from the database.
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

// RunFullWorkflow triggers the full sequence: scrape, parse, TMDB lookup, and relational linking.
func RunFullWorkflow(cfg *config.Config) {
	// Defensive panic recovery to prevent unhandled background panics from crashing the entire process
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

	// Option 1 & Option 3: Check database-seeding state to determine the dynamic crawling mode
	incremental := false
	if database.DB != nil {
		var count int
		_ = database.DB.View(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte("threads"))
			if b != nil {
				c := b.Cursor()
				for k, v := c.First(); k != nil; k, v = c.Next() {
					var t database.Thread
					if err := database.DecodeGob(v, &t); err == nil {
						if t.Status == "linked" {
							count++
							if count > 50 {
								break
							}
						}
					}
				}
			}
			return nil
		})

		// If database already contains more than 50 seeded records, safely transition to fast incremental sync
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

func processThread(thread crawler.CrawledThread, tmdbClient *metadata.TMDBClient, incremental bool) {
	// Defensive panic recovery for individual thread processing
	defer func() {
		if r := recover(); r != nil {
			utils.Logger.Error().
				Interface("panic", r).
				Str("title", thread.RawTitle).
				Msg("Recovered from panic during processThread processing.")
		}
	}()

	// 1. Check existing thread by raw_title using Bolt point lookup
	existing, err := database.FindThreadByRawTitle(nil, thread.RawTitle)
	if err == nil && existing != nil {
		// If thread hash matches exactly, nothing changed.
		if existing.ThreadHash == thread.ThreadHash {
			// Self-Healing Rule: If the thread is in "pending_tmdb" status, only skip it during 
			// lightweight incremental runs.
			if existing.Status == "pending_tmdb" && !incremental {
				// Retry matching in background during full scraper runs
			} else {
				return
			}
		}

		// If thread hash changed, delete the old stream relationships to prevent duplicates
		if existing.TmdbID != nil {
			_ = database.DB.Update(func(tx *bolt.Tx) error {
				return tx.Bucket([]byte("streams")).Delete([]byte(*existing.TmdbID))
			})
		}
	}

	// 2. Parse the RawTitle to clean it up with context-aware logic
	parsed := parser.ParseTitle(thread.RawTitle, thread.Type)
	if parsed == nil || parsed.Title == "" {
		_ = database.LogFailedThread(nil, thread.ThreadHash, thread.RawTitle, "Title parsing failed critically")
		return
	}

	// 3. TMDB lookup
	tmdbResult, errTmdb := tmdbClient.Search(parsed.Title, parsed.Year, thread.Type)
	if errTmdb != nil {
		utils.Logger.Warn().Err(errTmdb).Str("title", parsed.Title).Msg("TMDB lookup failed or score below threshold. Storing as pending_tmdb.")

		// Save as pending_tmdb for admin rescue panel
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

		// Check if this IMDb ID is already registered under an alternative TMDB ID
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

		// ZERO-STALE METADATA OPTIMIZATION: Discard massive external API JSON strings 
		// and save a lightweight empty structure "{}" as placeholder.
		// Cinemeta dynamically renders descriptions, artwork, and reviews on request.
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

		// Write-Time Sanitation Failsafe:
		// If Cinemeta details API returned an empty title (skeleton card), sanitize RawTitle on-the-fly.
		cleanTitle := tmdbResult.Title
		if cleanTitle == "" {
			parsed := parser.ParseTitle(thread.RawTitle, thread.Type)
			if parsed != nil && parsed.Title != "" {
				cleanTitle = parsed.Title
			} else {
				cleanTitle = thread.RawTitle
			}
		}

		// Create Thread record
		linkedThread := &database.Thread{
			ThreadHash: thread.ThreadHash,
			RawTitle:   thread.RawTitle,
			CleanTitle: cleanTitle,
			TmdbID:     &tmdbResult.TmdbID,
			Status:     "linked",
			Type:       thread.Type,
			PostedAt:   thread.PostedAt,
			Catalog:    thread.CatalogID,
			MagnetURIs: thread.MagnetURIs,
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

		// Cache hot properties outside loop to avoid redundant heap string copies on every magnet
		isSeries := strings.ToLower(thread.Type) == "series"
		var streams []database.Stream

		// Extract, construct, and upsert each magnet URI into a stream mapping
		for _, magnet := range thread.MagnetURIs {
			parsedMagnet := parser.ParseMagnet(magnet, thread.Type)
			if parsedMagnet == nil {
				continue
			}

			// Store magnet mapping in Cache so on-demand RD lookup can find it later by infohash
			cacheRecord := database.MagnetCache{
				Infohash:  parsedMagnet.Infohash,
				Magnet:    magnet,
				CreatedAt: time.Now(),
			}
			cacheBytes, _ := database.EncodeGob(cacheRecord)
			_ = magnetBucket.Put([]byte(parsedMagnet.Infohash), cacheBytes)

			// Generate stream
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

		// Clean up any old error parsing logs
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
