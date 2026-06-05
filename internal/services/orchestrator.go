// Version: 1.0.1
// Change log: Integrated a NULL-safe existence check for series and season pack streams to prevent duplicate rows in SQLite's unique constraint implementation.

package orchestrator

import (
	"encoding/json"
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
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
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
		_ = database.DB.Model(&database.Thread{}).Where("status = ?", "linked").Count(&linked)
		_ = database.DB.Model(&database.Thread{}).Where("status = ?", "pending_tmdb").Count(&pending)
		_ = database.DB.Model(&database.FailedThread{}).Count(&failed)
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

	scraped, err := crawler.RunCrawler(cfg)
	if err != nil {
		utils.Logger.Error().Err(err).Msg("Crawler execution failed catastrophically.")
		return
	}

	utils.Logger.Info().Int("count", len(scraped)).Msg("Forum crawl complete. Starting sequential thread metadata match processing.")
	tmdbClient := metadata.NewTMDBClient(cfg)

	for _, thread := range scraped {
		processThread(thread, tmdbClient)
	}

	utils.Logger.Info().Int("total_scraped", len(scraped)).Msg("Workflow thread processing complete.")
}

func processThread(thread crawler.CrawledThread, tmdbClient *metadata.TMDBClient) {
	// Defensive panic recovery for individual thread processing
	defer func() {
		if r := recover(); r != nil {
			utils.Logger.Error().
				Interface("panic", r).
				Str("title", thread.RawTitle).
				Msg("Recovered from panic during processThread processing.")
		}
	}()

	// 1. Check existing thread by raw_title
	var existing database.Thread
	err := database.DB.Where("raw_title = ?", thread.RawTitle).First(&existing).Error
	if err == nil {
		// If thread hash matches exactly, nothing changed. Skip entirely.
		if existing.ThreadHash == thread.ThreadHash {
			utils.Logger.Debug().Str("title", thread.RawTitle).Msg("Thread content unchanged. Skipping.")
			return
		}

		// Content has changed! Delete existing thread & cascade dependencies to recreate cleanly
		utils.Logger.Info().Str("title", thread.RawTitle).Msg("Thread magnets changed. Purging old record to reprocess.")
		errPurge := database.DB.Transaction(func(tx *gorm.DB) error {
			if existing.TmdbID != nil {
				_ = tx.Where("tmdb_id = ?", *existing.TmdbID).Delete(&database.Stream{})
			}
			return tx.Delete(&existing).Error
		})
		if errPurge != nil {
			utils.Logger.Error().Err(errPurge).Str("title", thread.RawTitle).Msg("Failed to transactional purge modified thread. Skipping.")
			return
		}
	}

	// 2. Parse title using our robust parser
	parsed := parser.ParseTitle(thread.RawTitle)
	if parsed == nil || parsed.Title == "" {
		_ = database.LogFailedThread(thread.ThreadHash, thread.RawTitle, "Title parsing failed critically", nil)
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

		_ = database.DB.Transaction(func(tx *gorm.DB) error {
			_ = database.DeleteFailedThread(thread.ThreadHash, tx)
			return database.CreateOrUpdateThread(pending, tx)
		})
		return
	}

	// 4. Resolve and save linked metadata and streams inside a safe transaction block
	errTx := database.DB.Transaction(func(tx *gorm.DB) error {
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

		// Create Thread record
		linkedThread := &database.Thread{
			ThreadHash: thread.ThreadHash,
			RawTitle:   thread.RawTitle,
			CleanTitle: tmdbResult.Title,
			TmdbID:     &tmdbResult.TmdbID,
			Status:     "linked",
			Type:       thread.Type,
			PostedAt:   thread.PostedAt,
			Catalog:    thread.CatalogID,
			MagnetURIs: thread.MagnetURIs,
		}
		if tmdbResult.Year > 0 {
			linkedThread.Year = &tmdbResult.Year
		}

		errThr := database.CreateOrUpdateThread(linkedThread, tx)
		if errThr != nil {
			return errThr
		}

		// Extract, construct, and upsert each magnet URI into a stream mapping
		for _, magnet := range thread.MagnetURIs {
			parsedMagnet := parser.ParseMagnet(magnet, thread.Type)
			if parsedMagnet == nil {
				continue
			}

			// Store magnet mapping in Cache so on-demand RD lookup can find it later by infohash
			cacheRecord := database.MagnetCache{
				Infohash: parsedMagnet.Infohash,
				Magnet:   magnet,
			}
			errCache := tx.Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "infohash"}},
				UpdateAll: true,
			}).Create(&cacheRecord).Error
			if errCache != nil {
				return errCache
			}

			// Generate relational streams
			stream := database.Stream{
				TmdbID:   tmdbResult.TmdbID,
				Infohash: parsedMagnet.Infohash,
				Quality:  parsedMagnet.Quality,
				Language: parsedMagnet.Language,
			}

			if strings.ToLower(thread.Type) == "series" {
				// Parse structural season and episode parameters
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
					// Full Season Pack (Episode fields remain NULL to capture any target match)
					stream.Episode = nil
					stream.EpisodeEnd = nil
				}
			} else {
				// Movie Streams do not require season/episode parameters
				stream.Season = nil
				stream.Episode = nil
				stream.EpisodeEnd = nil
			}

			// NULL-safe Unique Stream Existence Check:
			// Prevents SQLite unique index duplicate leaks on Nullable fields (season/episode)
			var existingStream database.Stream
			chkQuery := tx.Where("tmdb_id = ? AND infohash = ?", stream.TmdbID, stream.Infohash)
			if stream.Season != nil {
				chkQuery = chkQuery.Where("season = ?", *stream.Season)
			} else {
				chkQuery = chkQuery.Where("season IS NULL")
			}
			if stream.Episode != nil {
				chkQuery = chkQuery.Where("episode = ?", *stream.Episode)
			} else {
				chkQuery = chkQuery.Where("episode IS NULL")
			}

			if chkQuery.First(&existingStream).Error == nil {
				// Key already exists, perform an explicit in-place update
				stream.ID = existingStream.ID
				stream.CreatedAt = existingStream.CreatedAt
				stream.UpdatedAt = time.Now()
				_ = tx.Save(&stream)
			} else {
				// Completely unique key, insert record cleanly
				_ = tx.Create(&stream)
			}
		}

		// Clean up any old error parsing logs
		_ = database.DeleteFailedThread(thread.ThreadHash, tx)
		return nil
	})

	if errTx != nil {
		utils.Logger.Error().Err(errTx).Str("title", thread.RawTitle).Msg("Transaction failed while saving linked metadata.")
		_ = database.LogFailedThread(thread.ThreadHash, thread.RawTitle, fmt.Sprintf("Tx Save Error: %s", errTx.Error()), nil)
	} else {
		utils.Logger.Info().Str("title", thread.RawTitle).Msg("Successfully linked thread and saved stream references.")
	}
}
