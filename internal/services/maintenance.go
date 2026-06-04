package maintenance

import (
	"time"

	"github.com/kiskey/stremio-mvshows-go/internal/config"
	"github.com/kiskey/stremio-mvshows-go/internal/database"
	"github.com/kiskey/stremio-mvshows-go/internal/utils"
)

// PerformMaintenance runs WAL checkpoint truncation, database VACUUM, ANALYZE queries,
// and clears unaccessed debrid cache entries older than the configured expiry threshold.
func PerformMaintenance() {
	utils.Logger.Info().Msg("Starting database maintenance routines...")

	if database.DB == nil {
		utils.Logger.Error().Msg("Maintenance skipped: Database connection is nil.")
		return
	}

	cfg := config.Load()

	// 1. Idempotent cache cleanup for unaccessed entries
	if cfg.CacheExpiryEnabled && cfg.CacheExpiryDays > 0 {
		utils.Logger.Info().Int("expiry_days", cfg.CacheExpiryDays).Msg("Checking for expired debrid torrent cache entries...")
		cutoff := time.Now().AddDate(0, 0, -cfg.CacheExpiryDays)

		tx := database.DB.Where("last_checked < ?", cutoff).Delete(&database.DebridTorrent{})
		if tx.Error != nil {
			utils.Logger.Error().Err(tx.Error).Msg("Failed to clear expired debrid torrent cache during maintenance.")
		} else if tx.RowsAffected > 0 {
			utils.Logger.Info().
				Int64("cleared_count", tx.RowsAffected).
				Time("cutoff_time", cutoff).
				Msg("Expired debrid torrent cache cleared successfully.")
		} else {
			utils.Logger.Debug().Msg("No expired debrid torrent cache entries found.")
		}
	}

	// 2. Truncate the Write-Ahead Log (WAL) to shrink wal file sizes safely
	err := database.DB.Exec("PRAGMA wal_checkpoint(TRUNCATE);").Error
	if err != nil {
		utils.Logger.Warn().Err(err).Msg("PRAGMA wal_checkpoint failed during maintenance.")
	} else {
		utils.Logger.Debug().Msg("WAL checkpoint completed.")
	}

	// 3. Rebuild the database file, defragmenting and shrinking file size
	err = database.DB.Exec("VACUUM;").Error
	if err != nil {
		utils.Logger.Error().Err(err).Msg("VACUUM execution failed during maintenance.")
	} else {
		utils.Logger.Debug().Msg("VACUUM completed.")
	}

	// 4. Recalculate index statistics to help the SQLite optimizer make smarter queries
	err = database.DB.Exec("ANALYZE;").Error
	if err != nil {
		utils.Logger.Warn().Err(err).Msg("ANALYZE execution failed during maintenance.")
	} else {
		utils.Logger.Debug().Msg("ANALYZE completed.")
	}

	utils.Logger.Info().Msg("Database maintenance routines completed successfully.")
}
