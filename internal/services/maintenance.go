
package maintenance

import (
	"time"

	"github.com/kiskey/stremio-mvshows-go/internal/config"
	"github.com/kiskey/stremio-mvshows-go/internal/database"
	"github.com/kiskey/stremio-mvshows-go/internal/utils"
)

// PerformMaintenance runs incremental page vacuuming, optimizations, statistics calibrations,
// and truncates transaction logs cleanly without acquiring blocking exclusive system locks.
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

	// 2. Incremental Vacuum: Safely release deleted pages back to OS without blocking locks
	err := database.DB.Exec("PRAGMA incremental_vacuum(200);").Error
	if err != nil {
		utils.Logger.Warn().Err(err).Msg("PRAGMA incremental_vacuum failed during maintenance.")
	} else {
		utils.Logger.Debug().Msg("Incremental vacuum completed successfully.")
	}

	// 3. Recalculate index stats on active query structures with threshold boundaries
	_ = database.DB.Exec("PRAGMA analysis_limit=400;")
	err = database.DB.Exec("PRAGMA optimize;").Error
	if err != nil {
		utils.Logger.Warn().Err(err).Msg("PRAGMA optimize failed during maintenance.")
	} else {
		utils.Logger.Debug().Msg("PRAGMA optimize completed successfully.")
	}

	// 4. Truncate WAL cleanly AFTER all write/vacuuming operations have finished writing
	err = database.DB.Exec("PRAGMA wal_checkpoint(TRUNCATE);").Error
	if err != nil {
		utils.Logger.Warn().Err(err).Msg("PRAGMA wal_checkpoint failed during maintenance.")
	} else {
		utils.Logger.Debug().Msg("WAL checkpoint and transaction truncation completed.")
	}

	utils.Logger.Info().Msg("Database maintenance routines completed successfully.")
}
