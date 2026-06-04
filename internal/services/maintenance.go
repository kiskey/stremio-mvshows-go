package maintenance

import (
	"github.com/sevvian/smvshows-go/internal/database"
	"github.com/sevvian/smvshows-go/internal/utils"
)

// PerformMaintenance runs WAL checkpoint truncation, database VACUUM, and ANALYZE queries.
func PerformMaintenance() {
	utils.Logger.Info().Msg("Starting database maintenance routines...")

	if database.DB == nil {
		utils.Logger.Error().Msg("Maintenance skipped: Database connection is nil.")
		return
	}

	// Truncate the Write-Ahead Log (WAL) to shrink wal file sizes safely
	err := database.DB.Exec("PRAGMA wal_checkpoint(TRUNCATE);").Error
	if err != nil {
		utils.Logger.Warn().Err(err).Msg("PRAGMA wal_checkpoint failed during maintenance.")
	} else {
		utils.Logger.Debug().Msg("WAL checkpoint completed.")
	}

	// Rebuild the database file, defragmenting and shrinking file size
	err = database.DB.Exec("VACUUM;").Error
	if err != nil {
		utils.Logger.Error().Err(err).Msg("VACUUM execution failed during maintenance.")
	} else {
		utils.Logger.Debug().Msg("VACUUM completed.")
	}

	// Recalculate index statistics to help the SQLite optimizer make smarter queries
	err = database.DB.Exec("ANALYZE;").Error
	if err != nil {
		utils.Logger.Warn().Err(err).Msg("ANALYZE execution failed during maintenance.")
	} else {
		utils.Logger.Debug().Msg("ANALYZE completed.")
	}

	utils.Logger.Info().Msg("Database maintenance routines completed successfully.")
}
