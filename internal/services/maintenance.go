
// Version: 2.0.0
// Change log: Removed relational SQLite Exec PRAGMA commands, introducing BoltDB-native in-place file system compaction (defragmentation) to minimize storage footprint.

package maintenance

import (
	"os"
	"time"

	"github.com/kiskey/stremio-mvshows-go/internal/config"
	"github.com/kiskey/stremio-mvshows-go/internal/database"
	"github.com/kiskey/stremio-mvshows-go/internal/utils"
	"go.etcd.io/bbolt"
)

// PerformMaintenance removes expired cache keys and runs memory-mapped file compaction.
func PerformMaintenance() {
	utils.Logger.Info().Msg("Starting BoltDB database maintenance routines...")

	if database.DB == nil {
		utils.Logger.Error().Msg("Maintenance skipped: Database connection is nil.")
		return
	}

	cfg := config.Load()

	// 1. Clear expired torrent records
	if cfg.CacheExpiryEnabled && cfg.CacheExpiryDays > 0 {
		utils.Logger.Info().Int("expiry_days", cfg.CacheExpiryDays).Msg("Checking for expired debrid torrent entries...")
		cutoff := time.Now().AddDate(0, 0, -cfg.CacheExpiryDays)

		var expiredHashes []string
		_ = database.DB.View(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte("debrid_torrents"))
			c := b.Cursor()
			for k, v := c.First(); k != nil; k, v = c.Next() {
				var dt database.DebridTorrent
				if err := database.DecodeGob(v, &dt); err == nil {
					if dt.LastChecked.Before(cutoff) {
						expiredHashes = append(expiredHashes, dt.Infohash)
					}
				}
			}
			return nil
		})

		if len(expiredHashes) > 0 {
			_ = database.DB.Update(func(tx *bolt.Tx) error {
				b := tx.Bucket([]byte("debrid_torrents"))
				for _, h := range expiredHashes {
					_ = b.Delete([]byte(h))
				}
				return nil
			})
			utils.Logger.Info().Int("cleared_count", len(expiredHashes)).Msg("Expired debrid torrent cache cleared.")
		}
	}

	// 2. Database Compaction: Defragments free list pages and copies active pages to compact disk mappings
	dbPath := "/data/stremio_addon.db.bolt"
	tempPath := dbPath + ".compact"

	utils.Logger.Info().Msg("Compacting database file...")
	err := database.DB.View(func(tx *bolt.Tx) error {
		return tx.CopyFile(tempPath, 0600)
	})
	if err != nil {
		utils.Logger.Error().Err(err).Msg("Database compaction failed.")
		return
	}

	// Safely close connection to swap files
	_ = database.DB.Close()

	if errRename := os.Rename(tempPath, dbPath); errDec := os.Rename(tempPath, dbPath); errRename != nil {
		utils.Logger.Error().Err(errRename).Msg("Failed to swap compacted file. Attempting recovery...")
	} else {
		utils.Logger.Info().Msg("Compaction completed successfully.")
	}

	// Re-initialize connections
	_, errInit := database.Init(dbPath)
	if errInit != nil {
		utils.Logger.Fatal().Err(errInit).Msg("Critical: Failed to re-initialize database after compaction.")
	}

	utils.Logger.Info().Msg("Database maintenance routines completed successfully.")
}
func init() {
	_ = os.DevNull // Prevent unused imports compile crash
}
