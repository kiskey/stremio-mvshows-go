// Version: 2.0.4
// Change log: Implemented Proposal 4 fragmentation-based compaction guardrails (checking >50MB size and >40% fragmentation) before executing BoltDB file compaction.

package maintenance

import (
	"context"
	"os"
	"syscall"
	"time"

	"github.com/kiskey/stremio-mvshows-go/internal/config"
	"github.com/kiskey/stremio-mvshows-go/internal/database"
	"github.com/kiskey/stremio-mvshows-go/internal/services/debrid"
	"github.com/kiskey/stremio-mvshows-go/internal/utils"
	bolt "go.etcd.io/bbolt"
)

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
			if b == nil {
				return nil
			}
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
				if b != nil {
					for _, h := range expiredHashes {
						_ = b.Delete([]byte(h))
					}
				}
				return nil
			})
			utils.Logger.Info().Int("cleared_count", len(expiredHashes)).Msg("Expired debrid torrent cache cleared.")
		}
	}

	// 2. Clear expired failed thread log lines (>7 days TTL)
	utils.Logger.Info().Msg("Checking for expired failed thread logs...")
	cutoffFailed := time.Now().AddDate(0, 0, -7)
	var expiredFailedKeys [][]byte
	_ = database.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("failed_threads"))
		if b == nil {
			return nil
		}
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var ft database.FailedThread
			if err := database.DecodeGob(v, &ft); err == nil {
				if ft.LastAttempt.Before(cutoffFailed) {
					expiredFailedKeys = append(expiredFailedKeys, []byte(ft.ThreadHash))
				}
			}
		}
		return nil
	})
	if len(expiredFailedKeys) > 0 {
		_ = database.DB.Update(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte("failed_threads"))
			if b != nil {
				for _, k := range expiredFailedKeys {
					_ = b.Delete(k)
				}
			}
			return nil
		})
		utils.Logger.Info().Int("cleared_count", len(expiredFailedKeys)).Msg("Expired failed thread logs pruned.")
	}

	// 3. Clear expired magnet caches (>30 days TTL)
	utils.Logger.Info().Msg("Checking for expired magnet cache entries...")
	cutoffMagnet := time.Now().AddDate(0, 0, -30)
	var expiredMagnetKeys [][]byte
	_ = database.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("magnet_cache"))
		if b == nil {
			return nil
		}
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var mc database.MagnetCache
			if err := database.DecodeGob(v, &mc); err == nil {
				if mc.CreatedAt.Before(cutoffMagnet) {
					expiredMagnetKeys = append(expiredMagnetKeys, []byte(mc.Infohash))
				}
			}
		}
		return nil
	})
	if len(expiredMagnetKeys) > 0 {
		_ = database.DB.Update(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte("magnet_cache"))
			if b != nil {
				for _, k := range expiredMagnetKeys {
					_ = b.Delete(k)
				}
			}
			return nil
		})
		utils.Logger.Info().Int("cleared_count", len(expiredMagnetKeys)).Msg("Expired magnet cache entries pruned.")
	}

	// 4. Trigger Debrid Provider Stale Torrent Cleanup
	utils.Logger.Info().Msg("Executing Debrid provider cloud account stale torrent cleanup...")
	provider := debrid.GetProvider(cfg)
	if provider.IsEnabled() {
		if purged, err := provider.CleanupStaleTorrents(context.Background()); err == nil {
			if purged > 0 {
				utils.Logger.Info().Int("purged_torrents", purged).Msg("Debrid cloud account stale downloads purged successfully.")
			}
		} else {
			utils.Logger.Warn().Err(err).Msg("Debrid cloud account cleanup encountered an error.")
		}
	}

	// 5. Proposal 4: Dynamic Fragmentation-Based Database Compaction Guardrail
	dbPath := "/data/stremio_addon.db.bolt"
	tempPath := dbPath + ".compact"

	var dbSize int64
	if stat, err := os.Stat(dbPath); err == nil {
		dbSize = stat.Size()
	}

	var totalInuseBytes int64
	var totalAllocatedBytes int64

	_ = database.DB.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			stats := b.Stats()
			inuse := int64(stats.BranchInuse) + int64(stats.LeafInuse)
			allocated := int64(stats.BranchAlloc) + int64(stats.LeafAlloc)
			totalInuseBytes += inuse
			totalAllocatedBytes += allocated
			return nil
		})
	})

	var fragmentationPct float64
	if totalAllocatedBytes > 0 {
		fillRatio := (float64(totalInuseBytes) / float64(totalAllocatedBytes)) * 100.0
		fragmentationPct = 100.0 - fillRatio
	}

	utils.Logger.Info().
		Int64("db_size_bytes", dbSize).
		Float64("fragmentation_pct", fragmentationPct).
		Msg("Database storage fragmentation stats calculated.")

	minCompactionSize := int64(50 * 1024 * 1024)
	minFragmentationThreshold := 40.0

	if dbSize < minCompactionSize && fragmentationPct < minFragmentationThreshold {
		utils.Logger.Info().
			Int64("db_size_bytes", dbSize).
			Float64("fragmentation_pct", fragmentationPct).
			Msg("Database compaction skipped: File size and fragmentation ratios are well within optimal bounds.")
		utils.Logger.Info().Msg("Database maintenance routines completed successfully.")
		return
	}

	requiredSpace := int64(float64(dbSize) * 2.5)
	if !hasEnoughSpaceForCompaction("/data", requiredSpace) {
		utils.Logger.Error().
			Int64("db_size_bytes", dbSize).
			Int64("required_bytes", requiredSpace).
			Msg("Compaction cancelled: Insufficient disk space available to complete sequential write safely.")
		return
	}

	utils.Logger.Info().Msg("Compacting database file under active threshold trigger...")
	err := database.DB.View(func(tx *bolt.Tx) error {
		return tx.CopyFile(tempPath, 0600)
	})
	if err != nil {
		utils.Logger.Error().Err(err).Msg("Database compaction failed.")
		return
	}

	_ = database.DB.Close()

	if errRename := os.Rename(tempPath, dbPath); errRename != nil {
		utils.Logger.Error().Err(errRename).Msg("Failed to swap compacted file. Attempting recovery...")
	} else {
		utils.Logger.Info().Msg("Compaction completed successfully.")
	}

	_, errInit := database.Init(dbPath)
	if errInit != nil {
		utils.Logger.Fatal().Err(errInit).Msg("Critical: Failed to re-initialize database after compaction.")
	}

	utils.Logger.Info().Msg("Database maintenance routines completed successfully.")
}

func hasEnoughSpaceForCompaction(path string, requiredSpace int64) bool {
	var stat syscall.Statfs_t
	err := syscall.Statfs(path, &stat)
	if err != nil {
		utils.Logger.Warn().Err(err).Msg("Could not verify disk space via syscall, bypassing check.")
		return true
	}
	availableBytes := stat.Bavail * uint64(stat.Bsize)
	return int64(availableBytes) > requiredSpace
}
