
// Version: 2.0.2
// Change log: Added high-speed tmdb_thread_index bucket to manage microsecond-scale pointer lookups, avoiding full sweeps.

package database

import (
	"os"
	"path/filepath"
	"time"

	bolt "go.etcd.io/bbolt"
)

var DB *bolt.DB

// Init initializes the BoltDB database, configures transaction timeouts, and ensures Buckets exist.
func Init(dbPath string) (*bolt.DB, error) {
	dir := filepath.Dir(dbPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	var err error
	DB, err = bolt.Open(dbPath, 0600, &bolt.Options{Timeout: 2 * time.Second})
	if err != nil {
		return nil, err
	}

	err = DB.Update(func(tx *bolt.Tx) error {
		buckets := []string{
			"threads",
			"tmdb_metadata",
			"streams",
			"failed_threads",
			"debrid_torrents",
			"debrid_cache_locks",
			"magnet_cache",
			"torbox_id_map",
			"catalog_index",
			"tmdb_thread_index", // High-speed point index bucket
		}
		for _, bName := range buckets {
			_, errBucket := tx.CreateBucketIfNotExists([]byte(bName))
			if errBucket != nil {
				return errBucket
			}
		}
		return nil
	})
	if err != nil {
		_ = DB.Close()
		return nil, err
	}

	return DB, nil
}
