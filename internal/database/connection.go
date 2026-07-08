
// Version: 2.0.1
// Change log: Fixed undefined bolt namespace compiler error by explicitly aliasing go.etcd.io/bbolt import as bolt.

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
	// Ensure parent directory paths exist
	dir := filepath.Dir(dbPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	var err error
	// Open database with a 2-second timeout context to prevent file locking hangs on LXC templates
	DB, err = bolt.Open(dbPath, 0600, &bolt.Options{Timeout: 2 * time.Second})
	if err != nil {
		return nil, err
	}

	// Structural Buckets Setup inside an atomic initialization block
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
