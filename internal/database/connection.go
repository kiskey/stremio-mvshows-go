// Version: 2.0.3
// Change log: Added thread_id_index bucket to manage fast ID lookup pointers and auto-repaired legacy ID=0 records on database startup.

package database

import (
	"fmt"
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
			"thread_id_index",   // High-speed thread ID lookup index bucket
		}
		for _, bName := range buckets {
			_, errBucket := tx.CreateBucketIfNotExists([]byte(bName))
			if errBucket != nil {
				return errBucket
			}
		}

		// Self-healing migration: Repair legacy threads with ID == 0 and populate thread_id_index
		tb := tx.Bucket([]byte("threads"))
		idB := tx.Bucket([]byte("thread_id_index"))
		if tb != nil && idB != nil {
			type repairItem struct {
				key    []byte
				thread Thread
			}
			var toUpdate []repairItem

			c := tb.Cursor()
			for k, v := c.First(); k != nil; k, v = c.Next() {
				var t Thread
				if errDec := DecodeGob(v, &t); errDec == nil {
					if t.ID == 0 {
						seq, errSeq := tb.NextSequence()
						if errSeq == nil {
							t.ID = uint(seq)
							toUpdate = append(toUpdate, repairItem{key: k, thread: t})
						}
					} else {
						// Ensure index pointer is populated
						_ = idB.Put([]byte(fmt.Sprintf("%d", t.ID)), k)
					}
				}
			}

			for _, item := range toUpdate {
				bytesData, errEnc := EncodeGob(item.thread)
				if errEnc == nil {
					_ = tb.Put(item.key, bytesData)
					_ = idB.Put([]byte(fmt.Sprintf("%d", item.thread.ID)), item.key)
				}
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
