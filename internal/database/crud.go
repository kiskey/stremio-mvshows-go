
// Version: 2.0.6
// Change log: Refactored FindSeriesStreams to implement layered boundary checks on Nullable episode pointers, cleanly resolving empty streams listings for single-episode releases.

package database

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"sort"
	"strings"
	"time"

	bolt "go.etcd.io/bbolt"
)

// ── Generic Serialization Helpers ──

func EncodeGob(val interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(val)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// DecodeGob implements a resilient, panic-recovering GOB decoder to insulate the application
// from future struct changes (e.g. data type adjustments or field renamings) without server crashes.
func DecodeGob(data []byte, val interface{}) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("gob deserialization panic rescued: %v", r)
		}
	}()
	return gob.NewDecoder(bytes.NewReader(data)).Decode(val)
}

func runView(tx *bolt.Tx, fn func(tx *bolt.Tx) error) error {
	if tx != nil {
		return fn(tx)
	}
	return DB.View(fn)
}

func runUpdate(tx *bolt.Tx, fn func(tx *bolt.Tx) error) error {
	if tx != nil {
		return fn(tx)
	}
	return DB.Update(fn)
}

// ── Thread CRUD Operations ──

func FindThreadByHash(tx *bolt.Tx, hash string) (*Thread, error) {
	var t Thread
	err := runView(tx, func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("threads"))
		data := b.Get([]byte(hash))
		if data == nil {
			return bolt.ErrBucketNotFound
		}
		return DecodeGob(data, &t)
	})
	if err != nil {
		return nil, nil // Parity mapping for RecordNotFound checks
	}
	return &t, nil
}

func FindThreadByRawTitle(tx *bolt.Tx, rawTitle string) (*Thread, error) {
	var found *Thread
	err := runView(tx, func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("threads"))
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var t Thread
			if err := DecodeGob(v, &t); err == nil {
				if t.RawTitle == rawTitle {
					found = &t
					break
				}
			}
		}
		return nil
	})
	return found, err
}

func FindThreadByID(id uint) (*Thread, error) {
	var found *Thread
	err := DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("threads"))
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var t Thread
			if err := DecodeGob(v, &t); err == nil {
				if t.ID == id {
					found = &t
					break
				}
			}
		}
		return nil
	})
	return found, err
}

func CreateOrUpdateThread(tx *bolt.Tx, data *Thread) error {
	return runUpdate(tx, func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("threads"))
		idxB := tx.Bucket([]byte("catalog_index"))

		// 1. SELF-HEALING INDEX REGISTRY (Resolves Risk 1):
		// Locate and prune old index files if the thread's PostedAt timestamp, Catalog, or Type is updated
		existingData := b.Get([]byte(data.ThreadHash))
		if existingData != nil {
			var oldThread Thread
			if errDec := DecodeGob(existingData, &oldThread); errDec == nil {
				if oldThread.Catalog != "" {
					oldPosted := time.Now()
					if oldThread.PostedAt != nil {
						oldPosted = *oldThread.PostedAt
					}
					oldInverse := 9999999999 - oldPosted.Unix()
					oldIndexKey := fmt.Sprintf("cat:%s:%s:%010d:%s", oldThread.Catalog, oldThread.Type, oldInverse, oldThread.ThreadHash)
					_ = idxB.Delete([]byte(oldIndexKey))
				}
			}
		}

		// 2. Commit the updated Thread structure to the threads bucket
		bytesData, err := EncodeGob(data)
		if err != nil {
			return err
		}
		err = b.Put([]byte(data.ThreadHash), bytesData)
		if err != nil {
			return err
		}

		// 3. Write new, sorted Catalog Index Key (Resolves Risk 3)
		if data.Status == "linked" && data.Catalog != "" {
			postedTime := time.Now()
			if data.PostedAt != nil {
				postedTime = *data.PostedAt
			}
			inverseTime := 9999999999 - postedTime.Unix()
			indexKey := fmt.Sprintf("cat:%s:%s:%010d:%s", data.Catalog, data.Type, inverseTime, data.ThreadHash)
			_ = idxB.Put([]byte(indexKey), []byte(data.ThreadHash))
		}

		// 4. Update the high-speed TMDB mapping index
		if data.Status == "linked" && data.TmdbID != nil {
			threadIdxB := tx.Bucket([]byte("tmdb_thread_index"))
			_ = threadIdxB.Put([]byte(*data.TmdbID), []byte(data.ThreadHash))
		}

		return nil
	})
}

func DeleteThread(tx *bolt.Tx, t *Thread) error {
	return runUpdate(tx, func(tx *bolt.Tx) error {
		_ = tx.Bucket([]byte("threads")).Delete([]byte(t.ThreadHash))

		// Clean up catalog indexes
		idxB := tx.Bucket([]byte("catalog_index"))
		c := idxB.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			if strings.HasSuffix(string(k), ":"+t.ThreadHash) {
				_ = idxB.Delete(k)
			}
		}

		// Clean up TMDB relational thread pointer index
		if t.TmdbID != nil {
			_ = tx.Bucket([]byte("tmdb_thread_index")).Delete([]byte(*t.TmdbID))
			_ = tx.Bucket([]byte("streams")).Delete([]byte(*t.TmdbID))
		}
		return nil
	})
}

func GetPendingThreads() ([]Thread, error) {
	var list []Thread
	err := runView(nil, func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("threads"))
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var t Thread
			if err := DecodeGob(v, &t); err == nil {
				if t.Status == "pending_tmdb" {
					list = append(list, t)
				}
			}
		}
		return nil
	})
	sort.Slice(list, func(i, j int) bool {
		tI := time.Time{}
		if list[i].PostedAt != nil { tI = *list[i].PostedAt }
		tJ := time.Time{}
		if list[j].PostedAt != nil { tJ = *list[j].PostedAt }
		return tI.After(tJ)
	})
	return list, err
}

func GetRecentLinkedThreads() ([]Thread, error) {
	var list []Thread
	err := DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("threads"))
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var t Thread
			if err := DecodeGob(v, &t); err == nil {
				if t.Status == "linked" {
					list = append(list, t)
				}
			}
		}
		return nil
	})
	sort.Slice(list, func(i, j int) bool {
		return list[i].UpdatedAt.After(list[j].UpdatedAt)
	})
	if len(list) > 15 {
		list = list[:15]
	}
	return list, err
}

// GetRecentLinkedThreadsPaginated supports high-speed page-slicing inside Bbolt View transactions.
func GetRecentLinkedThreadsPaginated(offset, limit int) ([]Thread, error) {
	var list []Thread
	err := DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("threads"))
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var t Thread
			if err := DecodeGob(v, &t); err == nil {
				if t.Status == "linked" {
					list = append(list, t)
				}
			}
		}
		return nil
	})
	sort.Slice(list, func(i, j int) bool {
		return list[i].UpdatedAt.After(list[j].UpdatedAt)
	})

	if offset >= len(list) {
		return []Thread{}, nil
	}
	end := offset + limit
	if end > len(list) {
		end = len(list)
	}
	return list[offset:end], err
}

// ── Stream CRUD Operations ──

var streamsQualityRank = map[string]int{
	"4K":    1, "2160P": 1, "2160p": 1,
	"1080P": 2, "1080p": 2,
	"720P":  3, "720p":  3,
	"480P":  4, "480p":  4,
	"SD":    5, "sd":    5,
}

func sortStreamsByQuality(streams []Stream) {
	sort.Slice(streams, func(i, j int) bool {
		qI := streamsQualityRank[strings.ToUpper(streams[i].Quality)]
		qJ := streamsQualityRank[strings.ToUpper(streams[j].Quality)]
		if qI == 0 { qI = 99 }
		if qJ == 0 { qJ = 99 }
		return qI < qJ
	})
}

func FindSeriesStreams(tx *bolt.Tx, tmdbID string, season, episode int) ([]Stream, error) {
	var allStreams []Stream
	err := runView(tx, func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("streams"))
		data := b.Get([]byte(tmdbID))
		if data == nil {
			return nil
		}
		return DecodeGob(data, &allStreams)
	})
	if err != nil {
		return nil, err
	}

	var filtered []Stream
	for _, s := range allStreams {
		match := false
		if s.Season != nil && *s.Season == season {
			if s.Episode != nil {
				if s.EpisodeEnd != nil {
					// 1. Matches multi-episode ranges (e.g., Episode 1 to 6)
					if episode >= *s.Episode && episode <= *s.EpisodeEnd {
						match = true
					}
				} else {
					// 2. Matches standard single episode releases (e.g., Episode 2)
					if *s.Episode == episode {
						match = true
					}
				}
			} else {
				// 3. Matches Season Packs
				match = true
			}
		} else if s.Season == nil && s.Episode == nil {
			// 4. Matches global Series Packs / Fallback
			match = true
		}

		if match {
			filtered = append(filtered, s)
		}
	}

	sortStreamsByQuality(filtered)
	return filtered, nil
}

func FindMovieStreams(tx *bolt.Tx, tmdbID string) ([]Stream, error) {
	var allStreams []Stream
	err := runView(tx, func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("streams"))
		data := b.Get([]byte(tmdbID))
		if data == nil {
			return nil
		}
		return DecodeGob(data, &allStreams)
	})
	if err != nil {
		return nil, err
	}

	sortStreamsByQuality(allStreams)
	return allStreams, nil
}

func streamsMatchUnique(a, b Stream) bool {
	if a.TmdbID != b.TmdbID || a.Infohash != b.Infohash {
		return false
	}
	if (a.Season == nil) != (b.Season == nil) {
		return false
	}
	if a.Season != nil && *a.Season != *b.Season {
		return false
	}
	if (a.Episode == nil) != (b.Episode == nil) {
		return false
	}
	if a.Episode != nil && *a.Episode != *b.Episode {
		return false
	}
	return true
}

func CreateStreams(tx *bolt.Tx, streams []Stream) error {
	if len(streams) == 0 {
		return nil
	}
	return runUpdate(tx, func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("streams"))
		
		byTMDB := make(map[string][]Stream)
		for _, s := range streams {
			byTMDB[s.TmdbID] = append(byTMDB[s.TmdbID], s)
		}

		for tmdbID, list := range byTMDB {
			var existing []Stream
			data := b.Get([]byte(tmdbID))
			if data != nil {
				_ = DecodeGob(data, &existing)
			}

			for _, s := range list {
				duplicate := false
				for i, ext := range existing {
					if streamsMatchUnique(ext, s) {
						existing[i] = s
						duplicate = true
						break
					}
				}
				if !duplicate {
					existing = append(existing, s)
				}
			}

			encBytes, err := EncodeGob(existing)
			if err != nil {
				return err
			}
			_ = b.Put([]byte(tmdbID), encBytes)
		}
		return nil
	})
}

// ── FailedThread operations ──

func LogFailedThread(tx *bolt.Tx, hash, rawTitle, reason string) error {
	return runUpdate(tx, func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("failed_threads"))
		ft := FailedThread{
			ThreadHash:  hash,
			RawTitle:    rawTitle,
			Reason:      reason,
			LastAttempt: time.Now(),
		}
		ftBytes, err := EncodeGob(ft)
		if err != nil {
			return err
		}
		return b.Put([]byte(hash), ftBytes)
	})
}

// GetFailedThreads retrieves all recorded parsing/workflow failures (Unpaginated standard list).
func GetFailedThreads() ([]FailedThread, error) {
	var list []FailedThread
	err := runView(nil, func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("failed_threads"))
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var ft FailedThread
			if err := DecodeGob(v, &ft); err == nil {
				list = append(list, ft)
			}
		}
		return nil
	})
	sort.Slice(list, func(i, j int) bool {
		return list[i].LastAttempt.After(list[j].LastAttempt)
	})
	return list, err
}

// GetFailedThreadsPaginated compiles failing entries sequentially inside slice offsets
func GetFailedThreadsPaginated(offset, limit int) ([]FailedThread, error) {
	var list []FailedThread
	err := runView(nil, func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("failed_threads"))
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var ft FailedThread
			if err := DecodeGob(v, &ft); err == nil {
				list = append(list, ft)
			}
		}
		return nil
	})
	sort.Slice(list, func(i, j int) bool {
		return list[i].LastAttempt.After(list[j].LastAttempt)
	})
	if offset >= len(list) {
		return []FailedThread{}, nil
	}
	end := offset + limit
	if end > len(list) {
		end = len(list)
	}
	return list[offset:end], err
}

func DeleteFailedThread(tx *bolt.Tx, hash string) error {
	return runUpdate(tx, func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("failed_threads"))
		return b.Delete([]byte(hash))
	})
}

// ── Lock managers ──

func IsDebridCacheLocked(hash string) bool {
	locked := false
	_ = DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("debrid_cache_locks"))
		// Force lowercase comparison to prevent key lookup mismatch
		if b.Get([]byte(strings.ToLower(hash))) != nil {
			locked = true
		}
		return nil
	})
	return locked
}

func CreateDebridCacheLock(hash string) error {
	return DB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("debrid_cache_locks"))
		lock := DebridCacheLock{Infohash: strings.ToLower(hash), CreatedAt: time.Now()}
		lockBytes, err := EncodeGob(lock)
		if err != nil {
			return err
		}
		return b.Put([]byte(strings.ToLower(hash)), lockBytes)
	})
}

func DeleteDebridCacheLock(hash string) error {
	return DB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("debrid_cache_locks"))
		return b.Delete([]byte(strings.ToLower(hash)))
	})
}
