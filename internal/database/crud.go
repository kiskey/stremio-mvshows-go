// Version: 2.3.1
// Change log: Corrected slice return type in GetFailedThreadsPaginated from []Thread{} to []FailedThread{} to resolve Go compiler type mismatch build error.

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

// DecodeGob implements a resilient, panic-recovering GOB decoder.
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
		return nil, nil
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
		idB := tx.Bucket([]byte("thread_id_index"))
		if idB != nil {
			hashBytes := idB.Get([]byte(fmt.Sprintf("%d", id)))
			if hashBytes != nil {
				tb := tx.Bucket([]byte("threads"))
				if tb != nil {
					data := tb.Get(hashBytes)
					if data != nil {
						var t Thread
						if err := DecodeGob(data, &t); err == nil {
							found = &t
							return nil
						}
					}
				}
			}
		}

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

func GetThreadByTmdbID(tx *bolt.Tx, tmdbID string) (*Thread, error) {
	var found *Thread
	err := runView(tx, func(tx *bolt.Tx) error {
		threadIdxB := tx.Bucket([]byte("tmdb_thread_index"))
		if threadIdxB != nil {
			hashBytes := threadIdxB.Get([]byte(tmdbID))
			if hashBytes != nil {
				tb := tx.Bucket([]byte("threads"))
				if tb != nil {
					data := tb.Get(hashBytes)
					if data != nil {
						var t Thread
						if err := DecodeGob(data, &t); err == nil {
							found = &t
							return nil
						}
					}
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
		idB, errIdBucket := tx.CreateBucketIfNotExists([]byte("thread_id_index"))
		if errIdBucket != nil {
			return errIdBucket
		}

		if data.ID == 0 {
			seq, errSeq := b.NextSequence()
			if errSeq != nil {
				return errSeq
			}
			data.ID = uint(seq)
		}

		existingData := b.Get([]byte(data.ThreadHash))
		if existingData != nil {
			var oldThread Thread
			if errDec := DecodeGob(existingData, &oldThread); errDec == nil {
				if data.URL == "" && oldThread.URL != "" {
					data.URL = oldThread.URL
				}

				if oldThread.Catalog != "" {
					oldPosted := time.Unix(0, 0)
					if oldThread.PostedAt != nil {
						oldPosted = *oldThread.PostedAt
					}
					oldInverse := 9999999999 - oldPosted.Unix()
					oldIndexKey := fmt.Sprintf("cat:%s:%s:%010d:%s", oldThread.Catalog, oldThread.Type, oldInverse, oldThread.ThreadHash)
					_ = idxB.Delete([]byte(oldIndexKey))
				}
				if oldThread.TmdbID != nil && (data.TmdbID == nil || *oldThread.TmdbID != *data.TmdbID) {
					_ = tx.Bucket([]byte("tmdb_thread_index")).Delete([]byte(*oldThread.TmdbID))
					hasShare := false
					tCursor := b.Cursor()
					for tk, tv := tCursor.First(); tk != nil; tk, tv = tCursor.Next() {
						if string(tk) != data.ThreadHash {
							var other Thread
							if errDec := DecodeGob(tv, &other); errDec == nil {
								if other.TmdbID != nil && *other.TmdbID == *oldThread.TmdbID {
									hasShare = true
									break
								}
							}
						}
					}
					if !hasShare {
						_ = DeleteStreamsByTmdbID(tx, *oldThread.TmdbID)
					}
				}
			}
		}

		bytesData, err := EncodeGob(data)
		if err != nil {
			return err
		}
		err = b.Put([]byte(data.ThreadHash), bytesData)
		if err != nil {
			return err
		}

		if data.Status == "linked" && data.Catalog != "" {
			postedTime := time.Unix(0, 0)
			if data.PostedAt != nil {
				postedTime = *data.PostedAt
			}
			inverseTime := 9999999999 - postedTime.Unix()
			indexKey := fmt.Sprintf("cat:%s:%s:%010d:%s", data.Catalog, data.Type, inverseTime, data.ThreadHash)
			_ = idxB.Put([]byte(indexKey), []byte(data.ThreadHash))
		}

		if data.Status == "linked" && data.TmdbID != nil {
			threadIdxB := tx.Bucket([]byte("tmdb_thread_index"))
			_ = threadIdxB.Put([]byte(*data.TmdbID), []byte(data.ThreadHash))
		}

		_ = idB.Put([]byte(fmt.Sprintf("%d", data.ID)), []byte(data.ThreadHash))

		if strings.ToLower(data.Type) == "series" && data.Status == "linked" {
			_ = AutoEnrollSeries(tx, data)
		}

		return nil
	})
}

func DeleteThread(tx *bolt.Tx, t *Thread) error {
	return runUpdate(tx, func(tx *bolt.Tx) error {
		_ = tx.Bucket([]byte("threads")).Delete([]byte(t.ThreadHash))

		idxB := tx.Bucket([]byte("catalog_index"))
		c := idxB.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			if strings.HasSuffix(string(k), ":"+t.ThreadHash) {
				_ = idxB.Delete(k)
			}
		}

		if t.TmdbID != nil {
			_ = tx.Bucket([]byte("tmdb_thread_index")).Delete([]byte(*t.TmdbID))

			hasShare := false
			tb := tx.Bucket([]byte("threads"))
			tCursor := tb.Cursor()
			for tk, tv := tCursor.First(); tk != nil; tk, tv = tCursor.Next() {
				if string(tk) != t.ThreadHash {
					var other Thread
					if errDec := DecodeGob(tv, &other); errDec == nil {
						if other.TmdbID != nil && *other.TmdbID == *t.TmdbID {
							hasShare = true
							break
						}
					}
				}
			}

			if !hasShare {
				_ = DeleteStreamsByTmdbID(tx, *t.TmdbID)
			}
		}

		idB := tx.Bucket([]byte("thread_id_index"))
		if idB != nil {
			_ = idB.Delete([]byte(fmt.Sprintf("%d", t.ID)))
		}

		_ = DeleteMonitoredSeries(tx, t.ThreadHash)

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

// ── Stream CRUD Operations (Composite Key Architecture: tmdbID:infohash) ──

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

func DeleteStreamsByTmdbID(tx *bolt.Tx, tmdbID string) error {
	return runUpdate(tx, func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("streams"))
		if b == nil {
			return nil
		}
		prefix := []byte(tmdbID + ":")
		cursor := b.Cursor()
		var keysToDelete [][]byte

		for k, _ := cursor.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = cursor.Next() {
			keyCopy := make([]byte, len(k))
			copy(keyCopy, k)
			keysToDelete = append(keysToDelete, keyCopy)
		}

		for _, k := range keysToDelete {
			_ = b.Delete(k)
		}
		return nil
	})
}

func FindSeriesStreams(tx *bolt.Tx, tmdbID string, season, episode int) ([]Stream, error) {
	var allStreams []Stream
	err := runView(tx, func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("streams"))
		if b == nil {
			return nil
		}
		prefix := []byte(tmdbID + ":")
		cursor := b.Cursor()

		for k, v := cursor.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = cursor.Next() {
			var s Stream
			if errDec := DecodeGob(v, &s); errDec == nil {
				allStreams = append(allStreams, s)
			}
		}
		return nil
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
					if episode >= *s.Episode && episode <= *s.EpisodeEnd {
						match = true
					}
				} else {
					if *s.Episode == episode {
						match = true
					}
				}
			} else {
				match = true
			}
		} else if s.Season == nil && s.Episode == nil {
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
		if b == nil {
			return nil
		}
		prefix := []byte(tmdbID + ":")
		cursor := b.Cursor()

		for k, v := cursor.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = cursor.Next() {
			var s Stream
			if errDec := DecodeGob(v, &s); errDec == nil {
				allStreams = append(allStreams, s)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	sortStreamsByQuality(allStreams)
	return allStreams, nil
}

func CreateStreams(tx *bolt.Tx, streams []Stream) error {
	if len(streams) == 0 {
		return nil
	}
	return runUpdate(tx, func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("streams"))
		if b == nil {
			return bolt.ErrBucketNotFound
		}

		for _, s := range streams {
			if s.TmdbID == "" || s.Infohash == "" {
				continue
			}
			compositeKey := fmt.Sprintf("%s:%s", s.TmdbID, strings.ToLower(s.Infohash))
			encBytes, err := EncodeGob(s)
			if err != nil {
				return err
			}
			_ = b.Put([]byte(compositeKey), encBytes)
		}
		return nil
	})
}

// ── MonitoredSeries CRUD Operations ──

func GetMonitoredSeriesList() ([]MonitoredSeries, error) {
	var list []MonitoredSeries
	err := runView(nil, func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("monitored_series"))
		if b == nil {
			return nil
		}
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var ms MonitoredSeries
			if errDec := DecodeGob(v, &ms); errDec == nil {
				list = append(list, ms)
			}
		}
		return nil
	})
	sort.Slice(list, func(i, j int) bool {
		return list[i].LastUpdated.After(list[j].LastUpdated)
	})
	return list, err
}

func GetActiveMonitoredSeries() ([]MonitoredSeries, error) {
	var list []MonitoredSeries
	err := runView(nil, func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("monitored_series"))
		if b == nil {
			return nil
		}
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var ms MonitoredSeries
			if errDec := DecodeGob(v, &ms); errDec == nil {
				if ms.Status == "active" && ms.URL != "" {
					list = append(list, ms)
				}
			}
		}
		return nil
	})
	return list, err
}

func GetMonitoredSeriesByHash(tx *bolt.Tx, threadHash string) (*MonitoredSeries, error) {
	var ms MonitoredSeries
	err := runView(tx, func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("monitored_series"))
		if b == nil {
			return bolt.ErrBucketNotFound
		}
		data := b.Get([]byte(threadHash))
		if data == nil {
			return bolt.ErrBucketNotFound
		}
		return DecodeGob(data, &ms)
	})
	if err != nil {
		return nil, nil
	}
	return &ms, nil
}

func SetMonitoredSeries(tx *bolt.Tx, ms *MonitoredSeries) error {
	return runUpdate(tx, func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("monitored_series"))
		if b == nil {
			return bolt.ErrBucketNotFound
		}
		if ms.CreatedAt.IsZero() {
			ms.CreatedAt = time.Now()
		}
		ms.LastUpdated = time.Now()
		bytesData, errEnc := EncodeGob(*ms)
		if errEnc != nil {
			return errEnc
		}
		return b.Put([]byte(ms.ThreadHash), bytesData)
	})
}

func DeleteMonitoredSeries(tx *bolt.Tx, threadHash string) error {
	return runUpdate(tx, func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("monitored_series"))
		if b == nil {
			return nil
		}
		return b.Delete([]byte(threadHash))
	})
}

func AutoEnrollSeries(tx *bolt.Tx, t *Thread) error {
	if t == nil || strings.ToLower(t.Type) != "series" || t.ThreadHash == "" {
		return nil
	}
	return runUpdate(tx, func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("monitored_series"))
		if b == nil {
			return nil
		}

		title := t.CleanTitle
		if title == "" {
			title = t.RawTitle
		}

		existingData := b.Get([]byte(t.ThreadHash))
		if existingData != nil {
			var ms MonitoredSeries
			if errDec := DecodeGob(existingData, &ms); errDec == nil {
				ms.Title = title
				ms.RawTitle = t.RawTitle
				if t.URL != "" {
					ms.URL = t.URL
				}
				ms.LastUpdated = time.Now()
				bytesData, _ := EncodeGob(ms)
				return b.Put([]byte(t.ThreadHash), bytesData)
			}
		}

		ms := MonitoredSeries{
			ThreadHash:  t.ThreadHash,
			URL:         t.URL,
			Title:       title,
			RawTitle:    t.RawTitle,
			Status:      "active",
			LastChecked: time.Now(),
			LastUpdated: time.Now(),
			CreatedAt:   time.Now(),
		}
		bytesData, errEnc := EncodeGob(ms)
		if errEnc != nil {
			return errEnc
		}
		return b.Put([]byte(t.ThreadHash), bytesData)
	})
}

func AutoArchiveInactiveSeries(tx *bolt.Tx, inactivityDays int) (int, error) {
	archivedCount := 0
	err := runUpdate(tx, func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("monitored_series"))
		if b == nil {
			return nil
		}
		cutoff := time.Now().AddDate(0, 0, -inactivityDays)
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var ms MonitoredSeries
			if errDec := DecodeGob(v, &ms); errDec == nil {
				if ms.Status == "active" && ms.LastUpdated.Before(cutoff) {
					ms.Status = "archived"
					bytesData, errEnc := EncodeGob(ms)
					if errEnc == nil {
						_ = b.Put(k, bytesData)
						archivedCount++
					}
				}
			}
		}
		return nil
	})
	return archivedCount, err
}

// ── FailedThread Operations ──

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

// ── Lock Managers ──

func IsDebridCacheLocked(hash string) bool {
	locked := false
	_ = DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("debrid_cache_locks"))
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
