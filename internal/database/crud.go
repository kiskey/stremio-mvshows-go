// Version: 2.8.0
// Change log: Integrated metadata.NormalizeMediaType across local pre-lookup, graph visualizer, and series auto-enrollment; updated GetEntityGraphData to support canonical tt... primary keys.

package database

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/kiskey/stremio-mvshows-go/internal/services/metadata"
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

func FindLinkedThreadByTitleYearType(tx *bolt.Tx, title string, year int, mediaType string) (*Thread, error) {
	if title == "" || year <= 0 || mediaType == "" {
		return nil, nil
	}

	normTargetTitle := metadata.NormalizeTitleForMatching(title)
	if normTargetTitle == "" {
		return nil, nil
	}

	targetType := metadata.NormalizeMediaType(mediaType)
	var found *Thread

	err := runView(tx, func(tx *bolt.Tx) error {
		tb := tx.Bucket([]byte("threads"))
		if tb == nil {
			return nil
		}
		c := tb.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var t Thread
			if errDec := DecodeGob(v, &t); errDec == nil {
				// Factor 5: Status & TmdbID Prerequisite
				if t.Status != "linked" || t.TmdbID == nil || *t.TmdbID == "" {
					continue
				}

				// Factor 1: Strict Media Type Isolation
				if metadata.NormalizeMediaType(t.Type) != targetType {
					continue
				}

				// Factor 2: Exact Year Alignment
				if t.Year == nil || *t.Year != year {
					continue
				}

				// Factor 3: Exact Normalized Title Match
				normStoredTitle := metadata.NormalizeTitleForMatching(t.CleanTitle)
				if normStoredTitle == "" {
					normStoredTitle = metadata.NormalizeTitleForMatching(t.RawTitle)
				}

				if normStoredTitle == normTargetTitle {
					found = &t
					break
				}
			}
		}
		return nil
	})

	return found, err
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

		data.Type = metadata.NormalizeMediaType(data.Type)

		existingData := b.Get([]byte(data.ThreadHash))
		if existingData != nil {
			var oldThread Thread
			if errDec := DecodeGob(existingData, &oldThread); errDec == nil {
				oldThread.Type = metadata.NormalizeMediaType(oldThread.Type)

				if data.ID == 0 && oldThread.ID > 0 {
					data.ID = oldThread.ID
				}

				if data.URL == "" && oldThread.URL != "" {
					data.URL = oldThread.URL
				}

				// Merge and deduplicate magnet URIs on thread updates
				if len(oldThread.MagnetURIs) > 0 {
					seenMags := make(map[string]bool)
					var merged []string
					for _, m := range data.MagnetURIs {
						if m != "" && !seenMags[m] {
							seenMags[m] = true
							merged = append(merged, m)
						}
					}
					for _, m := range oldThread.MagnetURIs {
						if m != "" && !seenMags[m] {
							seenMags[m] = true
							merged = append(merged, m)
						}
					}
					data.MagnetURIs = merged
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

		if data.ID == 0 {
			seq, errSeq := b.NextSequence()
			if errSeq != nil {
				return errSeq
			}
			data.ID = uint(seq)
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

		if data.Type == "series" && data.Status == "linked" {
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

func BulkSetMonitoredSeriesStatus(tx *bolt.Tx, threadHashes []string, status string) (int, error) {
	if len(threadHashes) == 0 {
		return 0, nil
	}
	updatedCount := 0
	err := runUpdate(tx, func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("monitored_series"))
		if b == nil {
			return bolt.ErrBucketNotFound
		}

		if status == "delete" {
			for _, h := range threadHashes {
				if h != "" {
					_ = b.Delete([]byte(h))
					updatedCount++
				}
			}
			return nil
		}

		for _, h := range threadHashes {
			if h == "" {
				continue
			}
			existingData := b.Get([]byte(h))
			if existingData != nil {
				var ms MonitoredSeries
				if errDec := DecodeGob(existingData, &ms); errDec == nil {
					ms.Status = status
					ms.LastUpdated = time.Now()
					bytesData, errEnc := EncodeGob(ms)
					if errEnc == nil {
						_ = b.Put([]byte(h), bytesData)
						updatedCount++
					}
				}
			}
		}
		return nil
	})
	return updatedCount, err
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
	if t == nil || metadata.NormalizeMediaType(t.Type) != "series" || t.ThreadHash == "" {
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

// ── Panel G: Entity Relation Graph Data Traversal ──

type GraphEntity struct {
	Meta struct {
		TmdbID    string `json:"tmdb_id"`
		ImdbID    string `json:"imdb_id"`
		Title     string `json:"title"`
		Year      int    `json:"year"`
		GapStatus string `json:"gap_status"` // "HEALTHY", "MISSING_IMDB_GAP", "PENDING_MATCH", "UNLINKED"
		GapReason string `json:"gap_reason"`
	} `json:"meta"`
	Threads []GraphThread `json:"threads"`
	Streams []GraphStream `json:"streams"`
}

type GraphThread struct {
	ID          uint      `json:"id"`
	ThreadHash  string    `json:"thread_hash"`
	RawTitle    string    `json:"raw_title"`
	CleanTitle  string    `json:"clean_title"`
	Status      string    `json:"status"`
	Type        string    `json:"type"`
	Catalog     string    `json:"catalog"`
	URL         string    `json:"url"`
	MagnetCount int       `json:"magnet_count"`
	Monitored   string    `json:"monitored_status"`
	LastSeen    time.Time `json:"last_seen"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type GraphStream struct {
	ID       uint   `json:"id"`
	TmdbID   string `json:"tmdb_id"`
	Season   *int   `json:"season"`
	Episode  *int   `json:"episode"`
	Infohash string `json:"infohash"`
	Quality  string `json:"quality"`
	Language string `json:"language"`
	Locked   bool   `json:"is_locked"`
}

func GetEntityGraphData(tx *bolt.Tx, query string, contentType string) (*GraphEntity, error) {
	q := strings.TrimSpace(query)
	if q == "" {
		return nil, fmt.Errorf("query parameter is required")
	}

	normQuery := metadata.NormalizeTitleForMatching(q)
	mediaTypeFilter := metadata.NormalizeMediaType(contentType)

	graph := &GraphEntity{}
	var matchedTmdbID string
	var matchedImdbID string
	var primaryTitle string
	var primaryYear int

	monitoredMap := make(map[string]string)
	_ = runView(tx, func(tx *bolt.Tx) error {
		monB := tx.Bucket([]byte("monitored_series"))
		if monB != nil {
			c := monB.Cursor()
			for k, v := c.First(); k != nil; k, v = c.Next() {
				var ms MonitoredSeries
				if errDec := DecodeGob(v, &ms); errDec == nil {
					monitoredMap[ms.ThreadHash] = ms.Status
				}
			}
		}
		return nil
	})

	err := runView(tx, func(tx *bolt.Tx) error {
		tb := tx.Bucket([]byte("threads"))
		metaB := tx.Bucket([]byte("tmdb_metadata"))
		locksB := tx.Bucket([]byte("debrid_cache_locks"))
		streamsB := tx.Bucket([]byte("streams"))

		if metaB != nil {
			metaData := metaB.Get([]byte(q))
			if metaData != nil {
				var m TmdbMetadata
				if errDec := DecodeGob(metaData, &m); errDec == nil {
					matchedTmdbID = m.TmdbID
					if m.ImdbID != nil {
						matchedImdbID = *m.ImdbID
					}
					if m.Year != nil {
						primaryYear = *m.Year
					}
				}
			}
		}

		if tb != nil {
			c := tb.Cursor()
			for k, v := c.First(); k != nil; k, v = c.Next() {
				var t Thread
				if errDec := DecodeGob(v, &t); errDec == nil {
					if mediaTypeFilter != "" && mediaTypeFilter != "all" && metadata.NormalizeMediaType(t.Type) != mediaTypeFilter {
						continue
					}

					match := false
					if strings.HasPrefix(q, "tt") && t.TmdbID != nil {
						if (matchedTmdbID != "" && *t.TmdbID == matchedTmdbID) || *t.TmdbID == q {
							match = true
						}
					} else if matchedTmdbID != "" && t.TmdbID != nil && *t.TmdbID == matchedTmdbID {
						match = true
					} else {
						normClean := metadata.NormalizeTitleForMatching(t.CleanTitle)
						normRaw := metadata.NormalizeTitleForMatching(t.RawTitle)
						if normClean == normQuery || normRaw == normQuery || strings.Contains(normClean, normQuery) || strings.Contains(normRaw, normQuery) {
							match = true
							if matchedTmdbID == "" && t.TmdbID != nil {
								matchedTmdbID = *t.TmdbID
							}
						}
					}

					if match {
						if primaryTitle == "" {
							primaryTitle = t.CleanTitle
							if primaryTitle == "" {
								primaryTitle = t.RawTitle
							}
						}
						if primaryYear == 0 && t.Year != nil {
							primaryYear = *t.Year
						}

						mStatus := "none"
						if val, ok := monitoredMap[t.ThreadHash]; ok {
							mStatus = val
						}

						graph.Threads = append(graph.Threads, GraphThread{
							ID:          t.ID,
							ThreadHash:  t.ThreadHash,
							RawTitle:    t.RawTitle,
							CleanTitle:  t.CleanTitle,
							Status:      t.Status,
							Type:        t.Type,
							Catalog:     t.Catalog,
							URL:         t.URL,
							MagnetCount: len(t.MagnetURIs),
							Monitored:   mStatus,
							LastSeen:    t.LastSeen,
							UpdatedAt:   t.UpdatedAt,
						})
					}
				}
			}
		}

		if matchedTmdbID != "" && metaB != nil {
			data := metaB.Get([]byte(matchedTmdbID))
			if data != nil {
				var m TmdbMetadata
				if errDec := DecodeGob(data, &m); errDec == nil {
					if m.ImdbID != nil && *m.ImdbID != "" {
						matchedImdbID = *m.ImdbID
					}
					if m.Year != nil {
						primaryYear = *m.Year
					}
				}
			}
		}

		// Traverse streams using matched ID or canonical tt... ID
		lookupStreamIDs := []string{}
		if matchedTmdbID != "" { lookupStreamIDs = append(lookupStreamIDs, matchedTmdbID) }
		if matchedImdbID != "" && matchedImdbID != matchedTmdbID { lookupStreamIDs = append(lookupStreamIDs, matchedImdbID) }

		if len(lookupStreamIDs) > 0 && streamsB != nil {
			seenStreamKeys := make(map[string]bool)

			for _, targetID := range lookupStreamIDs {
				prefix := []byte(targetID + ":")
				sCursor := streamsB.Cursor()
				for k, v := sCursor.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = sCursor.Next() {
					if seenStreamKeys[string(k)] { continue }
					seenStreamKeys[string(k)] = true

					var s Stream
					if errDec := DecodeGob(v, &s); errDec == nil {
						isLocked := false
						if locksB != nil && locksB.Get([]byte(strings.ToLower(s.Infohash))) != nil {
							isLocked = true
						}
						graph.Streams = append(graph.Streams, GraphStream{
							ID:       s.ID,
							TmdbID:   s.TmdbID,
							Season:   s.Season,
							Episode:  s.Episode,
							Infohash: s.Infohash,
							Quality:  s.Quality,
							Language: s.Language,
							Locked:   isLocked,
						})
					}
				}
			}
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	graph.Meta.TmdbID = matchedTmdbID
	graph.Meta.ImdbID = matchedImdbID
	graph.Meta.Title = primaryTitle
	graph.Meta.Year = primaryYear

	if matchedTmdbID != "" {
		if matchedImdbID != "" || strings.HasPrefix(matchedTmdbID, "tt") {
			graph.Meta.GapStatus = "HEALTHY"
			graph.Meta.GapReason = "100% Verified Link: Both TMDB ID and IMDb ID mapped cleanly."
		} else {
			graph.Meta.GapStatus = "MISSING_IMDB_GAP"
			graph.Meta.GapReason = "IMDb Pointer Gap: TMDB ID present, but secondary IMDb ID pointer missing."
		}
	} else if len(graph.Threads) > 0 {
		hasPending := false
		for _, th := range graph.Threads {
			if th.Status == "pending_tmdb" {
				hasPending = true
				break
			}
		}
		if hasPending {
			graph.Meta.GapStatus = "PENDING_MATCH"
			graph.Meta.GapReason = "Pending Matching Pool: Thread in pending_tmdb queue awaiting linking."
		} else {
			graph.Meta.GapStatus = "UNLINKED"
			graph.Meta.GapReason = "Unlinked Thread: Thread present in storage without assigned TMDB ID."
		}
	} else {
		graph.Meta.GapStatus = "NOT_FOUND"
		graph.Meta.GapReason = "No matching database records located for query."
	}

	return graph, nil
}
