// Version: 2.7.0
// Change log: Implemented getThreadInvariantGroupKey using type_year_cleantitle fallback for legacy entries missing URLs, enabling full consolidation of historical duplicate threads.

package main

import (
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/kiskey/stremio-mvshows-go/internal/database"
	"github.com/kiskey/stremio-mvshows-go/internal/services/parser"
	bolt "go.etcd.io/bbolt"
)

func oldGenerateThreadHash(title string, magnetURIs []string) string {
	sorted := make([]string, len(magnetURIs))
	copy(sorted, magnetURIs)
	sortStrings(sorted)
	data := title + strings.Join(sorted, "")
	h := sha256.Sum256([]byte(data))
	return hex.EncodeToString(h[:])
}

func sortStrings(slice []string) {
	for i := 0; i < len(slice); i++ {
		for j := i + 1; j < len(slice); j++ {
			if slice[i] > slice[j] {
				slice[i], slice[j] = slice[j], slice[i]
			}
		}
	}
}

func formatBytes(bytes int64) string {
	if bytes < 1024 {
		return fmt.Sprintf("%d B", bytes)
	}
	if bytes < 1024*1024 {
		return fmt.Sprintf("%.2f KB", float64(bytes)/1024)
	}
	return fmt.Sprintf("%.2f MB", float64(bytes)/1024/1024)
}

// getThreadInvariantGroupKey constructs an invariant grouping key:
// 1. Primary: Topic ID if URL is available (topic_<id>)
// 2. Fallback: type_year_cleantitle (e.g. series_2025_lbw love beyond wicket)
func getThreadInvariantGroupKey(t *database.Thread) string {
	if t.URL != "" {
		topicID := parser.ExtractTopicID(t.URL)
		if topicID != "" {
			h := sha256.Sum256([]byte("topic_" + topicID))
			return hex.EncodeToString(h[:])
		}
	}

	cleanTitle := t.CleanTitle
	yearVal := 0
	if t.Year != nil {
		yearVal = *t.Year
	}

	pr := parser.ParseRelease(t.RawTitle, t.Type)
	if pr != nil && pr.IsValid && pr.CleanTitle != "" {
		cleanTitle = pr.CleanTitle
		if yearVal == 0 && pr.Year > 0 {
			yearVal = pr.Year
		}
	}

	cleanNorm := strings.ToLower(strings.TrimSpace(cleanTitle))
	cleanNorm = strings.Join(strings.Fields(cleanNorm), " ")

	keyStr := fmt.Sprintf("%s_%d_%s", strings.ToLower(t.Type), yearVal, cleanNorm)
	h := sha256.Sum256([]byte(keyStr))
	return hex.EncodeToString(h[:])
}

func main() {
	dbPath := flag.String("db", "/data/stremio_addon.db.bolt", "Path to the active Bbolt database")
	repair := flag.Bool("repair", false, "Execute automatic hash migration, duplicate pruning, stream regenerator, and index repair")
	pruneFailuresDays := flag.Int("prune-failures-older-than", 7, "Prune failed threads older than N days")
	flag.Parse()

	log.Println("==================================================")
	log.Println("► BBOLT UNIFIED DIAGNOSTIC & TRANSITION INSPECTOR")
	log.Printf("Target Database: %s\n", *dbPath)
	log.Println("==================================================")

	if _, err := os.Stat(*dbPath); os.IsNotExist(err) {
		log.Fatalf("Critical: Database file does not exist at %s\n", *dbPath)
	}

	db, err := database.Init(*dbPath)
	if err != nil {
		log.Fatalf("Failed to open database connection: %v\n", err)
	}
	defer db.Close()

	log.Println("Auditing database records for Topic ID and Clean Title invariant duplicates...")
	duplicatesMap := make(map[string][]database.Thread)
	legacyHashCount := 0

	_ = db.View(func(tx *bolt.Tx) error {
		tb := tx.Bucket([]byte("threads"))
		c := tb.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var t database.Thread
			if err := database.DecodeGob(v, &t); err == nil {
				t.ThreadHash = string(k)

				if len(t.MagnetURIs) > 0 {
					oldHash := oldGenerateThreadHash(t.RawTitle, t.MagnetURIs)
					if string(k) == oldHash && oldHash != getThreadInvariantGroupKey(&t) {
						legacyHashCount++
					}
				}

				targetHash := getThreadInvariantGroupKey(&t)
				duplicatesMap[targetHash] = append(duplicatesMap[targetHash], t)
			}
		}
		return nil
	})

	var duplicateTitles []string
	totalRedundantCount := 0

	for _, list := range duplicatesMap {
		if len(list) > 1 {
			duplicateTitles = append(duplicateTitles, list[0].RawTitle)
			totalRedundantCount += (len(list) - 1)
		}
	}

	log.Printf("Inspection Complete.\n")
	log.Printf("  - Legacy Format Hashes Found: %d records\n", legacyHashCount)
	log.Printf("  - Duplicate Title/Topic ID Groups Detected: %d groups (containing %d redundant rows)\n", len(duplicateTitles), totalRedundantCount)

	var orphanedIndexKeys [][]byte
	_ = db.View(func(tx *bolt.Tx) error {
		idxB := tx.Bucket([]byte("catalog_index"))
		thrB := tx.Bucket([]byte("threads"))
		c := idxB.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			tHash := string(v)
			if thrB.Get([]byte(tHash)) == nil {
				tempKey := make([]byte, len(k))
				copy(tempKey, k)
				orphanedIndexKeys = append(orphanedIndexKeys, tempKey)
			}
		}
		return nil
	})
	log.Printf("  - Orphaned Catalog Keys Found: %d indexes\n", len(orphanedIndexKeys))

	log.Println("==================================================")
	log.Println("► BBOLT PHYSICAL FILE PAGE STATS REPORT")
	log.Println("==================================================")

	var totalKeys int
	var totalInuseBytes int64
	var totalAllocatedBytes int64

	_ = db.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			stats := b.Stats()
			totalKeys += stats.KeyN

			inuse := int64(stats.BranchInuse) + int64(stats.LeafInuse)
			allocated := int64(stats.BranchAlloc) + int64(stats.LeafAlloc)

			totalInuseBytes += inuse
			totalAllocatedBytes += allocated

			pageCount := stats.BranchPageN + stats.BranchOverflowN + stats.LeafPageN + stats.LeafOverflowN
			overflowPageCount := stats.BranchOverflowN + stats.LeafOverflowN

			log.Printf("Bucket: %q\n", string(name))
			log.Printf("  - KeyCount:            %d keys\n", stats.KeyN)
			log.Printf("  - Total Pages:         %d pages (including %d overflow pages)\n", pageCount, overflowPageCount)
			log.Printf("  - Logical Space InUse: %s (Allocated space: %s)\n", formatBytes(inuse), formatBytes(allocated))
			if allocated > 0 {
				log.Printf("  - Page Fill Ratio:     %.1f%%\n", (float64(inuse)/float64(allocated))*100)
			}
			return nil
		})
	})

	var diskSize int64
	if stat, err := os.Stat(*dbPath); err == nil {
		diskSize = stat.Size()
	}

	var fragmentationPct float64
	if totalAllocatedBytes > 0 {
		fillRatio := (float64(totalInuseBytes) / float64(totalAllocatedBytes)) * 100.0
		fragmentationPct = 100.0 - fillRatio
	}

	log.Println("--------------------------------------------------")
	log.Printf("NATIVE PERFORMANCE SUMMARY:\n")
	log.Printf("  - Overall Keys Tracked:         %d entries\n", totalKeys)
	log.Printf("  - Logical Content In-Use:       %s\n", formatBytes(totalInuseBytes))
	log.Printf("  - Virtual Mapped Allocations:   %s\n", formatBytes(totalAllocatedBytes))
	log.Printf("  - Storage Page Fragmentation:   %.1f%%\n", fragmentationPct)
	log.Printf("  - Physical File Size on Disk:   %s\n", formatBytes(diskSize))
	if diskSize > 0 {
		log.Printf("  - Total Storage Efficiency:     %.1f%%\n", (float64(totalInuseBytes)/float64(diskSize))*100)
	}

	var missingThreadIdxKeys bool
	_ = db.View(func(tx *bolt.Tx) error {
		tb := tx.Bucket([]byte("threads"))
		threadIdxB := tx.Bucket([]byte("tmdb_thread_index"))
		c := tb.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var t database.Thread
			if errDec := database.DecodeGob(v, &t); errDec == nil {
				if t.Status == "linked" && t.TmdbID != nil {
					if threadIdxB.Get([]byte(*t.TmdbID)) == nil {
						missingThreadIdxKeys = true
						break
					}
				}
			}
		}
		return nil
	})

	if !*repair && legacyHashCount == 0 && len(duplicateTitles) == 0 && len(orphanedIndexKeys) == 0 && !missingThreadIdxKeys {
		log.Println("==================================================")
		log.Println("► VERDICT: [CLEAN] - No structural anomalies found in database.")
		log.Println("==================================================")
		return
	}

	if !*repair {
		log.Println("==================================================")
		log.Println("► VERDICT: Audited in Dry-Run mode. No writes occurred.")
		log.Printf("  - High-Speed Lookups Missing Indexes: %v\n", missingThreadIdxKeys)
		log.Println("To apply transitions, compile missing indices, optimize magnets, and prune duplicates, run with: --repair")
		log.Println("==================================================")
		return
	}

	log.Println("==================================================")
	log.Println("► INITIATING AUTOMATED REPAIR & TRANSITION PHASE...")
	log.Println("==================================================")

	err = db.Update(func(tx *bolt.Tx) error {
		tb := tx.Bucket([]byte("threads"))
		idxB := tx.Bucket([]byte("catalog_index"))
		threadIdxB := tx.Bucket([]byte("tmdb_thread_index"))
		idB, _ := tx.CreateBucketIfNotExists([]byte("thread_id_index"))
		monitoredB, _ := tx.CreateBucketIfNotExists([]byte("monitored_series"))
		streamsB := tx.Bucket([]byte("streams"))
		metaB := tx.Bucket([]byte("tmdb_metadata"))
		magnetB := tx.Bucket([]byte("magnet_cache"))

		log.Println("Re-indexing Metadata bucket into high-speed Dual-Key layout...")
		var metadataToRewrite []database.TmdbMetadata
		metaCursor := metaB.Cursor()
		for k, v := metaCursor.First(); k != nil; k, v = metaCursor.Next() {
			var m database.TmdbMetadata
			if errDec := database.DecodeGob(v, &m); errDec == nil {
				if string(k) == m.TmdbID {
					metadataToRewrite = append(metadataToRewrite, m)
				}
			}
		}

		for _, m := range metadataToRewrite {
			bytesData, _ := database.EncodeGob(m)
			_ = metaB.Put([]byte(m.TmdbID), bytesData)
			if m.ImdbID != nil && *m.ImdbID != "" {
				_ = metaB.Put([]byte(*m.ImdbID), bytesData)
			}
		}

		for targetNewHash, list := range duplicatesMap {
			sort.Slice(list, func(i, j int) bool {
				return list[i].UpdatedAt.After(list[j].UpdatedAt)
			})

			keptThread := list[0]

			log.Printf("Processing & Compacting Group: %q (Hash: %s)\n", keptThread.RawTitle, targetNewHash)

			seenMags := make(map[string]bool)
			var allMergedMags []string

			for _, item := range list {
				for _, m := range item.MagnetURIs {
					cleanM := parser.StripTrackersFromMagnet(m)
					if cleanM != "" && !seenMags[cleanM] {
						seenMags[cleanM] = true
						allMergedMags = append(allMergedMags, cleanM)
					}
				}
				if keptThread.URL == "" && item.URL != "" {
					keptThread.URL = item.URL
				}
			}
			keptThread.MagnetURIs = allMergedMags

			if keptThread.URL != "" {
				keptThread.URL = parser.FormatCanonicalTopicURL(keptThread.URL)
			}

			if keptThread.ThreadHash != targetNewHash {
				_ = tb.Delete([]byte(keptThread.ThreadHash))
			}

			prTitle := parser.ParseRelease(keptThread.RawTitle, keptThread.Type)
			if prTitle.IsValid && prTitle.CleanTitle != "" {
				keptThread.CleanTitle = prTitle.CleanTitle
			}

			if keptThread.ID == 0 {
				seq, errSeq := tb.NextSequence()
				if errSeq == nil {
					keptThread.ID = uint(seq)
				}
			}

			keptThread.ThreadHash = targetNewHash
			bytesData, _ := database.EncodeGob(keptThread)
			_ = tb.Put([]byte(targetNewHash), bytesData)

			if idB != nil && keptThread.ID > 0 {
				_ = idB.Put([]byte(fmt.Sprintf("%d", keptThread.ID)), []byte(targetNewHash))
			}

			if keptThread.Status == "linked" && keptThread.Catalog != "" {
				postedTime := time.Unix(0, 0)
				if keptThread.PostedAt != nil {
					postedTime = *keptThread.PostedAt
				}
				inverseTime := 9999999999 - postedTime.Unix()
				indexKey := fmt.Sprintf("cat:%s:%s:%010d:%s", keptThread.Catalog, keptThread.Type, inverseTime, targetNewHash)
				_ = idxB.Put([]byte(indexKey), []byte(targetNewHash))
			}

			if keptThread.Status == "linked" && keptThread.TmdbID != nil {
				_ = threadIdxB.Put([]byte(*keptThread.TmdbID), []byte(targetNewHash))
			}

			if strings.ToLower(keptThread.Type) == "series" && keptThread.Status == "linked" {
				_ = database.AutoEnrollSeries(tx, &keptThread)
			}

			for i := 1; i < len(list); i++ {
				trashThread := list[i]

				log.Printf("  [PRUNING DUPLICATE THREAD] Hash=%s Title=%q\n", trashThread.ThreadHash, trashThread.RawTitle)

				_ = tb.Delete([]byte(trashThread.ThreadHash))

				if trashThread.ID > 0 && idB != nil {
					_ = idB.Delete([]byte(fmt.Sprintf("%d", trashThread.ID)))
				}

				if monitoredB != nil {
					_ = monitoredB.Delete([]byte(trashThread.ThreadHash))
				}

				var indexKeysPrune [][]byte
				idxCursor := idxB.Cursor()
				for k, _ := idxCursor.First(); k != nil; k, _ = idxCursor.Next() {
					if strings.HasSuffix(string(k), ":"+trashThread.ThreadHash) {
						tempKey := make([]byte, len(k))
						copy(tempKey, k)
						indexKeysPrune = append(indexKeysPrune, tempKey)
					}
				}

				for _, k := range indexKeysPrune {
					_ = idxB.Delete(k)
				}
			}
		}

		log.Println("Populating high-speed Thread index pointers bucket...")
		threadCursor := tb.Cursor()
		for k, v := threadCursor.First(); k != nil; k, v = threadCursor.Next() {
			var t database.Thread
			if errDec := database.DecodeGob(v, &t); errDec == nil {
				if t.Status == "linked" && t.TmdbID != nil {
					_ = threadIdxB.Put([]byte(*t.TmdbID), k)
				}
				if idB != nil && t.ID > 0 {
					_ = idB.Put([]byte(fmt.Sprintf("%d", t.ID)), k)
				}
				if strings.ToLower(t.Type) == "series" && t.Status == "linked" {
					_ = database.AutoEnrollSeries(tx, &t)
				}
			}
		}

		log.Println("Compacting magnet_cache bucket (removing redundant trackers)...")
		var magnetCacheToRewrite []database.MagnetCache
		magnetCursor := magnetB.Cursor()
		for k, v := magnetCursor.First(); k != nil; k, v = magnetCursor.Next() {
			var mc database.MagnetCache
			if errDec := database.DecodeGob(v, &mc); errDec == nil {
				mc.Magnet = parser.StripTrackersFromMagnet(mc.Magnet)
				magnetCacheToRewrite = append(magnetCacheToRewrite, mc)
			}
		}
		for _, mc := range magnetCacheToRewrite {
			bytesData, _ := database.EncodeGob(mc)
			_ = magnetB.Put([]byte(mc.Infohash), bytesData)
		}

		log.Println("Regenerating and correcting all stream indices from raw magnets using composite keys...")
		var streamKeysToDelete [][]byte
		streamsCursor := streamsB.Cursor()
		for k, _ := streamsCursor.First(); k != nil; k, _ = streamsCursor.Next() {
			tempKey := make([]byte, len(k))
			copy(tempKey, k)
			streamKeysToDelete = append(streamKeysToDelete, tempKey)
		}
		for _, k := range streamKeysToDelete {
			_ = streamsB.Delete(k)
		}

		var allRegenStreams []database.Stream
		threadCursorForStreams := tb.Cursor()
		for k, v := threadCursorForStreams.First(); k != nil; k, v = threadCursorForStreams.Next() {
			var t database.Thread
			if errDec := database.DecodeGob(v, &t); errDec == nil {
				if t.Status == "linked" && t.TmdbID != nil {
					isSeries := strings.ToLower(t.Type) == "series"
					for _, magnet := range t.MagnetURIs {
						parsedMagnet := parser.ParseMagnet(magnet, t.Type)
						if parsedMagnet == nil {
							continue
						}

						cleanMagnet := parser.StripTrackersFromMagnet(magnet)
						cacheRecord := database.MagnetCache{
							Infohash:  parsedMagnet.Infohash,
							Magnet:    cleanMagnet,
							CreatedAt: time.Now(),
						}
						cacheBytes, _ := database.EncodeGob(cacheRecord)
						_ = magnetB.Put([]byte(parsedMagnet.Infohash), cacheBytes)

						stream := database.Stream{
							TmdbID:    *t.TmdbID,
							Infohash:  parsedMagnet.Infohash,
							Quality:   parsedMagnet.Quality,
							Language:  parsedMagnet.Language,
							CreatedAt: time.Now(),
							UpdatedAt: time.Now(),
						}

						if isSeries {
							seasonVal := parsedMagnet.Season
							if seasonVal == 0 {
								seasonVal = 1
							}
							stream.Season = &seasonVal

							if parsedMagnet.Type == "SINGLE_EPISODE" {
								epVal := parsedMagnet.Episode
								stream.Episode = &epVal
								stream.EpisodeEnd = &epVal
							} else if parsedMagnet.Type == "EPISODE_PACK" {
								startVal := parsedMagnet.EpisodeStart
								endVal := parsedMagnet.EpisodeEnd
								stream.Episode = &startVal
								stream.EpisodeEnd = &endVal
							}
						}

						allRegenStreams = append(allRegenStreams, stream)
					}
				}
			}
		}

		if len(allRegenStreams) > 0 {
			log.Printf("Successfully generated %d repaired stream pointers. Writing composite keys (tmdbID:infohash) to Bbolt streams bucket...\n", len(allRegenStreams))
			streamIDCounter := uint(0)
			for _, s := range allRegenStreams {
				streamIDCounter++
				s.ID = streamIDCounter
				compositeKey := fmt.Sprintf("%s:%s", s.TmdbID, strings.ToLower(s.Infohash))
				bytesData, _ := database.EncodeGob(s)
				_ = streamsB.Put([]byte(compositeKey), bytesData)
			}
		}

		if len(orphanedIndexKeys) > 0 {
			log.Printf("Pruning %d orphaned keys from catalog_index...\n", len(orphanedIndexKeys))
			for _, k := range orphanedIndexKeys {
				_ = idxB.Delete(k)
			}
		}

		log.Printf("Compacting failed_threads bucket (pruning logs older than %d days)...\n", *pruneFailuresDays)
		var failedKeysToPrune [][]byte
		failedCursor := tx.Bucket([]byte("failed_threads")).Cursor()
		cutoffFailed := time.Now().AddDate(0, 0, -*pruneFailuresDays)
		for k, v := failedCursor.First(); k != nil; k, v = failedCursor.Next() {
			var ft database.FailedThread
			if errDec := database.DecodeGob(v, &ft); errDec == nil {
				if ft.LastAttempt.Before(cutoffFailed) {
					failedKeysToPrune = append(failedKeysToPrune, k)
				}
			}
		}
		if len(failedKeysToPrune) > 0 {
			log.Printf("Pruning %d expired failed thread log lines...\n", len(failedKeysToPrune))
			for _, k := range failedKeysToPrune {
				_ = tx.Bucket([]byte("failed_threads")).Delete(k)
			}
		}

		log.Println("Auditing debrid_cache_locks bucket (pruning locks older than 24 hours)...")
		var lockKeysToPrune [][]byte
		lockCursor := tx.Bucket([]byte("debrid_cache_locks")).Cursor()
		cutoffLock := time.Now().Add(-24 * time.Hour)
		for k, v := lockCursor.First(); k != nil; k, v = lockCursor.Next() {
			var l database.DebridCacheLock
			if errDec := database.DecodeGob(v, &l); errDec == nil {
				if l.CreatedAt.Before(cutoffLock) {
					lockKeysToPrune = append(lockKeysToPrune, k)
				}
			}
		}
		if len(lockKeysToPrune) > 0 {
			log.Printf("Pruning %d stale debrid cache locks...\n", len(lockKeysToPrune))
			for _, k := range lockKeysToPrune {
				_ = tx.Bucket([]byte("debrid_cache_locks")).Delete(k)
			}
		}

		log.Println("Auditing torbox_id_map bucket...")
		var torboxKeysToPrune [][]byte
		torboxCursor := tx.Bucket([]byte("torbox_id_map")).Cursor()
		for k, v := torboxCursor.First(); k != nil; k, v = torboxCursor.Next() {
			var m database.TorboxIdMap
			if errDec := database.DecodeGob(v, &m); errDec == nil {
				if tx.Bucket([]byte("magnet_cache")).Get([]byte(m.Hash)) == nil {
					torboxKeysToPrune = append(torboxKeysToPrune, k)
				}
			}
		}
		if len(torboxKeysToPrune) > 0 {
			log.Printf("Pruning %d orphaned keys from torbox_id_map...\n", len(torboxKeysToPrune))
			for _, k := range torboxKeysToPrune {
				_ = tx.Bucket([]byte("torbox_id_map")).Delete(k)
			}
		}

		return nil
	})

	if err != nil {
		log.Fatalf("Transition transaction failed: %v\n", err)
	}

	log.Println("Database repair, stream regeneration, index compiling, and hash migration committed successfully!")

	log.Println("Shrinking database file size via sequential compaction...")
	compactPath := *dbPath + ".compacted"
	_ = os.Remove(compactPath)

	errComp := db.View(func(tx *bolt.Tx) error {
		return tx.CopyFile(compactPath, 0600)
	})
	if errComp != nil {
		log.Fatalf("Compaction step failed: %v\n", errComp)
	}

	_ = db.Close()
	_ = os.Remove(*dbPath)
	errSwap := os.Rename(compactPath, *dbPath)
	if errSwap != nil {
		log.Fatalf("Failed to swap compacted file: %v\n", errSwap)
	}

	log.Println("==================================================")
	log.Printf("► VERDICT: [SUCCESS] - COMPACTION LOG COMPLETED SUCCESFULLY.\n")
	log.Println("==================================================")
}
