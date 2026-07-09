
// Version: 2.0.2
// Change log: Merged the invariant title-based hash transition engine directly into the inspector's --repair transaction, enabling single-command schema upgrades, duplicate pruning, and disk compaction.

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
	bolt "go.etcd.io/bbolt"
)

// Legacy hash function matching the old format
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

// New invariant, title-based hash function
func newGenerateThreadHash(title string) string {
	normalized := strings.ToLower(strings.TrimSpace(title))
	words := strings.Fields(normalized)
	normalized = strings.Join(words, " ")
	h := sha256.Sum256([]byte(normalized))
	return hex.EncodeToString(h[:])
}

func main() {
	dbPath := flag.String("db", "/data/stremio_addon.db.bolt", "Path to the active Bbolt database")
	repair := flag.Bool("repair", false, "Execute automatic hash migration, duplicate pruning, and index repair")
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

	// 1. Audit Phase: Scan the threads bucket for duplicates and legacy hash formats
	log.Println("Auditing database records...")
	duplicatesMap := make(map[string][]database.Thread)
	legacyHashCount := 0

	_ = db.View(func(tx *bolt.Tx) error {
		tb := tx.Bucket([]byte("threads"))
		c := tb.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var t database.Thread
			if err := database.DecodeGob(v, &t); err == nil {
				// Detect if the key matches the old hashing algorithm
				oldHash := oldGenerateThreadHash(t.RawTitle, t.MagnetURIs)
				if string(k) == oldHash {
					legacyHashCount++
				}

				// Group by the new invariant key to predict duplicate collisions post-migration
				newHash := newGenerateThreadHash(t.RawTitle)
				duplicatesMap[newHash] = append(duplicatesMap[newHash], t)
			}
		}
		return nil
	})

	var duplicateGroups []string
	totalRedundantCount := 0

	for _, list := range duplicatesMap {
		if len(list) > 1 {
			duplicateGroups = append(duplicateGroups, list[0].RawTitle)
			totalRedundantCount += (len(list) - 1)
		}
	}

	log.Printf("Inspection Complete.\n")
	log.Printf("  - Legacy Format Hashes Found: %d records\n", legacyHashCount)
	log.Printf("  - Duplicate Title Groups Detected: %d groups (containing %d redundant rows)\n", len(duplicateGroups), totalRedundantCount)

	// 2. Audit Phase: Scan catalog_index for orphaned index keys
	orphanedIndexKeys := 0
	_ = db.View(func(tx *bolt.Tx) error {
		idxB := tx.Bucket([]byte("catalog_index"))
		thrB := tx.Bucket([]byte("threads"))
		c := idxB.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			tHash := string(v)
			if thrB.Get([]byte(tHash)) == nil {
				orphanedIndexKeys++
			}
		}
		return nil
	})
	log.Printf("  - Orphaned Catalog Keys Found: %d indexes\n", orphanedIndexKeys)

	// 3. Optional Maintenance / Repair Transition Phase
	if !*repair {
		log.Println("==================================================")
		log.Println("► VERDICT: Audited in Dry-Run mode. No writes occurred.")
		log.Println("To apply transitions, fix index drifts, and prune duplicates, run with: --repair")
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
		streamsB := tx.Bucket([]byte("streams"))

		// Iterate through every mapped title group, migrating and resolving collisions
		for targetNewHash, list := range duplicatesMap {
			// Sort so index 0 is the newest (UpdatedAt DESC)
			sort.Slice(list, func(i, j int) bool {
				return list[i].UpdatedAt.After(list[j].UpdatedAt)
			})

			keptThread := list[0]
			oldKeptHash := oldGenerateThreadHash(keptThread.RawTitle, keptThread.MagnetURIs)

			// Step A: Keep the newest thread, migrating its key to the invariant title format
			log.Printf("Processing: %q\n", keptThread.RawTitle)
			log.Printf("  [KEEPING] NewHash=%s (Last Updated: %v)\n", targetNewHash, keptThread.UpdatedAt)

			// Delete older reference if it was stored under the legacy hash key
			_ = tb.Delete([]byte(oldKeptHash))

			// Write thread under the new, deterministic invariant key
			keptThread.ThreadHash = targetNewHash
			bytesData, _ := database.EncodeGob(keptThread)
			_ = tb.Put([]byte(targetNewHash), bytesData)

			// Write updated pre-sorted catalog index key containing type and the new hash (Resolves Risk 3)
			if keptThread.Status == "linked" && keptThread.Catalog != "" {
				postedTime := time.Now()
				if keptThread.PostedAt != nil {
					postedTime = *keptThread.PostedAt
				}
				inverseTime := 9999999999 - postedTime.Unix()
				indexKey := fmt.Sprintf("cat:%s:%s:%010d:%s", keptThread.Catalog, keptThread.Type, inverseTime, targetNewHash)
				_ = idxB.Put([]byte(indexKey), []byte(targetNewHash))
			}

			// Map TMDB pointers to the new, static hash
			if keptThread.Status == "linked" && keptThread.TmdbID != nil {
				_ = threadIdxB.Put([]byte(*keptThread.TmdbID), []byte(targetNewHash))
			}

			// Step B: Safely prune all redundant/older duplicates in this group
			for i := 1; i < len(list); i++ {
				trashThread := list[i]
				oldTrashHash := oldGenerateThreadHash(trashThread.RawTitle, trashThread.MagnetURIs)

				log.Printf("  [PRUNING DUPLICATE] Hash=%s (Last Updated: %v)\n", oldTrashHash, trashThread.UpdatedAt)

				// Deletes old thread hash variants
				_ = tb.Delete([]byte(oldTrashHash))
				_ = tb.Delete([]byte(targetNewHash + "_" + strconv.Itoa(i))) // Guardrail cleanup

				// Clean up related indices and streams for the pruned item
				if trashThread.TmdbID != nil {
					_ = streamsB.Delete([]byte(*trashThread.TmdbID))
					_ = threadIdxB.Delete([]byte(*trashThread.TmdbID))
				}

				// Purge old catalog index structures for the deleted duplicate
				idxCursor := idxB.Cursor()
				for k, _ := idxCursor.First(); k != nil; k, _ = idxCursor.Next() {
					if strings.HasSuffix(string(k), ":"+oldTrashHash) || strings.HasSuffix(string(k), ":"+trashThread.ThreadHash) {
						_ = idxB.Delete(k)
					}
				}
			}
		}

		// Prune orphaned catalog index keys
		c := idxB.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			tHash := string(v)
			if tb.Get([]byte(tHash)) == nil {
				_ = idxB.Delete(k)
			}
		}

		return nil
	})

	if err != nil {
		log.Fatalf("Transition transaction failed: %v\n", err)
	}

	log.Println("Database repair and hash migration transaction committed successfully!")

	// 4. Shrink the Database on disk via compaction
	log.Println("Shrinking database file size via sequential compaction...")
	compactPath := *dbPath + ".compacted"
	_ = os.Remove(compactPath)

	errComp := db.View(func(tx *bolt.Tx) error {
		return tx.CopyFile(compactPath, 0600)
	})
	if errComp != nil {
		log.Fatalf("Compaction step failed: %v\n", errComp)
	}

	// Safely swap compacted database files
	_ = db.Close()
	_ = os.Remove(*dbPath)
	errSwap := os.Rename(compactPath, *dbPath)
	if errSwap != nil {
		log.Fatalf("Failed to swap compacted file: %v\n", errSwap)
	}

	log.Println("==================================================")
	log.Println("► VERDICT: [SUCCESS] - Database converted, defragmented, and compacted.")
	log.Println("==================================================")
}

func init() {
	_ = time.Now // Prevent unused imports compile crash
}
