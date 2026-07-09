
// Version: 2.0.0
// Description: Custom diagnostic and automated repair utility for BoltDB, scanning for duplicated titles, catalog index drifts, and executing safe disk compaction.

package main

import (
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

func main() {
	dbPath := flag.String("db", "/data/stremio_addon.db.bolt", "Path to the active Bbolt database")
	repair := flag.Bool("repair", false, "Execute automatic duplicate pruning and index repair")
	flag.Parse()

	log.Println("==================================================")
	log.Println("► BBOLT DATABASE DIAGNOSTIC INSPECTOR")
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

	// 1. Audit Phase: Scan the threads bucket for duplicates by Raw Title
	log.Println("Auditing database records...")
	duplicatesMap := make(map[string][]database.Thread)

	_ = db.View(func(tx *bolt.Tx) error {
		tb := tx.Bucket([]byte("threads"))
		c := tb.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var t database.Thread
			if err := database.DecodeGob(v, &t); err == nil {
				duplicatesMap[t.RawTitle] = append(duplicatesMap[t.RawTitle], t)
			}
		}
		return nil
	})

	var duplicateTitles []string
	totalOrphanedThreadsCount := 0

	for title, list := range duplicatesMap {
		if len(list) > 1 {
			duplicateTitles = append(duplicateTitles, title)
			totalOrphanedThreadsCount += (len(list) - 1)
		}
	}

	log.Printf("Inspection Complete. Found %d duplicate title groups containing %d redundant rows.\n", len(duplicateTitles), totalOrphanedThreadsCount)

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
	log.Printf("Found %d orphaned keys inside catalog_index.\n", orphanedIndexKeys)

	// 3. Optional Maintenance Phase
	if !*repair {
		log.Println("==================================================")
		log.Println("► VERDICT: Audited in Dry-Run mode. No writes occurred.")
		log.Println("To apply fixes and clean up, execute with: --repair")
		log.Println("==================================================")
		return
	}

	log.Println("==================================================")
	log.Println("► INITIATING AUTOMATED REPAIR PHASE...")
	log.Println("==================================================")

	err = db.Update(func(tx *bolt.Tx) error {
		tb := tx.Bucket([]byte("threads"))
		idxB := tx.Bucket([]byte("catalog_index"))
		threadIdxB := tx.Bucket([]byte("tmdb_thread_index"))
		streamsB := tx.Bucket([]byte("streams"))

		// Prune Duplicate Threads (Retain only the most recently updated row)
		for _, title := range duplicateTitles {
			list := duplicatesMap[title]
			// Sort so index 0 is the newest (UpdatedAt DESC)
			sort.Slice(list, func(i, j int) bool {
				return list[i].UpdatedAt.After(list[j].UpdatedAt)
			})

			log.Printf("Pruning duplicates for: %q\n", title)
			log.Printf("  [KEEPING] Hash=%s (Last Updated: %v)\n", list[0].ThreadHash, list[0].UpdatedAt)

			for i := 1; i < len(list); i++ {
				trashThread := list[i]
				log.Printf("  [DELETING] Hash=%s (Last Updated: %v)\n", trashThread.ThreadHash, trashThread.UpdatedAt)

				// Prune the old thread
				_ = tb.Delete([]byte(trashThread.ThreadHash))

				// Prune related streams and lookups
				if trashThread.TmdbID != nil {
					_ = streamsB.Delete([]byte(*trashThread.TmdbID))
					_ = threadIdxB.Delete([]byte(*trashThread.TmdbID))
				}

				// Prune corresponding old catalog index key
				cursor := idxB.Cursor()
				for k, _ := cursor.First(); k != nil; k, _ = cursor.Next() {
					if strings.HasSuffix(string(k), ":"+trashThread.ThreadHash) {
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
		log.Fatalf("Repair process failed: %v\n", err)
	}

	log.Println("Database repair transaction committed successfully!")

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
	log.Println("► VERDICT: [SUCCESS] - Database defragmented and compacted.")
	log.Println("==================================================")
}
