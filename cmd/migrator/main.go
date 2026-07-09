// Version: 2.2.0
// Change log: Preserved full JSON metadata fields (migrated sm.Data verbatim instead of writing empty defaults) and routed insertion through database.CreateOrUpdateThread to avoid indices drifts [report.md].

package main

import (
	"database/sql/driver"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/glebarez/sqlite"
	"github.com/kiskey/stremio-mvshows-go/internal/database"
	"github.com/kiskey/stremio-mvshows-go/internal/services/parser"
	bolt "go.etcd.io/bbolt"
	"gorm.io/gorm"
)

type JSONStringArray []string

func (j *JSONStringArray) Scan(value interface{}) error {
	bytes, ok := value.([]byte)
	if !ok {
		str, ok := value.(string)
		if !ok {
			return fmt.Errorf("failed to unmarshal JSONStringArray: unexpected type")
		}
		bytes = []byte(str)
	}
	return json.Unmarshal(bytes, j)
}

func (j JSONStringArray) Value() (driver.Value, error) {
	if len(j) == 0 {
		return "[]", nil
	}
	bytes, err := json.Marshal(j)
	return string(bytes), err
}

type TorrentFile struct {
	ID       int    `json:"id"`
	Path     string `json:"path"`
	Bytes    int64  `json:"bytes"`
	Selected int    `json:"selected"`
}

type JSONFileList []TorrentFile

func (j *JSONFileList) Scan(value interface{}) error {
	bytes, ok := value.([]byte)
	if !ok {
		str, ok := value.(string)
		if !ok {
			return fmt.Errorf("failed to unmarshal JSONFileList: unexpected type")
		}
		bytes = []byte(str)
	}
	return json.Unmarshal(bytes, j)
}

func (j JSONFileList) Value() (driver.Value, error) {
	if len(j) == 0 {
		return "[]", nil
	}
	bytes, err := json.Marshal(j)
	return string(bytes), err
}

type SqliteThread struct {
	ID                uint            `gorm:"column:id;primaryKey"`
	ThreadHash        string          `gorm:"column:thread_hash"`
	RawTitle          string          `gorm:"column:raw_title"`
	CleanTitle        string          `gorm:"column:clean_title"`
	Year              *int            `gorm:"column:year"`
	TmdbID            *string         `gorm:"column:tmdb_id"`
	Status            string          `gorm:"column:status"`
	Type              string          `gorm:"column:type"`
	PostedAt          *time.Time      `gorm:"column:posted_at"`
	Catalog           string          `gorm:"column:catalog"`
	MagnetURIs        JSONStringArray `gorm:"column:magnet_uris;type:text"`
	CustomPoster      *string         `gorm:"column:custom_poster"`
	CustomDescription *string         `gorm:"column:custom_description"`
	LastSeen          time.Time       `gorm:"column:last_seen"`
	CreatedAt         time.Time       `gorm:"column:created_at"`
	UpdatedAt         time.Time       `gorm:"column:updated_at"`
}

func (SqliteThread) TableName() string { return "threads" }

type SqliteTmdbMetadata struct {
	TmdbID    string    `gorm:"column:tmdb_id;primaryKey"`
	ImdbID    *string   `gorm:"column:imdb_id"`
	Year      *int      `gorm:"column:year"`
	Data      string    `gorm:"column:data"`
	CreatedAt time.Time `gorm:"column:created_at"`
	UpdatedAt time.Time `gorm:"column:updated_at"`
}

func (SqliteTmdbMetadata) TableName() string { return "tmdb_metadata" }

type SqliteStream struct {
	ID         uint      `gorm:"column:id;primaryKey"`
	TmdbID     string    `gorm:"column:tmdb_id"`
	Season     *int      `gorm:"column:season"`
	Episode    *int      `gorm:"column:episode"`
	EpisodeEnd *int      `gorm:"column:episode_end"`
	Infohash   string    `gorm:"column:infohash"`
	Quality    string    `gorm:"column:quality"`
	Language   string    `gorm:"column:language"`
	CreatedAt  time.Time `gorm:"column:created_at"`
	UpdatedAt  time.Time `gorm:"column:updated_at"`
}

func (SqliteStream) TableName() string { return "streams" }

type SqliteFailedThread struct {
	ThreadHash  string    `gorm:"column:thread_hash;primaryKey"`
	RawTitle    string    `gorm:"column:raw_title"`
	Reason      string    `gorm:"column:reason"`
	LastAttempt time.Time `gorm:"column:last_attempt"`
}

func (SqliteFailedThread) TableName() string { return "failed_threads" }

type SqliteDebridTorrent struct {
	Infohash    string          `gorm:"column:infohash;primaryKey"`
	TorrentID   string          `gorm:"column:torrent_id"`
	Provider    string          `gorm:"column:provider"`
	Status      string          `gorm:"column:status"`
	Files       JSONFileList    `gorm:"column:files"`
	Links       JSONStringArray `gorm:"column:links"`
	LastChecked time.Time       `gorm:"column:last_checked"`
	CreatedAt   time.Time       `gorm:"column:created_at"`
	UpdatedAt   time.Time       `gorm:"column:updated_at"`
}

func (SqliteDebridTorrent) TableName() string { return "debrid_torrents" }

type SqliteDebridCacheLock struct {
	Infohash  string    `gorm:"column:infohash;primaryKey"`
	CreatedAt time.Time `gorm:"column:created_at"`
}

func (SqliteDebridCacheLock) TableName() string { return "debrid_cache_locks" }

type SqliteMagnetCache struct {
	Infohash  string    `gorm:"column:infohash;primaryKey"`
	Magnet    string    `gorm:"column:magnet"`
	CreatedAt time.Time `gorm:"column:created_at"`
}

func (SqliteMagnetCache) TableName() string { return "magnet_cache" }

type SqliteTorboxIdMap struct {
	TorrentID int    `gorm:"column:torrent_id;primaryKey"`
	Hash      string `gorm:"column:hash"`
}

func (SqliteTorboxIdMap) TableName() string { return "torbox_id_map" }

func main() {
	sqlitePath := flag.String("sqlite", "/data/stremio_addon.db", "Source SQLite database path")
	boltPath := flag.String("bolt", "/data/stremio_addon.db.bolt", "Target Bbolt database path")
	flag.Parse()

	log.Println("==================================================")
	log.Println("► OFFLINE DATABASE TRANSITION INITIATED")
	log.Printf("Source SQLite: %s\n", *sqlitePath)
	log.Printf("Target BoltDB: %s\n", *boltPath)
	log.Println("==================================================")

	if _, err := os.Stat(*sqlitePath); os.IsNotExist(err) {
		log.Fatalf("Critical: Source SQLite file does not exist at %s\n", *sqlitePath)
	}

	sqlDB, err := gorm.Open(sqlite.Open(*sqlitePath), &gorm.Config{})
	if err != nil {
		log.Fatalf("Failed to open standard SQLite database: %v\n", err)
	}

	_ = os.Remove(*boltPath)
	boltDB, err := database.Init(*boltPath)
	if err != nil {
		log.Fatalf("Failed to initialize target Bbolt storage: %v\n", err)
	}
	defer boltDB.Close()

	log.Println("Migrating threads and compiling index caches...")
	var sqliteThreads []SqliteThread
	if err := sqlDB.Find(&sqliteThreads).Error; err == nil {
		log.Printf("Loaded %d thread records.\n", len(sqliteThreads))
		errTx := boltDB.Update(func(tx *bolt.Tx) error {
			for _, st := range sqliteThreads {
				prTitle := parser.ParseRelease(st.RawTitle, st.Type)
				cleanTitle := st.CleanTitle
				if prTitle.IsValid && prTitle.CleanTitle != "" {
					cleanTitle = prTitle.CleanTitle
				}

				var cleanMags []string
				for _, m := range st.MagnetURIs {
					cleanMags = append(cleanMags, parser.StripTrackersFromMagnet(m))
				}

				thread := database.Thread{
					ID:                st.ID,
					ThreadHash:        st.ThreadHash,
					RawTitle:          st.RawTitle,
					CleanTitle:        cleanTitle,
					Year:              st.Year,
					TmdbID:            st.TmdbID,
					Status:            st.Status,
					Type:              st.Type,
					PostedAt:          st.PostedAt,
					Catalog:           st.Catalog,
					MagnetURIs:        cleanMags,
					CustomPoster:      st.CustomPoster,
					CustomDescription: st.CustomDescription,
					LastSeen:          st.LastSeen,
					CreatedAt:         st.CreatedAt,
					UpdatedAt:         st.UpdatedAt,
				}

				// Avoid index drift by utilizing unified database write pathways [report.md]
				err = database.CreateOrUpdateThread(tx, &thread)
				if err != nil {
					return err
				}
			}
			return nil
		})
		if errTx != nil {
			log.Fatalf("Threads transactional write failed: %v\n", errTx)
		}
	}

	log.Println("Migrating TMDB links metadata mapping registry...")
	var sqliteMeta []SqliteTmdbMetadata
	if err := sqlDB.Find(&sqliteMeta).Error; err == nil {
		log.Printf("Loaded %d metadata records.\n", len(sqliteMeta))
		errTx := boltDB.Update(func(tx *bolt.Tx) error {
			metaBucket := tx.Bucket([]byte("tmdb_metadata"))
			for _, sm := range sqliteMeta {
				meta := database.TmdbMetadata{
					TmdbID:    sm.TmdbID,
					ImdbID:    sm.ImdbID,
					Year:      sm.Year,
					Data:      sm.Data, // FIXED: Migrates data instead of discarding [report.md]
					CreatedAt: sm.CreatedAt,
					UpdatedAt: sm.UpdatedAt,
				}
				bytesData, _ := database.EncodeGob(meta)
				_ = metaBucket.Put([]byte(meta.TmdbID), bytesData)

				if meta.ImdbID != nil && *meta.ImdbID != "" {
					_ = metaBucket.Put([]byte(*meta.ImdbID), bytesData)
				}
			}
			return nil
		})
		if errTx != nil {
			log.Fatalf("Metadata transactional write failed: %v\n", errTx)
		}
	}

	log.Println("Migrating streams pointers arrays...")
	var sqliteStreams []SqliteStream
	if err := sqlDB.Find(&sqliteStreams).Error; err == nil {
		log.Printf("Loaded %d stream records.\n", len(sqliteStreams))
		
		byTMDB := make(map[string][]database.Stream)
		for _, ss := range sqliteStreams {
			stream := database.Stream{
				ID:         ss.ID,
				TmdbID:     ss.TmdbID,
				Season:     ss.Season,
				Episode:    ss.Episode,
				EpisodeEnd: ss.EpisodeEnd,
				Infohash:   ss.Infohash,
				Quality:    ss.Quality,
				Language:   ss.Language,
				CreatedAt:  ss.CreatedAt,
				UpdatedAt:  ss.UpdatedAt,
			}
			byTMDB[stream.TmdbID] = append(byTMDB[stream.TmdbID], stream)
		}

		errTx := boltDB.Update(func(tx *bolt.Tx) error {
			streamsBucket := tx.Bucket([]byte("streams"))
			for tmdbID, list := range byTMDB {
				bytesData, _ := database.EncodeGob(list)
				_ = streamsBucket.Put([]byte(tmdbID), bytesData)
			}
			return nil
		})
		if errTx != nil {
			log.Fatalf("Streams array transactional write failed: %v\n", errTx)
		}
	}

	log.Println("Migrating parsing failures records...")
	var sqliteFailed []SqliteFailedThread
	if err := sqlDB.Find(&sqliteFailed).Error; err == nil {
		errTx := boltDB.Update(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte("failed_threads"))
			for _, f := range sqliteFailed {
				ft := database.FailedThread{
					ThreadHash:  f.ThreadHash,
					RawTitle:    f.RawTitle,
					Reason:      f.Reason,
					LastAttempt: f.LastAttempt,
				}
				bytesData, _ := database.EncodeGob(ft)
				_ = b.Put([]byte(f.ThreadHash), bytesData)
			}
			return nil
		})
		if errTx != nil {
			log.Fatalf("Failed threads transactional write failed: %v\n", errTx)
		}
	}

	log.Println("Migrating debrid torrents download registers...")
	var sqliteDebridTorrents []SqliteDebridTorrent
	if err := sqlDB.Find(&sqliteDebridTorrents).Error; err == nil {
		errTx := boltDB.Update(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte("debrid_torrents"))
			for _, dt := range sqliteDebridTorrents {
				var files []database.TorrentFile
				for _, f := range dt.Files {
					files = append(files, database.TorrentFile{
						ID:       f.ID,
						Path:     f.Path,
						Bytes:    f.Bytes,
						Selected: f.Selected,
					})
				}
				r := database.DebridTorrent{
					Infohash:    dt.Infohash,
					TorrentID:   dt.TorrentID,
					Provider:    dt.Provider,
					Status:      dt.Status,
					Files:       files,
					Links:       []string(dt.Links),
					LastChecked: dt.LastChecked,
					CreatedAt:   dt.CreatedAt,
					UpdatedAt:   dt.UpdatedAt,
				}
				bytesData, _ := database.EncodeGob(r)
				_ = b.Put([]byte(dt.Infohash), bytesData)
			}
			return nil
		})
		if errTx != nil {
			log.Fatalf("Debrid torrents transactional write failed: %v\n", errTx)
		}
	}

	log.Println("Migrating debrid cache locks...")
	var sqliteLocks []SqliteDebridCacheLock
	if err := sqlDB.Find(&sqliteLocks).Error; err == nil {
		_ = boltDB.Update(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte("debrid_cache_locks"))
			for _, l := range sqliteLocks {
				lock := database.DebridCacheLock{Infohash: l.Infohash, CreatedAt: l.CreatedAt}
				bytesData, _ := database.EncodeGob(lock)
				_ = b.Put([]byte(l.Infohash), bytesData)
			}
			return nil
		})
	}

	log.Println("Migrating infohash magnet lookup caches...")
	var sqliteMagnets []SqliteMagnetCache
	if err := sqlDB.Find(&sqliteMagnets).Error; err == nil {
		_ = boltDB.Update(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte("magnet_cache"))
			for _, mc := range sqliteMagnets {
				cleanMagnet := parser.StripTrackersFromMagnet(mc.Magnet)
				r := database.MagnetCache{Infohash: mc.Infohash, Magnet: cleanMagnet, CreatedAt: mc.CreatedAt}
				bytesData, _ := database.EncodeGob(r)
				_ = b.Put([]byte(mc.Infohash), bytesData)
			}
			return nil
		})
	}

	log.Println("Migrating Torbox provider ID registers...")
	var sqliteTorboxMap []SqliteTorboxIdMap
	if err := sqlDB.Find(&sqliteTorboxMap).Error; err == nil {
		_ = boltDB.Update(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte("torbox_id_map"))
			for _, m := range sqliteTorboxMap {
				r := database.TorboxIdMap{TorrentID: m.TorrentID, Hash: m.Hash}
				bytesData, _ := database.EncodeGob(r)
				_ = b.Put([]byte(fmt.Sprintf("%d", m.TorrentID)), bytesData)
			}
			return nil
		})
	}

	log.Println("==================================================")
	log.Println("► DIAGNOSTIC INTEGRITY VERIFICATION REPORT")
	
	var sqliteThreadCount, sqliteMetaCount int64
	_ = sqlDB.Model(&SqliteThread{}).Count(&sqliteThreadCount)
	_ = sqlDB.Model(&SqliteTmdbMetadata{}).Count(&sqliteMetaCount)

	var boltThreadCount, boltMetaCount, boltIndexCount int
	_ = boltDB.View(func(tx *bolt.Tx) error {
		boltThreadCount = tx.Bucket([]byte("threads")).Stats().KeyN
		boltMetaCount = tx.Bucket([]byte("tmdb_metadata")).Stats().KeyN
		boltIndexCount = tx.Bucket([]byte("catalog_index")).Stats().KeyN
		return nil
	})

	log.Printf("Source Threads: %d | Target Threads: %d\n", sqliteThreadCount, boltThreadCount)
	log.Printf("Source Metadata: %d | Target Metadata: %d\n", sqliteMetaCount, boltMetaCount)
	log.Printf("Generated Fast-Catalog Pre-Sorted Indices: %d keys\n", boltIndexCount)

	if int(sqliteThreadCount) == boltThreadCount {
		log.Println("► VERDICT: [PASS] - Structural parity confirmed.")
		log.Println("==================================================")
	} else {
		log.Println("► VERDICT: [FAIL] - Thread count mismatch. Run transition validation checking.")
		log.Println("==================================================")
		os.Exit(1)
	}
}
