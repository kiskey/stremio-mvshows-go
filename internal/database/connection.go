
package database

import (
	"log"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/glebarez/sqlite" // Pure-Go GORM SQLite driver
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
)

var DB *gorm.DB

// Init initializes the SQLite database, configures WAL mode and connection pools, and runs explicit DDL setup.
// Includes a self-healing schema recovery handler to survive persistent volume corruption and datatype/relationship mismatches.
func Init(dbPath string, level gormlogger.LogLevel) (*gorm.DB, error) {
	// Ensure the parent directory exists
	dir := filepath.Dir(dbPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	var err error
	DB, err = gorm.Open(sqlite.Open(dbPath), &gorm.Config{
		DisableForeignKeyConstraintWhenMigrating: true, // Prevents physical database foreign keys
		IgnoreRelationshipsWhenMigrating:         true, // Telling GORM to ignore model relationships during runtime schema generation
		Logger: gormlogger.New(
			log.New(os.Stdout, "\r\n", log.LstdFlags),
			gormlogger.Config{
				SlowThreshold:             200 * time.Millisecond,
				LogLevel:                  level,
				IgnoreRecordNotFoundError: true,
				Colorful:                  true,
			},
		),
	})
	if err != nil {
		return nil, err
	}

	sqlDB, err := DB.DB()
	if err != nil {
		return nil, err
	}

	// Optimize SQLite performance for concurrent read throughput under multi-threaded Go execution.
	sqlDB.SetMaxOpenConns(20) 
	sqlDB.SetMaxIdleConns(5)
	sqlDB.SetConnMaxLifetime(time.Hour)

	// Enable WAL (Write-Ahead Logging) for safe, concurrent read/write transactions
	DB.Exec("PRAGMA journal_mode=WAL;")
	DB.Exec("PRAGMA synchronous=NORMAL;")
	DB.Exec("PRAGMA foreign_keys=OFF;") // Keeps SQLite database execution free of physical key mismatches
	DB.Exec("PRAGMA busy_timeout=5000;") // Wait up to 5s for locks to clear before throwing an error

	// PEAK PERFORMANCE TUNING: Force temporary databases into memory and allocate page cache buffers (~20MB)
	DB.Exec("PRAGMA temp_store=MEMORY;")
	DB.Exec("PRAGMA cache_size=-20000;")
	
	// Configure migration-safe non-blocking incremental auto-vacuum
	DB.Exec("PRAGMA auto_vacuum=INCREMENTAL;")

	// Ensure legacy non-composite unique indexes are cleared
	DB.Exec("DROP INDEX IF EXISTS idx_streams_infohash;")
	DB.Exec("DROP INDEX IF EXISTS idx_streams_infohash_unique;")

	// Execute highly stable, explicit SQL DDL schemas to bypass GORM's schema-parsing bugs completely
	err = createExplicitTables(DB)

	if err == nil {
		// Run table integrity check to catch hidden legacy schema datatype/foreign key mismatches
		err = verifyTableIntegrity(DB)
	}
	
	// ── SELF-HEALING SCHEMA RECOVERY ──
	// If the schema verification fails, we close, backup, and rebuild a pristine DB.
	if err != nil {
		log.Printf("WARNING: Database migration or integrity verification failed: %v", err)
		log.Println("Attempting database schema recovery: backing up old database and starting fresh.")
		
		// Close GORM connection safely to release open handles
		if sqlDB, errDb := DB.DB(); errDb == nil {
			_ = sqlDB.Close()
		}

		// Perform full, WAL-aware database backup and file cleanup
		backupAndRemoveDatabase(dbPath)

		// Re-initialize a brand new, clean database file
		return Init(dbPath, level)
	}

	// Ensure the unique composite index exists for Stream
	DB.Exec(`CREATE UNIQUE INDEX IF NOT EXISTS idx_stream_unique ON streams(tmdb_id, season, episode, infohash)`)

	return DB, nil
}

// verifyTableIntegrity tests writing a text value to tmdb_id in TmdbMetadata to detect datatype/affinity corruption.
func verifyTableIntegrity(db *gorm.DB) error {
	// Clean up any stray dummy record from a previous abrupt crash
	_ = db.Unscoped().Where("tmdb_id = ?", "tv:test_integrity_dummy").Delete(&TmdbMetadata{}).Error

	dummy := TmdbMetadata{
		TmdbID: "tv:test_integrity_dummy",
		Data:   "{}",
	}
	err := db.Create(&dummy).Error
	if err != nil {
		return err
	}
	// Clean up the dummy record
	_ = db.Unscoped().Delete(&dummy).Error
	return nil
}

// backupAndRemoveDatabase renames the database file and its SQLite WAL/SHM sidecars to prevent dirty schema recoveries.
func backupAndRemoveDatabase(dbPath string) {
	timestamp := strconv.FormatInt(time.Now().Unix(), 10)

	// 1. Backup the main SQLite database file
	_ = os.Rename(dbPath, dbPath+".bak_"+timestamp)

	// 2. Backup the active Write-Ahead Log (WAL) sidecar if present
	walPath := dbPath + "-wal"
	if _, err := os.Stat(walPath); err == nil {
		_ = os.Rename(walPath, walPath+".bak_"+timestamp)
	}

	// 3. Backup the Shared Memory (SHM) sidecar index if present
	shmPath := dbPath + "-shm"
	if _, err := os.Stat(shmPath); err == nil {
		_ = os.Rename(shmPath, shmPath+".bak_"+timestamp)
	}
}

// createExplicitTables constructs tables and indices explicitly using raw SQLite DDL statements.
func createExplicitTables(db *gorm.DB) error {
	tx := db.Begin()
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
		}
	}()

	ddls := []string{
		`CREATE TABLE IF NOT EXISTS tmdb_metadata (
			tmdb_id TEXT PRIMARY KEY,
			imdb_id TEXT UNIQUE,
			year INTEGER,
			data TEXT NOT NULL,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
		);`,
		`CREATE INDEX IF NOT EXISTS idx_tmdb_metadata_year ON tmdb_metadata(year);`,

		`CREATE TABLE IF NOT EXISTS threads (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			thread_hash TEXT UNIQUE NOT NULL,
			raw_title TEXT NOT NULL,
			clean_title TEXT,
			year INTEGER,
			tmdb_id TEXT,
			status TEXT NOT NULL DEFAULT 'linked',
			type TEXT NOT NULL DEFAULT 'series',
			posted_at DATETIME,
			catalog TEXT,
			magnet_uris TEXT,
			custom_poster TEXT,
			custom_description TEXT,
			last_seen DATETIME DEFAULT CURRENT_TIMESTAMP,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
		);`,
		`CREATE INDEX IF NOT EXISTS idx_threads_clean_title ON threads(clean_title);`,
		`CREATE INDEX IF NOT EXISTS idx_threads_year ON threads(year);`,
		`CREATE INDEX IF NOT EXISTS idx_threads_tmdb_id ON threads(tmdb_id);`,
		`CREATE INDEX IF NOT EXISTS idx_threads_status ON threads(status);`,
		`CREATE INDEX IF NOT EXISTS idx_threads_type ON threads(type);`,
		`CREATE INDEX IF NOT EXISTS idx_threads_posted_at ON threads(posted_at);`,
		`CREATE INDEX IF NOT EXISTS idx_threads_catalog ON threads(catalog);`,
		`CREATE INDEX IF NOT EXISTS idx_threads_last_seen ON threads(last_seen);`,
		`CREATE INDEX IF NOT EXISTS idx_threads_catalog_status_type_posted ON threads(catalog, status, type, posted_at DESC);`,

		`CREATE TABLE IF NOT EXISTS streams (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			tmdb_id TEXT NOT NULL,
			season INTEGER,
			episode INTEGER,
			episode_end INTEGER,
			infohash TEXT NOT NULL,
			quality TEXT,
			language TEXT,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
		);`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_stream_unique ON streams(tmdb_id, season, episode, infohash);`,
		`CREATE INDEX IF NOT EXISTS idx_streams_tmdb_id ON streams(tmdb_id);`,
		`CREATE INDEX IF NOT EXISTS idx_streams_season ON streams(season);`,
		`CREATE INDEX IF NOT EXISTS idx_streams_episode ON streams(episode);`,
		`CREATE INDEX IF NOT EXISTS idx_streams_quality ON streams(quality);`,

		`CREATE TABLE IF NOT EXISTS failed_threads (
			thread_hash TEXT PRIMARY KEY,
			raw_title TEXT,
			reason TEXT,
			last_attempt DATETIME DEFAULT CURRENT_TIMESTAMP
		);`,
		`CREATE INDEX IF NOT EXISTS idx_failed_threads_last_attempt ON failed_threads(last_attempt);`,

		`CREATE TABLE IF NOT EXISTS debrid_torrents (
			infohash TEXT PRIMARY KEY,
			torrent_id TEXT UNIQUE NOT NULL,
			provider TEXT NOT NULL DEFAULT 'realdebrid',
			status TEXT NOT NULL,
			files TEXT,
			links TEXT,
			last_checked DATETIME DEFAULT CURRENT_TIMESTAMP,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
		);`,
		`CREATE INDEX IF NOT EXISTS idx_debrid_torrents_provider ON debrid_torrents(provider);`,

		`CREATE TABLE IF NOT EXISTS debrid_cache_locks (
			infohash TEXT PRIMARY KEY,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP
		);`,

		`CREATE TABLE IF NOT EXISTS magnet_cache (
			infohash TEXT PRIMARY KEY,
			magnet TEXT NOT NULL,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP
		);`,

		`CREATE TABLE IF NOT EXISTS torbox_id_map (
			torrent_id INTEGER PRIMARY KEY,
			hash TEXT UNIQUE NOT NULL
		);`,
	}

	for _, ddl := range ddls {
		if err := tx.Exec(ddl).Error; err != nil {
			tx.Rollback()
			return err
		}
	}

	return tx.Commit().Error
}
