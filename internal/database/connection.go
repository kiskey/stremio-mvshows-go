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

// Init initializes the SQLite database, configures WAL mode and connection pools, and runs AutoMigrate.
// Includes a self-healing schema recovery handler to survive persistent volume corruption and datatype/relationship mismatches.
func Init(dbPath string, level gormlogger.LogLevel) (*gorm.DB, error) {
	// Ensure the parent directory exists
	dir := filepath.Dir(dbPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	var err error
	DB, err = gorm.Open(sqlite.Open(dbPath), &gorm.Config{
		DisableForeignKeyConstraintWhenMigrating: true, // Prevents SQLite from creating invalid/circular physical constraints
		IgnoreRelationshipsWhenMigrating:         true, // Tells GORM to ignore model relationships during schema migration (runtime preloads still work 100%)
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

	// Ensure legacy non-composite unique indexes are cleared
	DB.Exec("DROP INDEX IF EXISTS idx_streams_infohash;")
	DB.Exec("DROP INDEX IF EXISTS idx_streams_infohash_unique;")

	// AutoMigrate all tables
	err = DB.AutoMigrate(
		&Thread{},
		&TmdbMetadata{},
		&Stream{},
		&FailedThread{},
		&DebridTorrent{},
		&DebridCacheLock{},
		&MagnetCache{},
		&TorboxIdMap{},
	)

	if err == nil {
		// Run table integrity check to catch hidden legacy schema datatype/foreign key mismatches
		err = verifyTableIntegrity(DB)
	}
	
	// ── SELF-HEALING SCHEMA RECOVERY ──
	// If AutoMigrate or integrity verification fails, we close, backup, and rebuild a pristine DB.
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
