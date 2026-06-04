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
// Includes a self-healing schema recovery handler to survive persistent volume corruption.
func Init(dbPath string, level gormlogger.LogLevel) (*gorm.DB, error) {
	// Ensure the parent directory exists
	dir := filepath.Dir(dbPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	var err error
	DB, err = gorm.Open(sqlite.Open(dbPath), &gorm.Config{
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
	DB.Exec("PRAGMA foreign_keys=ON;")
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
	
	// ── SELF-HEALING SCHEMA RECOVERY ──
	// If AutoMigrate fails, it is almost certainly due to legacy schema mismatches or foreign key constraints
	// created in previous versions. We close the connection, backup the file, and rebuild a pristine DB.
	if err != nil {
		log.Printf("WARNING: Database migration failed (likely due to legacy SQLite schema corruption): %v", err)
		log.Println("Attempting database schema recovery: backing up old database and starting fresh.")
		
		// Close GORM connection safely
		if sqlDB, errDb := DB.DB(); errDb == nil {
			_ = sqlDB.Close()
		}

		// Backup the corrupted database file with a timestamp suffix
		backupPath := dbPath + ".bak_" + strconv.FormatInt(time.Now().Unix(), 10)
		_ = os.Rename(dbPath, backupPath)

		// Re-initialize a brand new, clean database file
		return Init(dbPath, level)
	}

	// Ensure the unique composite index exists for Stream
	DB.Exec(`CREATE UNIQUE INDEX IF NOT EXISTS idx_stream_unique ON streams(tmdb_id, season, episode, infohash)`)

	return DB, nil
}
