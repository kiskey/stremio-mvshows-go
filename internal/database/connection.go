package database

import (
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/glebarez/sqlite" // Pure-Go GORM SQLite driver
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
)

var DB *gorm.DB

// Init initializes the SQLite database, configures WAL mode and connection pools, and runs AutoMigrate.
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
	// Raising MaxOpenConns from 1 to 20 unblocks concurrent reads, while SQLite's WAL mode and busy_timeout
	// safely handle queueing any concurrent write transactions.
	sqlDB.SetMaxOpenConns(20) 
	sqlDB.SetMaxIdleConns(5)
	sqlDB.SetConnMaxLifetime(time.Hour)

	// Enable WAL (Write-Ahead Logging) for safe, concurrent read/write transactions
	DB.Exec("PRAGMA journal_mode=WAL;")
	DB.Exec("PRAGMA synchronous=NORMAL;")
	DB.Exec("PRAGMA foreign_keys=ON;")
	DB.Exec("PRAGMA busy_timeout=5000;") // Wait up to 5s for locks to clear before throwing an error

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
	if err != nil {
		return nil, err
	}

	// Ensure the unique composite index exists for Stream
	DB.Exec(`CREATE UNIQUE INDEX IF NOT EXISTS idx_stream_unique ON streams(tmdb_id, season, episode, infohash)`)

	return DB, nil
}
