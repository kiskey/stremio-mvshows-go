package database

import (
	"log"
	"os"
	"path/filepath"
	"time"

	"gorm.io/driver/sqlite"
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

	// Optimize SQLite performance and concurrent read/write throughput
	sqlDB.SetMaxOpenConns(1) // SQLite works best with serialized writes
	sqlDB.SetMaxIdleConns(1)
	sqlDB.SetConnMaxLifetime(time.Hour)

	// Enable WAL (Write-Ahead Logging) for safe concurrent reads/writes
	DB.Exec("PRAGMA journal_mode=WAL;")
	DB.Exec("PRAGMA synchronous=NORMAL;")
	DB.Exec("PRAGMA foreign_keys=ON;")
	DB.Exec("PRAGMA busy_timeout=5000;")

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
