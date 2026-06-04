package database

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"time"
)

// ── Custom JSON Scanner/Valuers for SQLite Compatibility ──

type JSONStringArray []string

func (j *JSONStringArray) Scan(value interface{}) error {
	bytes, ok := value.([]byte)
	if !ok {
		str, ok := value.(string)
		if !ok {
			return errors.New("failed to unmarshal JSONStringArray: unexpected type")
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
			return errors.New("failed to unmarshal JSONFileList: unexpected type")
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

// ── Database GORM Models ──

type Thread struct {
	ID                uint            `gorm:"primaryKey;autoIncrement" json:"id"`
	ThreadHash        string          `gorm:"uniqueIndex;not null" json:"thread_hash"`
	RawTitle          string          `gorm:"not null" json:"raw_title"`
	CleanTitle        string          `gorm:"index" json:"clean_title"`
	Year              *int            `gorm:"index" json:"year"`
	TmdbID            *string         `gorm:"type:text;index" json:"tmdb_id"`
	Status            string          `gorm:"not null;default:'linked';index" json:"status"`
	Type              string          `gorm:"not null;default:'series';index" json:"type"`
	PostedAt          *time.Time      `gorm:"index" json:"posted_at"`
	Catalog           string          `gorm:"index" json:"catalog"`
	MagnetURIs        JSONStringArray `gorm:"type:text" json:"magnet_uris"`
	CustomPoster      *string         `json:"custom_poster"`
	CustomDescription *string         `gorm:"type:text" json:"custom_description"`
	LastSeen          time.Time       `gorm:"autoUpdateTime;index" json:"last_seen"`
	CreatedAt         time.Time       `json:"created_at"`
	UpdatedAt         time.Time       `json:"updated_at"`
	TmdbMetadata      *TmdbMetadata   `gorm:"foreignKey:TmdbID" json:"tmdb_metadata,omitempty"`
}

func (Thread) TableName() string { return "threads" }

type TmdbMetadata struct {
	TmdbID    string    `gorm:"primaryKey;type:text" json:"tmdb_id"`
	ImdbID    *string   `gorm:"uniqueIndex" json:"imdb_id"` // Changed to pointer (*string) to allow safe database NULL inserts instead of conflicting ""
	Year      *int      `gorm:"index" json:"year"`
	Data      string    `gorm:"type:text;not null" json:"data"` // Full JSON metadata payload
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
	Threads   []Thread  `gorm:"foreignKey:TmdbID" json:"threads,omitempty"`
}

func (TmdbMetadata) TableName() string { return "tmdb_metadata" }

type Stream struct {
	ID         uint   `gorm:"primaryKey;autoIncrement" json:"id"`
	TmdbID     string `gorm:"type:text;uniqueIndex:idx_stream_unique;index;not null" json:"tmdb_id"`
	Season     *int   `gorm:"uniqueIndex:idx_stream_unique;index" json:"season"`
	Episode    *int   `gorm:"uniqueIndex:idx_stream_unique;index" json:"episode"`
	EpisodeEnd *int   `json:"episode_end"`
	Infohash   string `gorm:"uniqueIndex:idx_stream_unique;not null" json:"infohash"` // Removed global duplicate uniqueIndex to allow index-range and pack variations
	Quality    string `gorm:"index" json:"quality"`
	Language   string `json:"language"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

func (Stream) TableName() string { return "streams" }

type FailedThread struct {
	ThreadHash  string    `gorm:"primaryKey" json:"thread_hash"`
	RawTitle    string    `gorm:"type:text" json:"raw_title"`
	Reason      string    `gorm:"type:text" json:"reason"`
	LastAttempt time.Time `gorm:"default:CURRENT_TIMESTAMP;index" json:"last_attempt"`
}

func (FailedThread) TableName() string { return "failed_threads" }

type DebridTorrent struct {
	Infohash    string          `gorm:"primaryKey" json:"infohash"`
	TorrentID   string          `gorm:"uniqueIndex;not null" json:"torrent_id"`
	Provider    string          `gorm:"not null;default:'realdebrid';index" json:"provider"`
	Status      string          `gorm:"not null" json:"status"`
	Files       JSONFileList    `gorm:"type:text" json:"files"`
	Links       JSONStringArray `gorm:"type:text" json:"links"`
	LastChecked time.Time       `gorm:"default:CURRENT_TIMESTAMP" json:"last_checked"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
}

func (DebridTorrent) TableName() string { return "debrid_torrents" }

type DebridCacheLock struct {
	Infohash  string    `gorm:"primaryKey" json:"infohash"`
	CreatedAt time.Time `gorm:"default:CURRENT_TIMESTAMP" json:"created_at"`
}

func (DebridCacheLock) TableName() string { return "debrid_cache_locks" }

type MagnetCache struct {
	Infohash  string    `gorm:"primaryKey" json:"infohash"`
	Magnet    string    `gorm:"type:text;not null" json:"magnet"`
	CreatedAt time.Time `gorm:"default:CURRENT_TIMESTAMP" json:"created_at"`
}

func (MagnetCache) TableName() string { return "magnet_cache" }

type TorboxIdMap struct {
	TorrentID int    `gorm:"primaryKey" json:"torrent_id"`
	Hash      string `gorm:"uniqueIndex;not null" json:"hash"`
}

func (TorboxIdMap) TableName() string { return "torbox_id_map" }
