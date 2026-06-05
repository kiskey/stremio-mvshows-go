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

// ── Database GORM Models with Explicit Column Binding ──

type Thread struct {
	ID                uint            `gorm:"column:id;primaryKey;autoIncrement" json:"id"`
	ThreadHash        string          `gorm:"column:thread_hash;uniqueIndex;not null" json:"thread_hash"`
	RawTitle          string          `gorm:"column:raw_title;not null" json:"raw_title"`
	CleanTitle        string          `gorm:"column:clean_title;index" json:"clean_title"`
	Year              *int            `gorm:"column:year;index" json:"year"`
	TmdbID            *string         `gorm:"column:tmdb_id;type:text;index" json:"tmdb_id"`
	Status            string          `gorm:"column:status;not null;default:'linked';index" json:"status"`
	Type              string          `gorm:"column:type;not null;default:'series';index" json:"type"`
	PostedAt          *time.Time      `gorm:"column:posted_at;index" json:"posted_at"`
	Catalog           string          `gorm:"column:catalog;index" json:"catalog"`
	MagnetURIs        JSONStringArray `gorm:"column:magnet_uris;type:text" json:"magnet_uris"`
	CustomPoster      *string         `gorm:"column:custom_poster" json:"custom_poster"`
	CustomDescription *string         `gorm:"column:custom_description;type:text" json:"custom_description"`
	LastSeen          time.Time       `gorm:"column:last_seen;autoUpdateTime;index" json:"last_seen"`
	CreatedAt         time.Time       `gorm:"column:created_at" json:"created_at"`
	UpdatedAt         time.Time       `gorm:"column:updated_at" json:"updated_at"`
	TmdbMetadata      *TmdbMetadata   `gorm:"foreignKey:TmdbID" json:"tmdb_metadata,omitempty"`
}

func (Thread) TableName() string { return "threads" }

type TmdbMetadata struct {
	TmdbID    string    `gorm:"column:tmdb_id;primaryKey;type:text" json:"tmdb_id"`
	ImdbID    *string   `gorm:"column:imdb_id;uniqueIndex" json:"imdb_id"` // Allows NULL inserts without unique constraint conflicts
	Year      *int      `gorm:"column:year;index" json:"year"`
	Data      string    `gorm:"column:data;type:text;not null" json:"data"` // Full JSON metadata payload
	CreatedAt time.Time `gorm:"column:created_at" json:"created_at"`
	UpdatedAt time.Time `gorm:"column:updated_at" json:"updated_at"`
	Threads   []Thread  `gorm:"foreignKey:TmdbID" json:"threads,omitempty"`
}

func (TmdbMetadata) TableName() string { return "tmdb_metadata" }

type Stream struct {
	ID         uint      `gorm:"column:id;primaryKey;autoIncrement" json:"id"`
	TmdbID     string    `gorm:"column:tmdb_id;type:text;uniqueIndex:idx_stream_unique;index;not null" json:"tmdb_id"`
	Season     *int      `gorm:"column:season;uniqueIndex:idx_stream_unique;index" json:"season"`
	Episode    *int      `gorm:"column:episode;uniqueIndex:idx_stream_unique;index" json:"episode"`
	EpisodeEnd *int      `gorm:"column:episode_end" json:"episode_end"`
	Infohash   string    `gorm:"column:infohash;uniqueIndex:idx_stream_unique;not null" json:"infohash"`
	Quality    string    `gorm:"column:quality;index" json:"quality"`
	Language   string    `gorm:"column:language" json:"language"`
	CreatedAt  time.Time `gorm:"column:created_at" json:"created_at"`
	UpdatedAt  time.Time `gorm:"column:updated_at" json:"updated_at"`
}

func (Stream) TableName() string { return "streams" }

type FailedThread struct {
	ThreadHash  string    `gorm:"column:thread_hash;primaryKey" json:"thread_hash"`
	RawTitle    string    `gorm:"column:raw_title;type:text" json:"raw_title"`
	Reason      string    `gorm:"column:reason;type:text" json:"reason"`
	LastAttempt time.Time `gorm:"column:last_attempt;default:CURRENT_TIMESTAMP;index" json:"last_attempt"`
}

func (FailedThread) TableName() string { return "failed_threads" }

type DebridTorrent struct {
	Infohash    string          `gorm:"column:infohash;primaryKey" json:"infohash"`
	TorrentID   string          `gorm:"column:torrent_id;uniqueIndex;not null" json:"torrent_id"`
	Provider    string          `gorm:"column:provider;not null;default:'realdebrid';index" json:"provider"`
	Status      string          `gorm:"column:status;not null" json:"status"`
	Files       JSONFileList    `gorm:"column:files;type:text" json:"files"`
	Links       JSONStringArray `gorm:"column:links;type:text" json:"links"`
	LastChecked time.Time       `gorm:"column:last_checked;default:CURRENT_TIMESTAMP" json:"last_checked"`
	CreatedAt   time.Time       `gorm:"column:created_at" json:"created_at"`
	UpdatedAt   time.Time       `gorm:"column:updated_at" json:"updated_at"`
}

func (DebridTorrent) TableName() string { return "debrid_torrents" }

type DebridCacheLock struct {
	Infohash  string    `gorm:"column:infohash;primaryKey" json:"infohash"`
	CreatedAt time.Time `gorm:"column:created_at;default:CURRENT_TIMESTAMP" json:"created_at"`
}

func (DebridCacheLock) TableName() string { return "debrid_cache_locks" }

type MagnetCache struct {
	Infohash  string    `gorm:"column:infohash;primaryKey" json:"infohash"`
	Magnet    string    `gorm:"column:magnet;type:text;not null" json:"magnet"`
	CreatedAt time.Time `gorm:"column:created_at;default:CURRENT_TIMESTAMP" json:"created_at"`
}

func (MagnetCache) TableName() string { return "magnet_cache" }

type TorboxIdMap struct {
	TorrentID int    `gorm:"column:torrent_id;primaryKey" json:"torrent_id"`
	Hash      string `gorm:"column:hash;uniqueIndex;not null" json:"hash"`
}

func (TorboxIdMap) TableName() string { return "torbox_id_map" }
