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
	ID                uint            `gorm:"primaryKey;autoIncrement"`
	ThreadHash        string          `gorm:"uniqueIndex;not null"`
	RawTitle          string          `gorm:"not null"`
	CleanTitle        string          `gorm:"index"`
	Year              *int            `gorm:"index"`
	TmdbID            *string         `gorm:"index"`
	Status            string          `gorm:"not null;default:'linked';index"`
	Type              string          `gorm:"not null;default:'series';index"`
	PostedAt          *time.Time      `gorm:"index"`
	Catalog           string          `gorm:"index"`
	MagnetURIs        JSONStringArray `gorm:"type:text"`
	CustomPoster      *string
	CustomDescription *string         `gorm:"type:text"`
	LastSeen          time.Time       `gorm:"autoUpdateTime;index"`
	CreatedAt         time.Time
	UpdatedAt         time.Time
	TmdbMetadata      *TmdbMetadata   `gorm:"foreignKey:TmdbID;references:TmdbID"`
}

func (Thread) TableName() string { return "threads" }

type TmdbMetadata struct {
	TmdbID    string    `gorm:"primaryKey"`
	ImdbID    string    `gorm:"uniqueIndex"`
	Year      *int      `gorm:"index"`
	Data      string    `gorm:"type:text;not null"` // Full JSON metadata payload
	CreatedAt time.Time
	UpdatedAt time.Time
	Threads   []Thread  `gorm:"foreignKey:TmdbID;references:TmdbID"`
}

func (TmdbMetadata) TableName() string { return "tmdb_metadata" }

type Stream struct {
	ID         uint   `gorm:"primaryKey;autoIncrement"`
	TmdbID     string `gorm:"uniqueIndex:idx_stream_unique;index;not null"`
	Season     *int   `gorm:"uniqueIndex:idx_stream_unique;index"`
	Episode    *int   `gorm:"uniqueIndex:idx_stream_unique;index"`
	EpisodeEnd *int
	Infohash   string `gorm:"uniqueIndex:idx_stream_unique;uniqueIndex;not null"`
	Quality    string `gorm:"index"`
	Language   string
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

func (Stream) TableName() string { return "streams" }

type FailedThread struct {
	ThreadHash  string    `gorm:"primaryKey"`
	RawTitle    string    `gorm:"type:text"`
	Reason      string    `gorm:"type:text"`
	LastAttempt time.Time `gorm:"default:CURRENT_TIMESTAMP;index"`
}

func (FailedThread) TableName() string { return "failed_threads" }

type DebridTorrent struct {
	Infohash    string          `gorm:"primaryKey"`
	TorrentID   string          `gorm:"uniqueIndex;not null"`
	Provider    string          `gorm:"not null;default:'realdebrid';index"`
	Status      string          `gorm:"not null"`
	Files       JSONFileList    `gorm:"type:text"`
	Links       JSONStringArray `gorm:"type:text"`
	LastChecked time.Time       `gorm:"default:CURRENT_TIMESTAMP"`
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

func (DebridTorrent) TableName() string { return "debrid_torrents" }

type DebridCacheLock struct {
	Infohash  string    `gorm:"primaryKey"`
	CreatedAt time.Time `gorm:"default:CURRENT_TIMESTAMP"`
}

func (DebridCacheLock) TableName() string { return "debrid_cache_locks" }

type MagnetCache struct {
	Infohash  string    `gorm:"primaryKey"`
	Magnet    string    `gorm:"type:text;not null"`
	CreatedAt time.Time `gorm:"default:CURRENT_TIMESTAMP"`
}

func (MagnetCache) TableName() string { return "magnet_cache" }

type TorboxIdMap struct {
	TorrentID int    `gorm:"primaryKey"`
	Hash      string `gorm:"uniqueIndex;not null"`
}

func (TorboxIdMap) TableName() string { return "torbox_id_map" }
