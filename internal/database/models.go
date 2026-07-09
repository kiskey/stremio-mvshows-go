
// Version: 2.0.0
// Change log: Converted database column maps into gob-compatible model structures. Added explicit Gob initialization block for strict type-safe streams deserialization.

package database

import (
	"encoding/gob"
	"time"
)

// GOB Registration to ensure type safety inside Bbolt nested stream slices
func init() {
	gob.Register(Thread{})
	gob.Register(TmdbMetadata{})
	gob.Register(Stream{})
	gob.Register(FailedThread{})
	gob.Register(DebridTorrent{})
	gob.Register(DebridCacheLock{})
	gob.Register(MagnetCache{})
	gob.Register(TorboxIdMap{})
}

type Thread struct {
	ID                uint           `json:"id"`
	ThreadHash        string         `json:"thread_hash"`
	RawTitle          string         `json:"raw_title"`
	CleanTitle        string         `json:"clean_title"`
	Year              *int           `json:"year"`
	TmdbID            *string        `json:"tmdb_id"`
	Status            string         `json:"status"`
	Type              string         `json:"type"`
	PostedAt          *time.Time     `json:"posted_at"`
	Catalog           string         `json:"catalog"`
	MagnetURIs        []string       `json:"magnet_uris"`
	CustomPoster      *string        `json:"custom_poster"`
	CustomDescription *string        `json:"custom_description"`
	LastSeen          time.Time      `json:"last_seen"`
	CreatedAt         time.Time      `json:"created_at"`
	UpdatedAt         time.Time      `json:"updated_at"`
	TmdbMetadata      *TmdbMetadata  `json:"tmdb_metadata,omitempty"`
}

type TmdbMetadata struct {
	TmdbID    string    `json:"tmdb_id"`
	ImdbID    *string   `json:"imdb_id"`
	Year      *int      `json:"year"`
	Data      string    `json:"data"` // Always empty JSON "{}"
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
	Threads   []Thread  `json:"threads,omitempty"`
}

type Stream struct {
	ID         uint      `json:"id"`
	TmdbID     string    `json:"tmdb_id"`
	Season     *int      `json:"season"`
	Episode    *int      `json:"episode"`
	EpisodeEnd *int      `json:"episode_end"`
	Infohash   string    `json:"infohash"`
	Quality    string    `json:"quality"`
	Language   string    `json:"language"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

type FailedThread struct {
	ThreadHash  string    `json:"thread_hash"`
	RawTitle    string    `json:"raw_title"`
	Reason      string    `json:"reason"`
	LastAttempt time.Time `json:"last_attempt"`
}

type TorrentFile struct {
	ID       int    `json:"id"`
	Path     string `json:"path"`
	Bytes    int64  `json:"bytes"`
	Selected int    `json:"selected"`
}

type DebridTorrent struct {
	Infohash    string        `json:"infohash"`
	TorrentID   string        `json:"torrent_id"`
	Provider    string        `json:"provider"`
	Status      string        `json:"status"`
	Files       []TorrentFile `json:"files"`
	Links       []string      `json:"links"`
	LastChecked time.Time     `json:"last_checked"`
	CreatedAt   time.Time     `json:"created_at"`
	UpdatedAt   time.Time     `json:"updated_at"`
}

type DebridCacheLock struct {
	Infohash  string    `json:"infohash"`
	CreatedAt time.Time `json:"created_at"`
}

type MagnetCache struct {
	Infohash  string    `json:"infohash"`
	Magnet    string    `json:"magnet"`
	CreatedAt time.Time `json:"created_at"`
}

type TorboxIdMap struct {
	TorrentID int    `json:"torrent_id"`
	Hash      string `json:"hash"`
}
