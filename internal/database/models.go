// Version: 2.2.0
// Change log: Added MagnetSetHash field to Thread struct to support O(1) content fingerprinting and zero-write bypass logic.

package database

import (
    "encoding/gob"
    "time"
)

// GOB Registration to ensure type safety inside Bbolt nested storage
func init() {
    gob.Register(Thread{})
    gob.Register(TmdbMetadata{})
    gob.Register(Stream{})
    gob.Register(FailedThread{})
    gob.Register(DebridTorrent{})
    gob.Register(DebridCacheLock{})
    gob.Register(MagnetCache{})
    gob.Register(TorboxIdMap{})
    gob.Register(MonitoredSeries{}) // Registered MonitoredSeries struct
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
    MagnetSetHash     string         `json:"magnet_set_hash"` // O(1) content fingerprint for bypass logic
    CustomPoster      *string        `json:"custom_poster"`
    CustomDescription *string        `json:"custom_description"`
    LastSeen          time.Time      `json:"last_seen"`
    CreatedAt         time.Time      `json:"created_at"`
    UpdatedAt         time.Time      `json:"updated_at"`
    URL               string         `json:"url,omitempty"`  // Retains direct thread address
    TmdbMetadata      *TmdbMetadata  `json:"-"`
}

type MonitoredSeries struct {
    ThreadHash   string    `json:"thread_hash"`
    URL          string    `json:"url"`
    Title        string    `json:"title"`
    RawTitle     string    `json:"raw_title"`
    Status       string    `json:"status"` // "active", "paused", "archived"
    EpisodeCount int       `json:"episode_count"`
    LastChecked  time.Time `json:"last_checked"`
    LastUpdated  time.Time `json:"last_updated"`
    CreatedAt    time.Time `json:"created_at"`
}

type TmdbMetadata struct {
    TmdbID    string    `json:"tmdb_id"`
    ImdbID    *string   `json:"imdb_id"`
    Year      *int      `json:"year"`
    Data      string    `json:"data"` // Holds the verbatim JSON response from TMDB / Cinemeta
    CreatedAt time.Time `json:"created_at"`
    UpdatedAt time.Time `json:"updated_at"`
}

type Stream struct {
    ID           uint      `json:"id"`
    TmdbID       string    `json:"tmdb_id"`
    Season       *int      `json:"season"`
    Episode      *int      `json:"episode"`
    EpisodeEnd   *int      `json:"episode_end"`
    Infohash     string    `json:"infohash"`
    Quality      string    `json:"quality"`
    Language     string    `json:"language"`
    CreatedAt    time.Time `json:"created_at"`
    UpdatedAt    time.Time `json:"updated_at"`
    SceneName    string    `json:"scene_name,omitempty"`
    EpisodeTitle string    `json:"episode_title,omitempty"`
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
