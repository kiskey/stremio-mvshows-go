package debrid

import (
	"context"
	"errors"
)

var ErrResourceNotFound = errors.New("resource not found")

type AddResult struct {
	ID     string
	Hash   string
	Name   string
	Cached bool
}

type TorrentInfo struct {
	ID       string
	Filename string
	Status   string
	Files    []FileInfo
	Links    []string
	Bytes    int64
	Seeders  int
}

type FileInfo struct {
	ID       int
	Path     string
	Bytes    int64
	Selected int
}

type UnrestrictResult struct {
	Download string
}

type Torrent struct {
	ID     string
	Hash   string
	Name   string
	Status string
}

type CacheStatus struct {
	Cached    bool
	TorrentID string
	Name      string
	Size      int64
	Files     []CacheFile
}

type CacheFile struct {
	ID   int
	Name string
	Size int64
}

type Provider interface {
	IsEnabled() bool
	AddMagnet(ctx context.Context, magnet string) (*AddResult, error)
	GetTorrentInfo(ctx context.Context, id string) (*TorrentInfo, error)
	SelectFiles(ctx context.Context, id string, fileIDs []string) error
	UnrestrictLink(ctx context.Context, link string) (*UnrestrictResult, error)
	DeleteTorrent(ctx context.Context, id string) error
	GetTorrents(ctx context.Context) ([]Torrent, error)
	CheckCached(ctx context.Context, hashes []string) (map[string]CacheStatus, error)
	AddAndSelect(ctx context.Context, magnet string) (*TorrentInfo, error)
	GetCachedFileInfo(ctx context.Context, hash, fileName string) (*FileInfo, error)
}
