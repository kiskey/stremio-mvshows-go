package debrid

import (
	"errors"
	"sync"

	"github.com/sevvian/smvshows-go/internal/config"
	"github.com/sevvian/smvshows-go/internal/database"
	"github.com/sevvian/smvshows-go/internal/utils"
	"gorm.io/gorm"
)

var (
	ErrResourceNotFound = errors.New("resource not found")
	ErrNotSupported     = errors.New("not supported")
)

type AddResult struct {
	ID     string
	Hash   string
	Name   string
	Cached bool
}

type TorrentFile struct {
	ID       int
	Path     string
	Bytes    int64
	Selected int
}

type TorrentInfo struct {
	ID       string
	Filename string
	Status   string
	Files    []TorrentFile
	Links    []string
}

type UnrestrictResult struct {
	Download string
}

type FileInfo struct {
	ID    int
	Path  string
	Bytes int64
}

type CacheInfo struct {
	Cached bool
	Name   string
	Size   int64
	Files  []FileInfo
}

type Provider interface {
	IsEnabled() bool
	AddMagnet(magnet string) (*AddResult, error)
	GetTorrentInfo(id string) (*TorrentInfo, error)
	SelectFiles(id string, fileIds string) error
	UnrestrictLink(link string) (*UnrestrictResult, error)
	AddAndSelect(magnet string) (*TorrentInfo, error)
	CheckCached(hashes []string) (map[string]CacheInfo, error)
	GetCachedFileInfo(hash, fileName string) (*FileInfo, error)
	GetDownloadLinkForFile(torrentID string, fileID int) (string, error)
}

// ── Disabled Provider Stub ──

type disabledProvider struct{}

func (d *disabledProvider) IsEnabled() bool                                         { return false }
func (d *disabledProvider) AddMagnet(string) (*AddResult, error)                    { return nil, errors.New("debrid service disabled") }
func (d *disabledProvider) GetTorrentInfo(string) (*TorrentInfo, error)              { return nil, errors.New("debrid service disabled") }
func (d *disabledProvider) SelectFiles(string, string) error                        { return errors.New("debrid service disabled") }
func (d *disabledProvider) UnrestrictLink(string) (*UnrestrictResult, error)        { return nil, errors.New("debrid service disabled") }
func (d *disabledProvider) AddAndSelect(string) (*TorrentInfo, error)                { return nil, errors.New("debrid service disabled") }
func (d *disabledProvider) CheckCached([]string) (map[string]CacheInfo, error)      { return nil, ErrNotSupported }
func (d *disabledProvider) GetCachedFileInfo(string, string) (*FileInfo, error)      { return nil, errors.New("debrid service disabled") }
func (d *disabledProvider) GetDownloadLinkForFile(string, int) (string, error)      { return "", errors.New("debrid service disabled") }

var (
	providerInstance Provider
	once             sync.Once
)

// GetProvider retrieves or initializes the configured debrid provider instance.
func GetProvider(cfg *config.Config) Provider {
	once.Do(func() {
		switch cfg.DebridService {
		case "realdebrid":
			providerInstance = NewRealDebridProvider(cfg)
		case "torbox":
			providerInstance = NewTorboxProvider(cfg)
		default:
			providerInstance = &disabledProvider{}
		}
	})
	return providerInstance
}

// CheckCached implements the unified cache-checking orchestrator.
// It queries the live provider API if available, else falls back cleanly to the local DB status.
func CheckCached(hashes []string, db *gorm.DB) map[string]bool {
	cfg := config.Load()
	p := GetProvider(cfg)

	normalized := make(map[string]bool)
	for _, h := range hashes {
		normalized[h] = false
	}

	if p.IsEnabled() {
		result, err := p.CheckCached(hashes)
		if err == nil {
			for h, info := range result {
				normalized[h] = info.Cached
			}
			return normalized
		}
		if !errors.Is(err, ErrNotSupported) {
			utils.Logger.Warn().Err(err).Msg("Provider CheckCached check failed, falling back to local database status.")
		}
	}

	// Local database fallback path
	if db != nil && len(hashes) > 0 {
		var records []database.DebridTorrent
		err := db.Where("infohash IN ?", hashes).Find(&records).Error
		if err == nil {
			for _, r := range records {
				normalized[r.Infohash] = (r.Status == "downloaded")
			}
		} else {
			utils.Logger.Error().Err(err).Msg("Database fallback search failed inside unified CheckCached.")
		}
	}

	return normalized
}
