package debrid

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/kiskey/stremio-mvshows-go/internal/config"
	"github.com/kiskey/stremio-mvshows-go/internal/database"
	"github.com/kiskey/stremio-mvshows-go/internal/utils"
	"gorm.io/gorm"
)

var (
	providerInstance Provider
	once             sync.Once
	TorrentInfoCache = NewMemoryCache(12 * time.Hour)
	URLCache         = NewMemoryCache(1 * time.Hour)
)

// GetProvider retrieves or initializes the configured debrid provider instance.
func GetProvider(cfg *config.Config) Provider {
	once.Do(func() {
		switch cfg.DebridService {
		case "realdebrid":
			providerInstance = NewRealDebrid()
		case "torbox":
			providerInstance = NewTorbox(nil)
		default:
			providerInstance = &disabledProvider{}
		}
	})
	return providerInstance
}

// LoadProvider acts as a parameter-less fallback to GetProvider, loading config on-demand.
func LoadProvider() Provider {
	return GetProvider(config.Load())
}

// ── In-Memory TTL Cache Implementation ──

type cacheEntry struct {
	value   map[string]interface{}
	expires time.Time
}

type MemoryCache struct {
	store map[string]cacheEntry
	mu    sync.RWMutex
	ttl   time.Duration
}

func NewMemoryCache(ttl time.Duration) *MemoryCache {
	return &MemoryCache{
		store: make(map[string]cacheEntry),
		ttl:   ttl,
	}
}

func (m *MemoryCache) Get(_ context.Context, key string) (map[string]interface{}, error) {
	m.mu.RLock()
	entry, ok := m.store[key]
	m.mu.RUnlock()
	if !ok || time.Now().After(entry.expires) {
		if ok {
			m.mu.Lock()
			delete(m.store, key)
			m.mu.Unlock()
		}
		return nil, nil
	}
	return entry.value, nil
}

func (m *MemoryCache) Set(_ context.Context, key string, value map[string]interface{}) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.store[key] = cacheEntry{value: value, expires: time.Now().Add(m.ttl)}
	return nil
}

func (m *MemoryCache) Update(_ context.Context, key string, updates map[string]interface{}) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	entry, ok := m.store[key]
	if !ok {
		entry = cacheEntry{value: make(map[string]interface{}), expires: time.Now().Add(m.ttl)}
	}
	for k, v := range updates {
		entry.value[k] = v
	}
	entry.expires = time.Now().Add(m.ttl)
	m.store[key] = entry
	return nil
}

func (m *MemoryCache) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.store, key)
	return nil
}

func (m *MemoryCache) GetByProviderID(_ context.Context, _ string) (map[string]interface{}, error) {
	return nil, nil
}

// ── Disabled Provider Stub ──

type disabledProvider struct{}

func (d *disabledProvider) IsEnabled() bool { return false }
func (d *disabledProvider) AddMagnet(ctx context.Context, magnet string) (*AddResult, error) {
	return nil, errors.New("debrid not configured")
}
func (d *disabledProvider) GetTorrentInfo(ctx context.Context, id string) (*TorrentInfo, error) {
	return nil, errors.New("debrid not configured")
}
func (d *disabledProvider) SelectFiles(ctx context.Context, id string, fileIDs []string) error {
	return errors.New("debrid not configured")
}
func (d *disabledProvider) UnrestrictLink(ctx context.Context, link string) (*UnrestrictResult, error) {
	return nil, errors.New("debrid not configured")
}
func (d *disabledProvider) DeleteTorrent(ctx context.Context, id string) error {
	return errors.New("debrid not configured")
}
func (d *disabledProvider) GetTorrents(ctx context.Context) ([]Torrent, error) {
	return nil, errors.New("debrid not configured")
}
func (d *disabledProvider) CheckCached(ctx context.Context, hashes []string) (map[string]CacheStatus, error) {
	result := make(map[string]CacheStatus)
	for _, h := range hashes {
		result[h] = CacheStatus{Cached: false}
	}
	return result, nil
}
func (d *disabledProvider) AddAndSelect(ctx context.Context, magnet string) (*TorrentInfo, error) {
	return nil, errors.New("debrid not configured")
}
func (d *disabledProvider) GetCachedFileInfo(ctx context.Context, hash, fileName string) (*FileInfo, error) {
	return nil, errors.New("debrid not configured")
}

// ── Unified CheckCached Fallback Orchestrator ──

// CheckCached implements unified cache checks for torrent lists.
// Calls the live provider CheckCached endpoint (e.g. TorBox) and falls back on SQLite DB (e.g. Real-Debrid).
func CheckCached(hashes []string, db *gorm.DB) map[string]bool {
	cfg := config.Load()
	p := GetProvider(cfg)

	normalized := make(map[string]bool)
	for _, h := range hashes {
		normalized[h] = false
	}

	if p.IsEnabled() {
		result, err := p.CheckCached(context.Background(), hashes)
		if err == nil {
			for h, info := range result {
				normalized[h] = info.Cached
			}
			return normalized
		}
		utils.Logger.Warn().Err(err).Msg("Provider CheckCached failed, falling back to local SQLite database cache status.")
	}

	// SQLite Local DB Fallback path
	if db != nil && len(hashes) > 0 {
		var records []database.DebridTorrent
		err := db.Where("infohash IN ?", hashes).Find(&records).Error
		if err == nil {
			for _, r := range records {
				normalized[r.Infohash] = (r.Status == "downloaded")
			}
		}
	}

	return normalized
}
