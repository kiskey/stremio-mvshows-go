package debrid

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/kiskey/stremio-mvshows-go/internal/config"
	"github.com/kiskey/stremio-mvshows-go/internal/database"
	"github.com/kiskey/stremio-mvshows-go/internal/utils"
	"golang.org/x/net/http2"
	"golang.org/x/time/rate"
)

type TorboxProvider struct {
	client            *resty.Client
	apiKey            string
	enabled           bool
	addLimiter        *rate.Limiter // Rate limit 8 adds/min for createtorrent
	generalLimiter    *rate.Limiter // General rate limit 5 reqs/sec per IP
	torrentSelections sync.Map      // map[int]map[int]bool (torrentID -> set of selected fileIDs)
}

// createOptimizedTorboxHTTPClient configures an transport optimized for low latency and high concurrency
func createOptimizedTorboxHTTPClient(timeout time.Duration) *http.Client {
	transport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,  // Faster connect timeout
			KeepAlive: 30 * time.Second, // Consistent keep-alive
		}).DialContext,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   100,              // Avoid connection starvation under concurrency
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   3 * time.Second,  // Faster TLS handshakes
		ExpectContinueTimeout: 1 * time.Second,
		ForceAttemptHTTP2:     true,             // Force HTTP/2 attempt
		TLSClientConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
		},
	}
	// Explicitly configure HTTP/2 transport settings
	_ = http2.ConfigureTransport(transport)

	return &http.Client{
		Transport: transport,
		Timeout:   timeout,
	}
}

func NewTorboxProvider(cfg *config.Config) *TorboxProvider {
	// Initialize resty using our fine-tuned transport
	httpClient := createOptimizedTorboxHTTPClient(20 * time.Second)
	restyClient := resty.NewWithClient(httpClient).
		SetBaseURL("https://api.torbox.app/v1/api").
		SetHeader("Authorization", "Bearer "+cfg.TorboxApiKey)

	return &TorboxProvider{
		client:         restyClient,
		apiKey:         cfg.TorboxApiKey,
		enabled:        cfg.IsTorboxEnabled,
		addLimiter:     rate.NewLimiter(rate.Every(time.Minute/8), 8),
		generalLimiter: rate.NewLimiter(rate.Limit(5), 5),
	}
}

func (t *TorboxProvider) IsEnabled() bool {
	return t.enabled && t.apiKey != ""
}

// ── TorBox API Models ──

type tbCreateResponse struct {
	Success bool   `json:"success"`
	Detail  string `json:"detail"`
	Data    struct {
		ID   int    `json:"id"`
		Hash string `json:"hash"`
		Name string `json:"name"`
	} `json:"data"`
}

type tbTorrentFile struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
	Size int64  `json:"size"`
}

type tbTorrentInfo struct {
	Name   string          `json:"name"`
	Hash   string          `json:"hash"`
	Status string          `json:"status"`
	State  string          `json:"state"`
	Files  []tbTorrentFile `json:"files"`
}

type tbTorrentInfoResponse struct {
	Success bool          `json:"success"`
	Data    tbTorrentInfo `json:"data"`
}

type tbRequestDlResponse struct {
	Success bool   `json:"success"`
	Data    string `json:"data"` // Direct URL returned
}

type tbCheckCachedResponse struct {
	Success bool                   `json:"success"`
	Data    map[string]interface{} `json:"data"` // e.g. {"hash": true, "hash": {"cached": true}}
}

// ── Provider Interface Implementation ──

func (t *TorboxProvider) AddMagnet(magnet string) (*AddResult, error) {
	// Apply rate limiting for torrent creation
	err := t.addLimiter.Wait(context.Background())
	if err != nil {
		return nil, err
	}

	var result tbCreateResponse
	resp, err := t.client.R().
		SetFormData(map[string]string{"magnet": magnet}).
		SetResult(&result).
		Post("/torrents/createtorrent")

	if err != nil {
		return nil, err
	}
	if resp.StatusCode() != 200 {
		return nil, fmt.Errorf("TorBox createtorrent failed with status %d: %s", resp.StatusCode(), resp.String())
	}
	if !result.Success {
		return nil, fmt.Errorf("TorBox createtorrent returned success=false: %s", result.Detail)
	}

	id := result.Data.ID
	hash := strings.ToLower(result.Data.Hash)

	// Persist the mapping in GORM so it survives server restarts cleanly
	if database.DB != nil && id > 0 && hash != "" {
		errDb := database.DB.Save(&database.TorboxIdMap{
			TorrentID: id,
			Hash:      hash,
		}).Error
		if errDb != nil {
			utils.Logger.Error().Err(errDb).Msg("Failed to persist TorboxIdMap mapping in DB.")
		}
	}

	return &AddResult{
		ID:   strconv.Itoa(id),
		Hash: hash,
		Name: result.Data.Name,
	}, nil
}

func (t *TorboxProvider) GetTorrentInfo(idStr string) (*TorrentInfo, error) {
	_ = t.generalLimiter.Wait(context.Background())

	id, err := strconv.Atoi(idStr)
	if err != nil {
		return nil, fmt.Errorf("invalid TorBox torrent ID: %s", idStr)
	}

	// Resolve the hash associated with this ID from DB
	var mapping database.TorboxIdMap
	if database.DB != nil {
		errDb := database.DB.First(&mapping, "torrent_id = ?", id).Error
		if errDb != nil {
			return nil, ErrResourceNotFound
		}
	} else {
		return nil, fmt.Errorf("database mapping unavailable")
	}

	var result tbTorrentInfoResponse
	resp, err := t.client.R().
		SetQueryParam("hash", mapping.Hash).
		SetResult(&result).
		Get("/torrents/torrentinfo")

	if err != nil {
		return nil, err
	}
	if resp.StatusCode() == 404 {
		return nil, ErrResourceNotFound
	}
	if resp.StatusCode() != 200 {
		return nil, fmt.Errorf("TorBox torrentinfo failed with status %d: %s", resp.StatusCode(), resp.String())
	}

	tbInfo := result.Data
	statusField := tbInfo.Status
	if statusField == "" {
		statusField = tbInfo.State
	}
	mappedStatus := mapTorboxStatus(statusField)

	// Build the GORM-compatible file structure
	files := make([]TorrentFile, len(tbInfo.Files))
	var selectionMap map[int]bool
	if val, ok := t.torrentSelections.Load(id); ok {
		selectionMap = val.(map[int]bool)
	}

	for i, f := range tbInfo.Files {
		selected := 1
		if len(selectionMap) > 0 {
			if selectionMap[f.ID] {
				selected = 1
			} else {
				selected = 0
			}
		}
		files[i] = TorrentFile{
			ID:       f.ID,
			Path:     f.Name,
			Bytes:    f.Size,
			Selected: selected,
		}
	}

	// Construct direct download links when ready on TorBox cache/seeding lists
	links := make([]string, len(files))
	if mappedStatus == "downloaded" {
		for i, f := range files {
			if f.Selected == 1 {
				dlLink, errDl := t.GetDownloadLinkForFile(idStr, f.ID)
				if errDl == nil {
					links[i] = dlLink
				} else {
					utils.Logger.Warn().Err(errDl).Int("fileID", f.ID).Msg("Failed to generate direct TorBox link for file.")
				}
			}
		}
	}

	return &TorrentInfo{
		ID:       idStr,
		Filename: tbInfo.Name,
		Status:   mappedStatus,
		Files:    files,
		Links:    links,
	}, nil
}

func (t *TorboxProvider) SelectFiles(idStr string, fileIds string) error {
	id, err := strconv.Atoi(idStr)
	if err != nil {
		return err
	}

	// Simulated selection: TorBox always downloads all files, so we manage selections locally
	selectionMap := make(map[int]bool)
	if fileIds != "all" {
		parts := strings.Split(fileIds, ",")
		for _, part := range parts {
			if fid, err := strconv.Atoi(part); err == nil {
				selectionMap[fid] = true
			}
		}
	}
	t.torrentSelections.Store(id, selectionMap)
	return nil
}

func (t *TorboxProvider) UnrestrictLink(link string) (*UnrestrictResult, error) {
	// TorBox links are direct and do not require unrestriction. Wrap as-is.
	if strings.HasPrefix(link, "http://") || strings.HasPrefix(link, "https://") {
		return &UnrestrictResult{Download: link}, nil
	}
	return nil, fmt.Errorf("TorBox does not support hoster link unrestriction")
}

func (t *TorboxProvider) AddAndSelect(magnet string) (*TorrentInfo, error) {
	addRes, err := t.AddMagnet(magnet)
	if err != nil {
		return nil, err
	}

	err = t.SelectFiles(addRes.ID, "all")
	if err != nil {
		return nil, err
	}

	return t.GetTorrentInfo(addRes.ID)
}

func (t *TorboxProvider) CheckCached(hashes []string) (map[string]CacheInfo, error) {
	_ = t.generalLimiter.Wait(context.Background())

	if len(hashes) == 0 {
		return make(map[string]CacheInfo), nil
	}

	var result tbCheckCachedResponse
	resp, err := t.client.R().
		SetBody(map[string]interface{}{"hashes": hashes}).
		SetResult(&result).
		Post("/torrents/checkcached")

	if err != nil {
		return nil, err
	}
	if resp.StatusCode() != 200 {
		return nil, fmt.Errorf("TorBox checkcached failed with status %d: %s", resp.StatusCode(), resp.String())
	}

	resMap := make(map[string]CacheInfo)
	for _, h := range hashes {
		lowerH := strings.ToLower(h)
		resMap[lowerH] = CacheInfo{Cached: false}
	}

	for rawH, rawVal := range result.Data {
		lowerH := strings.ToLower(rawH)
		cached := false

		// Parse boolean or complex object shapes
		if valBool, ok := rawVal.(bool); ok {
			cached = valBool
		} else if mapVal, ok := rawVal.(map[string]interface{}); ok {
			if val, ok := mapVal["cached"].(bool); ok {
				cached = val
			} else {
				// Fallback: presence of any matching data object indicates cached
				cached = true
			}
		}

		resMap[lowerH] = CacheInfo{Cached: cached}
	}

	return resMap, nil
}

func (t *TorboxProvider) GetCachedFileInfo(hash, fileName string) (*FileInfo, error) {
	return nil, ErrNotSupported
}

func (t *TorboxProvider) GetDownloadLinkForFile(torrentID string, fileID int) (string, error) {
	_ = t.generalLimiter.Wait(context.Background())

	var data map[string]interface{}
	resp, err := t.client.R().
		SetQueryParams(map[string]string{
			"token":      t.apiKey,
			"torrent_id": torrentID,
			"file_id":    strconv.Itoa(fileID),
			"redirect":   "false",
		}).
		SetResult(&data).
		Get("/torrents/requestdl")

	if err != nil {
		return "", err
	}
	if resp.StatusCode() != 200 {
		return "", fmt.Errorf("TorBox requestdl failed with status %d: %s", resp.StatusCode(), resp.String())
	}

	// Unmarshal correctly
	urlStr := ""
	if success, ok := data["success"].(bool); ok && success {
		if rawURL, ok := data["data"].(string); ok {
			urlStr = rawURL
		}
	}

	if urlStr == "" {
		return "", fmt.Errorf("failed to retrieve direct URL from TorBox payload: %s", resp.String())
	}

	return urlStr, nil
}

// ── TorBox Status Mapping Helper ──

func mapTorboxStatus(tbStatus string) string {
	s := strings.ToLower(tbStatus)
	switch s {
	case "downloading", "metadl", "checkingresumedata", "paused", "stalled (no seeds)":
		return "downloading"
	case "completed", "cached", "uploading", "seeding":
		return "downloaded"
	case "error", "failed":
		return "error"
	default:
		return "downloading"
	}
}
