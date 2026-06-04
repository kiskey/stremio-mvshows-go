package debrid

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/sevvian/smvshows-go/internal/config"
	"github.com/sevvian/smvshows-go/internal/utils"
)

type RealDebridProvider struct {
	client  *resty.Client
	apiKey  string
	enabled bool
}

func NewRealDebridProvider(cfg *config.Config) *RealDebridProvider {
	return &RealDebridProvider{
		client: resty.New().
			SetBaseURL("https://api.real-debrid.com/rest/1.0").
			SetTimeout(15 * time.Second).
			SetHeader("Authorization", "Bearer "+cfg.RealDebridAPIKey),
		apiKey:  cfg.RealDebridAPIKey,
		enabled: cfg.IsRDEnabled,
	}
}

func (r *RealDebridProvider) IsEnabled() bool {
	return r.enabled && r.apiKey != ""
}

// ── Real-Debrid API Models ──

type rdAddMagnetResponse struct {
	ID  string `json:"id"`
	URI string `json:"uri"`
}

type rdTorrentFile struct {
	ID       int    `json:"id"`
	Path     string `json:"path"`
	Bytes    int64  `json:"bytes"`
	Selected int    `json:"selected"`
}

type rdTorrentInfo struct {
	ID       string          `json:"id"`
	Filename string          `json:"filename"`
	Status   string          `json:"status"`
	Files    []rdTorrentFile `json:"files"`
	Links    []string        `json:"links"`
}

type rdUnrestrictResponse struct {
	Download string `json:"download"`
}

// ── Provider Interface Implementation ──

func (r *RealDebridProvider) AddMagnet(magnet string) (*AddResult, error) {
	var result rdAddMagnetResponse
	resp, err := r.client.R().
		SetFormData(map[string]string{"magnet": magnet}).
		SetResult(&result).
		Post("/torrents/addMagnet")

	if err != nil {
		return nil, err
	}
	if resp.StatusCode() != 201 {
		return nil, fmt.Errorf("RD addMagnet failed with status %d: %s", resp.StatusCode(), resp.String())
	}

	return &AddResult{
		ID:   result.ID,
		Hash: "", // RD doesn't return hash in addMagnet directly, retrieved in GetTorrentInfo if needed
		Name: "",
	}, nil
}

func (r *RealDebridProvider) GetTorrentInfo(id string) (*TorrentInfo, error) {
	var info rdTorrentInfo
	resp, err := r.client.R().
		SetResult(&info).
		Get("/torrents/info/" + id)

	if err != nil {
		return nil, err
	}
	if resp.StatusCode() == 404 {
		return nil, ErrResourceNotFound
	}
	if resp.StatusCode() != 200 {
		return nil, fmt.Errorf("RD getTorrentInfo failed with status %d: %s", resp.StatusCode(), resp.String())
	}

	files := make([]TorrentFile, len(info.Files))
	for i, f := range info.Files {
		files[i] = TorrentFile{
			ID:       f.ID,
			Path:     f.Path,
			Bytes:    f.Bytes,
			Selected: f.Selected,
		}
	}

	return &TorrentInfo{
		ID:       info.ID,
		Filename: info.Filename,
		Status:   mapRealDebridStatus(info.Status),
		Files:    files,
		Links:    info.Links,
	}, nil
}

func (r *RealDebridProvider) SelectFiles(id string, fileIds string) error {
	resp, err := r.client.R().
		SetFormData(map[string]string{"files": fileIds}).
		Post("/torrents/selectFiles/" + id)

	if err != nil {
		return err
	}
	if resp.StatusCode() == 404 {
		return ErrResourceNotFound
	}
	// 204 No Content is standard for selectFiles
	if resp.StatusCode() != 204 && resp.StatusCode() != 200 {
		return fmt.Errorf("RD selectFiles failed with status %d: %s", resp.StatusCode(), resp.String())
	}

	return nil
}

func (r *RealDebridProvider) UnrestrictLink(link string) (*UnrestrictResult, error) {
	var result rdUnrestrictResponse
	resp, err := r.client.R().
		SetFormData(map[string]string{"link": link}).
		SetResult(&result).
		Post("/unrestrict/link")

	if err != nil {
		return nil, err
	}
	if resp.StatusCode() != 200 {
		return nil, fmt.Errorf("RD unrestrictLink failed with status %d: %s", resp.StatusCode(), resp.String())
	}

	return &UnrestrictResult{
		Download: result.Download,
	}, nil
}

func (r *RealDebridProvider) AddAndSelect(magnet string) (*TorrentInfo, error) {
	addRes, err := r.AddMagnet(magnet)
	if err != nil {
		return nil, err
	}

	// Wait briefly for RD to process metadata
	time.Sleep(1 * time.Second)

	err = r.SelectFiles(addRes.ID, "all")
	if err != nil {
		// If immediate selection fails (e.g. metadata still converting), wait and retry once
		utils.Logger.Debug().Str("id", addRes.ID).Msg("RD selectFiles 'all' failed initially. Retrying after 2s pause.")
		time.Sleep(2 * time.Second)
		if retryErr := r.SelectFiles(addRes.ID, "all"); retryErr != nil {
			return nil, retryErr
		}
	}

	return r.GetTorrentInfo(addRes.ID)
}

func (r *RealDebridProvider) CheckCached(hashes []string) (map[string]CacheInfo, error) {
	// Real-Debrid does not support a batch cache-check endpoint;
	// we return ErrNotSupported to leverage the unified database cache fallback.
	return nil, ErrNotSupported
}

func (r *RealDebridProvider) GetCachedFileInfo(hash, fileName string) (*FileInfo, error) {
	// Optional method, used internally if needed
	return nil, ErrNotSupported
}

func (r *RealDebridProvider) GetDownloadLinkForFile(torrentID string, fileID int) (string, error) {
	info, err := r.GetTorrentInfo(torrentID)
	if err != nil {
		return "", err
	}

	// Real-Debrid links array corresponds index-wise to files inside the torrent.
	// Filter selected files and match the requested index.
	linkIndex := 0
	matched := false
	for _, f := range info.Files {
		if f.Selected == 1 {
			if f.ID == fileID {
				matched = true
				break
			}
			linkIndex++
		}
	}

	if !matched || linkIndex >= len(info.Links) {
		return "", fmt.Errorf("requested file ID %d is either not selected or links array is out of range", fileID)
	}

	unrestricted, err := r.UnrestrictLink(info.Links[linkIndex])
	if err != nil {
		return "", err
	}

	return unrestricted.Download, nil
}

// ── Real-Debrid Status Mapping Helper ──

func mapRealDebridStatus(rdStatus string) string {
	s := strings.ToLower(rdStatus)
	switch s {
	case "magnet_conversion", "waiting_files_selection", "queued":
		return "downloading"
	case "downloading":
		return "downloading"
	case "downloaded", "compressing", "uploading":
		return "downloaded"
	case "error", "magnet_error", "dead":
		return "error"
	default:
		return "downloading"
	}
}
