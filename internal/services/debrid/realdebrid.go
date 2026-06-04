package debrid

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/kiskey/stremio-mvshows-go/internal/config"
	"github.com/kiskey/stremio-mvshows-go/internal/utils"
)

type realDebridProvider struct {
	client       *http.Client
	torrentCache struct {
		mu        sync.RWMutex
		data      []Torrent
		fetchedAt time.Time
	}
}

func NewRealDebrid() Provider {
	return &realDebridProvider{
		client: utils.NewOptimizedClient(15 * time.Second),
	}
}

func (r *realDebridProvider) IsEnabled() bool {
	return config.IsRDEnabled
}

func (r *realDebridProvider) do(ctx context.Context, method, path string, body string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, "https://api.real-debrid.com/rest/1.0"+path, strings.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+config.RealDebridAPIKey)
	if body != "" {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	return r.client.Do(req)
}

func (r *realDebridProvider) AddMagnet(ctx context.Context, magnet string) (*AddResult, error) {
	body := "magnet=" + url.QueryEscape(magnet)
	resp, err := r.do(ctx, "POST", "/torrents/addMagnet", body)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 201 {
		return nil, fmt.Errorf("addMagnet status %d", resp.StatusCode)
	}
	var data map[string]interface{}
	if err := rdJsonDecode(resp, &data); err != nil {
		return nil, err
	}
	id, _ := data["id"].(string)
	utils.Logger.Info("real-debrid magnet added", "id", id)
	return &AddResult{ID: id}, nil
}

func (r *realDebridProvider) GetTorrentInfo(ctx context.Context, id string) (*TorrentInfo, error) {
	resp, err := r.do(ctx, "GET", "/torrents/info/"+id, "")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == 404 {
		return nil, ErrResourceNotFound
	}
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("getTorrentInfo status %d", resp.StatusCode)
	}
	var data map[string]interface{}
	if err := rdJsonDecode(resp, &data); err != nil {
		return nil, err
	}
	return mapRDInfo(data), nil
}

func mapRDInfo(data map[string]interface{}) *TorrentInfo {
	info := &TorrentInfo{}
	info.ID, _ = data["id"].(string)
	info.Filename, _ = data["filename"].(string)
	info.Status, _ = data["status"].(string)
	if b, ok := data["bytes"].(float64); ok {
		info.Bytes = int64(b)
	}
	if s, ok := data["seeders"].(float64); ok {
		info.Seeders = int(s)
	}
	if filesRaw, ok := data["files"].([]interface{}); ok {
		for _, f := range filesRaw {
			fm, _ := f.(map[string]interface{})
			fid, _ := fm["id"].(float64)
			path, _ := fm["path"].(string)
			bytes, _ := fm["bytes"].(float64)
			
			selected := 0
			if sVal, ok := fm["selected"].(float64); ok {
				selected = int(sVal)
			}

			info.Files = append(info.Files, FileInfo{
				ID:       int(fid),
				Path:     path,
				Bytes:    int64(bytes),
				Selected: selected,
			})
		}
	}
	if linksRaw, ok := data["links"].([]interface{}); ok {
		for _, l := range linksRaw {
			if ls, ok := l.(string); ok {
				info.Links = append(info.Links, ls)
			}
		}
	}
	return info
}

func (r *realDebridProvider) SelectFiles(ctx context.Context, id string, fileIDs []string) error {
	param := "all"
	if len(fileIDs) > 0 && fileIDs[0] != "all" {
		param = strings.Join(fileIDs, ",")
	}
	body := "files=" + param
	resp, err := r.do(ctx, "POST", "/torrents/selectFiles/"+id, body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == 202 {
		utils.Logger.Info("real-debrid files already selected", "id", id)
		return nil
	}
	if resp.StatusCode != 204 {
		return fmt.Errorf("selectFiles status %d", resp.StatusCode)
	}
	return nil
}

func (r *realDebridProvider) UnrestrictLink(ctx context.Context, link string) (*UnrestrictResult, error) {
	body := "link=" + url.QueryEscape(link)
	resp, err := r.do(ctx, "POST", "/unrestrict/link", body)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("unrestrict status %d", resp.StatusCode)
	}
	var data map[string]interface{}
	if err := rdJsonDecode(resp, &data); err != nil {
		return nil, err
	}
	dl, _ := data["download"].(string)
	return &UnrestrictResult{Download: dl}, nil
}

func (r *realDebridProvider) DeleteTorrent(ctx context.Context, id string) error {
	resp, err := r.do(ctx, "DELETE", "/torrents/delete/"+id, "")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	utils.Logger.Info("real-debrid torrent deleted", "id", id)
	return nil
}

func (r *realDebridProvider) GetTorrents(ctx context.Context) ([]Torrent, error) {
	resp, err := r.do(ctx, "GET", "/torrents", "")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var data []map[string]interface{}
	if err := rdJsonDecode(resp, &data); err != nil {
		return nil, err
	}
	var torrents []Torrent
	for _, t := range data {
		id, _ := t["id"].(string)
		hash, _ := t["hash"].(string)
		name, _ := t["filename"].(string)
		status, _ := t["status"].(string)
		torrents = append(torrents, Torrent{ID: id, Hash: hash, Name: name, Status: status})
	}
	return torrents, nil
}

func (r *realDebridProvider) getCachedTorrents(ctx context.Context) ([]Torrent, error) {
	r.torrentCache.mu.RLock()
	if time.Since(r.torrentCache.fetchedAt) < 30*time.Second && r.torrentCache.data != nil {
		defer r.torrentCache.mu.RUnlock()
		return r.torrentCache.data, nil
	}
	r.torrentCache.mu.RUnlock()

	torrents, err := r.GetTorrents(ctx)
	if err != nil {
		return nil, err
	}
	r.torrentCache.mu.Lock()
	r.torrentCache.data = torrents
	r.torrentCache.fetchedAt = time.Now()
	r.torrentCache.mu.Unlock()
	return torrents, nil
}

func (r *realDebridProvider) CheckCached(ctx context.Context, hashes []string) (map[string]CacheStatus, error) {
	result := make(map[string]CacheStatus)
	for _, h := range hashes {
		result[h] = CacheStatus{Cached: false}
	}

	torrents, err := r.getCachedTorrents(ctx)
	if err != nil {
		return result, nil // soft fail: mark all uncached
	}

	// Build hash lookup
	hashMap := make(map[string]Torrent)
	for _, t := range torrents {
		hashMap[strings.ToLower(t.Hash)] = t
	}

	for _, h := range hashes {
		hLower := strings.ToLower(h)
		if t, ok := hashMap[hLower]; ok {
			cs := CacheStatus{Cached: true, TorrentID: t.ID, Name: t.Name}
			result[h] = cs
		}
	}
	return result, nil
}

func (r *realDebridProvider) AddAndSelect(ctx context.Context, magnet string) (*TorrentInfo, error) {
	addRes, err := r.AddMagnet(ctx, magnet)
	if err != nil {
		return nil, err
	}
	err = r.SelectFiles(ctx, addRes.ID, []string{"all"})
	if err != nil {
		return nil, err
	}
	return r.GetTorrentInfo(ctx, addRes.ID)
}

func (r *realDebridProvider) GetCachedFileInfo(ctx context.Context, hash, fileName string) (*FileInfo, error) {
	return nil, fmt.Errorf("not supported for real-debrid")
}

func rdJsonDecode(resp *http.Response, v interface{}) error {
	return json.NewDecoder(resp.Body).Decode(v)
}
