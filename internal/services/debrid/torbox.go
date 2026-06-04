package debrid

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/kiskey/stremio-mvshows-go/internal/config"
	"github.com/kiskey/stremio-mvshows-go/internal/utils"
)

type torboxProvider struct {
	client        *http.Client
	apiKey        string
	enabled       bool
	cache         CacheStore
	selections    map[string]map[int]bool
	recentAdds    map[string]time.Time
	recentIDs     map[string]string
	addTimestamps []time.Time
	mu            sync.Mutex
}

type CacheStore interface {
	Get(ctx context.Context, hash string) (map[string]interface{}, error)
	Set(ctx context.Context, hash string, data map[string]interface{}) error
	Update(ctx context.Context, hash string, updates map[string]interface{}) error
	GetByProviderID(ctx context.Context, id string) (map[string]interface{}, error)
}

// ── Low-Allocation Optimized Type-Safe API Structs ──────────────

type TorboxCreateResponse struct {
	Success bool   `json:"success"`
	Detail  string `json:"detail"`
	Data    struct {
		TorrentID json.Number `json:"torrent_id"`
		ID        json.Number `json:"id"`
		Hash      string      `json:"hash"`
		Name      string      `json:"name"`
	} `json:"data"`
}

type TorboxTorrentItem struct {
	ID            json.Number `json:"id"`
	TorrentID     json.Number `json:"torrent_id"`
	Hash          string      `json:"hash"`
	Name          string      `json:"name"`
	Status        string      `json:"status"`
	DownloadState string      `json:"download_state"`
	Files         []struct {
		ID   json.Number `json:"id"`
		Name string      `json:"name"`
		Size int64       `json:"size"`
	} `json:"files"`
}

type TorboxMyListResponse struct {
	Success bool            `json:"success"`
	Data    json.RawMessage `json:"data"`
}

type TorboxCachedItem struct {
	ID    json.Number `json:"id"`
	Name  string      `json:"name"`
	Size  int64       `json:"size"`
	Files []struct {
		ID   json.Number `json:"id"`
		Name string      `json:"name"`
		Size int64       `json:"size"`
	} `json:"files"`
}

type TorboxCheckCachedResponse struct {
	Success bool                         `json:"success"`
	Data    map[string]TorboxCachedItem  `json:"data"`
}

type TorboxRequestDlResponse struct {
	Success bool            `json:"success"`
	Data    json.RawMessage `json:"data"`
}

func getIDAsString(item map[string]interface{}, keys ...string) string {
	for _, key := range keys {
		if val, ok := item[key]; ok && val != nil {
			if s, ok := val.(string); ok {
				return s
			}
			if f, ok := val.(float64); ok {
				return fmt.Sprintf("%.0f", f)
			}
		}
	}
	return ""
}

func NewTorbox(cache CacheStore) Provider {
	cfg := config.Load()
	// Dynamically expire shared memory caches for TorBox to enforce strict on-demand play redirects
	URLCache.ttl = 1 * time.Second
	TorrentInfoCache.ttl = 5 * time.Second

	return &torboxProvider{
		client:        utils.NewOptimizedClient(15 * time.Second),
		apiKey:        cfg.TorboxAPIKey,
		enabled:       cfg.IsTorboxEnabled,
		cache:         nil, // explicitly ignore and bypass local persistent DB caching for TorBox
		selections:    make(map[string]map[int]bool),
		recentAdds:    make(map[string]time.Time),
		recentIDs:     make(map[string]string),
		addTimestamps: []time.Time{},
	}
}

func (t *torboxProvider) IsEnabled() bool {
	return t.enabled && t.apiKey != ""
}

func (t *torboxProvider) do(ctx context.Context, method, path string, body io.Reader, contentType string) (*http.Response, error) {
	url := "https://api.torbox.app/v1/api" + path
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+t.apiKey)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	return t.client.Do(req)
}

func extractInfoHash(magnet string) string {
	idx := strings.Index(magnet, "btih:")
	if idx == -1 {
		return ""
	}
	start := idx + 5
	end := start + 40
	if end > len(magnet) {
		end = len(magnet)
	}
	return strings.ToLower(magnet[start:end])
}

func (t *torboxProvider) checkRateLimit() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := time.Now()
	cutoff := now.Add(-60 * time.Second)
	var kept []time.Time
	for _, ts := range t.addTimestamps {
		if ts.After(cutoff) {
			kept = append(kept, ts)
		}
	}
	t.addTimestamps = kept
	if len(kept) >= 8 {
		return false
	}
	t.addTimestamps = append(t.addTimestamps, now)
	return true
}

func (t *torboxProvider) cleanupStaleActiveTorrents(ctx context.Context, maxActive int) error {
	torrents, err := t.GetTorrents(ctx)
	if err != nil {
		return err
	}
	activeCount := 0
	var stale []Torrent
	for _, tr := range torrents {
		if tr.Status == "downloading" {
			activeCount++
		}
		if tr.Status == "error" {
			stale = append(stale, tr)
		}
	}

	for _, s := range stale {
		_ = t.DeleteTorrent(ctx, s.ID)
	}

	if activeCount >= maxActive {
		for i := len(torrents) - 1; i >= 0; i-- {
			if torrents[i].Status == "downloading" {
				_ = t.DeleteTorrent(ctx, torrents[i].ID)
				activeCount--
				if activeCount < maxActive {
					break
				}
			}
		}
	}
	return nil
}

func (t *torboxProvider) AddMagnet(ctx context.Context, magnet string) (*AddResult, error) {
	hash := extractInfoHash(magnet)
	if hash == "" {
		return nil, fmt.Errorf("invalid magnet link")
	}

	t.mu.Lock()
	if id, ok := t.recentIDs[hash]; ok && id != "" {
		t.mu.Unlock()
		return &AddResult{ID: id, Hash: hash, Cached: true}, nil
	}
	t.mu.Unlock()

	if !t.checkRateLimit() {
		return nil, fmt.Errorf("torbox addMagnet rate limit exceeded")
	}

	cfg := config.Load()
	if cfg.TorboxMaxActiveTorrents > 0 {
		if err := t.cleanupStaleActiveTorrents(ctx, cfg.TorboxMaxActiveTorrents); err != nil {
			utils.Logger.Warn().Err(err).Msg("torbox cleanup failed")
		}
	}

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	writer.WriteField("magnet", magnet)
	writer.Close()

	var lastErr error
	var resolvedResult *AddResult

	for attempt := 0; attempt < 3; attempt++ {
		resp, err := t.do(ctx, "POST", "/torrents/createtorrent", &body, writer.FormDataContentType())
		if err != nil {
			lastErr = err
			break
		}

		err = func() error {
			defer resp.Body.Close()

			var data TorboxCreateResponse
			if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
				return err
			}

			torrentID := data.Data.TorrentID.String()
			if torrentID == "" || torrentID == "0" {
				torrentID = data.Data.ID.String()
			}
			hashRet := data.Data.Hash
			name := data.Data.Name

			isCached := false
			if strings.Contains(strings.ToLower(data.Detail), "cached torrent") {
				isCached = true
			}
			if torrentID != "" && hashRet != "" {
				t.mu.Lock()
				t.recentIDs[hash] = torrentID
				t.recentIDs[strings.ToLower(hashRet)] = torrentID
				t.mu.Unlock()

				resolvedResult = &AddResult{ID: torrentID, Hash: hashRet, Name: name, Cached: isCached}
				return nil
			}
			return fmt.Errorf("addMagnet response missing id/hash")
		}()

		if err != nil {
			lastErr = err
			continue
		}
		if resolvedResult != nil {
			return resolvedResult, nil
		}
	}
	return nil, lastErr
}

func (t *torboxProvider) GetTorrentInfo(ctx context.Context, id string) (*TorrentInfo, error) {
	resp, err := t.do(ctx, "GET", "/torrents/mylist?id="+id, nil, "")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == 404 {
		return nil, ErrResourceNotFound
	}
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("gettorrentinfo status %d", resp.StatusCode)
	}

	var data TorboxMyListResponse
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return nil, err
	}

	var items []TorboxTorrentItem
	dataStr := strings.TrimSpace(string(data.Data))
	if strings.HasPrefix(dataStr, "[") {
		json.Unmarshal(data.Data, &items)
	} else if strings.HasPrefix(dataStr, "{") {
		var single TorboxTorrentItem
		if err := json.Unmarshal(data.Data, &single); err == nil {
			items = []TorboxTorrentItem{single}
		}
	}

	if len(items) == 0 {
		return nil, ErrResourceNotFound
	}
	return mapTBInfo(id, items[0], t.selections), nil
}

func mapTBInfo(id string, item TorboxTorrentItem, selections map[string]map[int]bool) *TorrentInfo {
	info := &TorrentInfo{ID: id}
	info.Filename = item.Name
	rawStatus := item.DownloadState
	if rawStatus == "" {
		rawStatus = item.Status
	}
	info.Status = mapTBStatus(rawStatus)
	selectedSet := selections[id]
	if selectedSet == nil {
		selectedSet = make(map[int]bool)
	}

	for i, f := range item.Files {
		var fid int
		if f.ID != "" {
			if id64, err := f.ID.Int64(); err == nil {
				fid = int(id64)
			} else {
				fid, _ = strconv.Atoi(f.ID.String())
			}
		} else {
			fid = i
		}

		sel := 1
		if len(selectedSet) > 0 && !selectedSet[fid] {
			sel = 0
		}
		info.Files = append(info.Files, FileInfo{ID: fid, Path: f.Name, Bytes: f.Size, Selected: sel})
	}

	if info.Status == "downloaded" {
		for _, f := range info.Files {
			info.Links = append(info.Links, fmt.Sprintf("tb:%s:%d", id, f.ID))
		}
	} else {
		for range info.Files {
			info.Links = append(info.Links, "")
		}
	}
	return info
}

func mapTBStatus(s string) string {
	switch strings.ToLower(s) {
	case "completed", "cached", "uploading", "seeding", "active", "downloaded":
		return "downloaded"
	case "downloading", "metadl", "checkingresumedata", "stalled", "stalled (no seeds)", "queued":
		return "downloading"
	case "error", "failed", "missingfiles", "expired":
		return "error"
	default:
		return "downloading"
	}
}

func (t *torboxProvider) SelectFiles(ctx context.Context, id string, fileIDs []string) error {
	resp, err := t.do(ctx, "GET", "/torrents/mylist?id="+id, nil, "")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("selectfiles status %d", resp.StatusCode)
	}

	var data TorboxMyListResponse
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return err
	}

	var items []TorboxTorrentItem
	dataStr := strings.TrimSpace(string(data.Data))
	if strings.HasPrefix(dataStr, "[") {
		json.Unmarshal(data.Data, &items)
	} else if strings.HasPrefix(dataStr, "{") {
		var single TorboxTorrentItem
		if err := json.Unmarshal(data.Data, &single); err == nil {
			items = []TorboxTorrentItem{single}
		}
	}

	if len(items) == 0 {
		return fmt.Errorf("torrent not found")
	}

	set := make(map[int]bool)
	if len(fileIDs) == 1 && fileIDs[0] == "all" {
		for i, f := range items[0].Files {
			var fid int
			if f.ID != "" {
				if id64, err := f.ID.Int64(); err == nil {
					fid = int(id64)
				} else {
					fid, _ = strconv.Atoi(f.ID.String())
				}
			} else {
				fid = i
			}
			set[fid] = true
		}
	} else {
		for _, fid := range fileIDs {
			var fileId int
			fmt.Sscanf(fid, "%d", &fileId)
			set[fileId] = true
		}
	}
	t.mu.Lock()
	t.selections[id] = set
	t.mu.Unlock()
	return nil
}

func (t *torboxProvider) UnrestrictLink(ctx context.Context, link string) (*UnrestrictResult, error) {
	if strings.HasPrefix(link, "http://") || strings.HasPrefix(link, "https://") {
		return &UnrestrictResult{Download: link}, nil
	}
	if strings.HasPrefix(link, "tb:") {
		parts := strings.Split(link, ":")
		if len(parts) == 3 {
			url, err := t.GetDownloadLinkForFile(ctx, parts[1], parts[2])
			if err != nil {
				return nil, err
			}
			return &UnrestrictResult{Download: url}, nil
		}
	}
	return nil, fmt.Errorf("invalid torbox link format")
}

func (t *torboxProvider) DeleteTorrent(ctx context.Context, id string) error {
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	writer.WriteField("id", id)
	writer.WriteField("action", "delete")
	writer.Close()
	resp, err := t.do(ctx, "POST", "/torrents/controltorrent", &body, writer.FormDataContentType())
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	t.mu.Lock()
	for h, rid := range t.recentIDs {
		if rid == id {
			delete(t.recentIDs, h)
		}
	}
	delete(t.selections, id)
	t.mu.Unlock()

	return nil
}

func (t *torboxProvider) GetTorrents(ctx context.Context) ([]Torrent, error) {
	resp, err := t.do(ctx, "GET", "/torrents/mylist", nil, "")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("gettorrents status %d", resp.StatusCode)
	}

	var data TorboxMyListResponse
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return nil, err
	}

	var items []TorboxTorrentItem
	dataStr := strings.TrimSpace(string(data.Data))
	if strings.HasPrefix(dataStr, "[") {
		json.Unmarshal(data.Data, &items)
	} else if strings.HasPrefix(dataStr, "{") {
		var single TorboxTorrentItem
		if err := json.Unmarshal(data.Data, &single); err == nil {
			items = []TorboxTorrentItem{single}
		}
	}

	var torrents []Torrent
	for _, item := range items {
		id := item.TorrentID.String()
		if id == "" || id == "0" {
			id = item.ID.String()
		}
		status := item.Status
		if status == "" {
			status = item.DownloadState
		}
		torrents = append(torrents, Torrent{ID: id, Hash: item.Hash, Name: item.Name, Status: status})
	}
	return torrents, nil
}

func (t *torboxProvider) CheckCached(ctx context.Context, hashes []string) (map[string]CacheStatus, error) {
	if len(hashes) == 0 {
		return map[string]CacheStatus{}, nil
	}
	bodyMap := map[string]interface{}{"hashes": hashes}
	b, _ := jsonMarshal(bodyMap)
	req, _ := http.NewRequestWithContext(ctx, "POST", "https://api.torbox.app/v1/api/torrents/checkcached?format=object&list_files=true", bytes.NewReader(b))
	cfg := config.Load()
	req.Header.Set("Authorization", "Bearer "+cfg.TorboxAPIKey)
	req.Header.Set("Content-Type", "application/json")
	resp, err := t.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("checkcached status %d", resp.StatusCode)
	}

	var data TorboxCheckCachedResponse
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return nil, err
	}

	result := make(map[string]CacheStatus)
	for _, h := range hashes {
		val, ok := data.Data[h]
		if !ok {
			val, ok = data.Data[strings.ToLower(h)]
		}
		if !ok {
			val, ok = data.Data[strings.ToUpper(h)]
		}
		if ok {
			cs := CacheStatus{Cached: true}
			cs.TorrentID = val.ID.String()
			cs.Name = val.Name
			cs.Size = val.Size
			for i, f := range val.Files {
				var fid int
				if f.ID != "" {
					if id64, err := f.ID.Int64(); err == nil {
						fid = int(id64)
					} else {
						fid, _ = strconv.Atoi(f.ID.String())
					}
				} else {
					fid = i
				}
				cs.Files = append(cs.Files, CacheFile{ID: fid, Name: f.Name, Size: f.Size})
			}

			if cs.TorrentID != "" {
				t.mu.Lock()
				t.recentIDs[strings.ToLower(h)] = cs.TorrentID
				t.mu.Unlock()
			}

			result[h] = cs
		} else {
			result[h] = CacheStatus{Cached: false}
		}
	}
	return result, nil
}

func (t *torboxProvider) GetDownloadLinkForFile(ctx context.Context, torrentID, fileID string) (string, error) {
	// Retrieve exact filename for our structured playback INFO logging
	fileName := "unknown"
	if info, err := t.GetTorrentInfo(ctx, torrentID); err == nil && info != nil {
		var targetFID int
		if fIDInt, err := strconv.Atoi(fileID); err == nil {
			targetFID = fIDInt
		}
		for _, f := range info.Files {
			if f.ID == targetFID {
				fileName = f.Path
				break
			}
		}
	}

	utils.Logger.Info().
		Str("torrent_id", torrentID).
		Str("file_id", fileID).
		Str("file_name", fileName).
		Msg("torbox request download link")

	url := fmt.Sprintf("https://api.torbox.app/v1/api/torrents/requestdl?token=%s&torrent_id=%s&file_id=%s&redirect=false", t.apiKey, torrentID, fileID)
	req, _ := http.NewRequestWithContext(ctx, "GET", url, nil)
	req.Header.Set("Authorization", "Bearer "+t.apiKey)
	resp, err := t.client.Do(req)

	if err != nil || (resp != nil && resp.StatusCode != 200) {
		if resp != nil {
			resp.Body.Close()
		}
		utils.Logger.Warn().
			Str("torrent_id", torrentID).
			Str("file_id", fileID).
			Msg("torbox download link request failed, attempting self-healing")

		var infoHash string
		t.mu.Lock()
		for h, rid := range t.recentIDs {
			if rid == torrentID {
				infoHash = h
				break
			}
		}
		t.mu.Unlock()

		if infoHash != "" {
			utils.Logger.Info().Str("hash", infoHash).Msg("self-healing: re-adding expired/deleted torrent")
			magnet := fmt.Sprintf("magnet:?xt=urn:btih:%s", infoHash)
			newInfo, err := t.AddAndSelect(ctx, magnet)
			if err == nil && newInfo != nil && newInfo.ID != torrentID {
				utils.Logger.Info().Str("new_id", newInfo.ID).Msg("self-healing successful, retrying requestdl with new torrent ID")
				return t.GetDownloadLinkForFile(ctx, newInfo.ID, fileID)
			}
		}
		return "", fmt.Errorf("requestdl failed and self-healing could not recover")
	}
	defer resp.Body.Close()

	var data TorboxRequestDlResponse
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return "", err
	}

	var link string
	dataStr := strings.TrimSpace(string(data.Data))
	if strings.HasPrefix(dataStr, "\"") {
		json.Unmarshal(data.Data, &link)
	} else {
		var obj struct {
			URL string `json:"url"`
		}
		if err := json.Unmarshal(data.Data, &obj); err == nil {
			link = obj.URL
		}
	}

	if link != "" {
		return link, nil
	}
	return "", fmt.Errorf("no download url")
}

func (t *torboxProvider) GetCachedFileInfo(ctx context.Context, hash, fileName string) (*FileInfo, error) {
	cacheResult, err := t.CheckCached(ctx, []string{hash})
	if err != nil {
		return nil, err
	}
	info := cacheResult[hash]
	if !info.Cached || len(info.Files) == 0 {
		return nil, nil
	}
	for _, f := range info.Files {
		if strings.HasSuffix(f.Name, fileName) || f.Name == fileName {
			return &FileInfo{
				ID:    f.ID,
				Path:  f.Name,
				Bytes: f.Size,
			}, nil
		}
	}
	return nil, nil
}

func (t *torboxProvider) AddAndSelect(ctx context.Context, magnet string) (*TorrentInfo, error) {
	addRes, err := t.AddMagnet(ctx, magnet)
	if err != nil {
		return nil, err
	}
	if addRes.ID != "" {
		if err := t.SelectFiles(ctx, addRes.ID, []string{"all"}); err != nil {
			return nil, err
		}
		return t.GetTorrentInfo(ctx, addRes.ID)
	}
	return nil, fmt.Errorf("addAndSelect failed")
}

func tbJsonDecode(resp *http.Response, v interface{}) error {
	return json.NewDecoder(resp.Body).Decode(v)
}

func jsonMarshal(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}
