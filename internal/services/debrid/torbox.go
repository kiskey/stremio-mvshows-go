// Version: 2.2.0
// Change log: Updated TorBox maintenance/admission flow for current API deletion semantics, configurable active-slot enforcement, stale cleanup, duplicate suppression, newest-request priority, and cycle-safe self-healing.

package debrid

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/kiskey/stremio-mvshows-go/internal/config"
	"github.com/kiskey/stremio-mvshows-go/internal/utils"
)

const torboxRecentRequestGrace = 10 * time.Minute

type torboxProvider struct {
	client        *http.Client
	apiKey        string
	enabled       bool
	cache         CacheStore
	selections    map[string]map[int]bool
	recentAdds    map[string]time.Time
	recentIDs     map[string]string
	addTimestamps []time.Time
	maxActive     int
	staleAfter    time.Duration
	mu            sync.Mutex
	admissionMu   sync.Mutex
}

type CacheStore interface {
	Get(ctx context.Context, hash string) (map[string]interface{}, error)
	Set(ctx context.Context, hash string, data map[string]interface{}) error
	Update(ctx context.Context, hash string, updates map[string]interface{}) error
	GetByProviderID(ctx context.Context, id string) (map[string]interface{}, error)
}

type TorboxCreateResponse struct {
	Success bool            `json:"success"`
	Detail  string          `json:"detail"`
	Error   json.RawMessage `json:"error"`
	Data    struct {
		TorrentID json.Number `json:"torrent_id"`
		ID        json.Number `json:"id"`
		Hash      string      `json:"hash"`
		Name      string      `json:"name"`
	} `json:"data"`
}

type TorboxActionResponse struct {
	Success bool            `json:"success"`
	Detail  string          `json:"detail"`
	Error   json.RawMessage `json:"error"`
}

type TorboxTorrentItem struct {
	ID               json.Number `json:"id"`
	TorrentID        json.Number `json:"torrent_id"`
	Hash             string      `json:"hash"`
	Name             string      `json:"name"`
	Status           string      `json:"status"`
	DownloadState    string      `json:"download_state"`
	Active           *bool       `json:"active"`
	DownloadFinished *bool       `json:"download_finished"`
	DownloadPresent  *bool       `json:"download_present"`
	DownloadSpeed    float64     `json:"download_speed"`
	Progress         float64     `json:"progress"`
	Seeds            float64     `json:"seeds"`
	Peers            float64     `json:"peers"`
	InactiveCheck    float64     `json:"inactive_check"`
	CreatedAt        string      `json:"created_at"`
	UpdatedAt        string      `json:"updated_at"`
	ExpiresAt        string      `json:"expires_at"`
	Files            []struct {
		ID   json.Number `json:"id"`
		Name string      `json:"name"`
		Size int64       `json:"size"`
	} `json:"files"`
}

type TorboxMyListResponse struct {
	Success bool            `json:"success"`
	Detail  string          `json:"detail"`
	Error   json.RawMessage `json:"error"`
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
	Success bool                        `json:"success"`
	Data    map[string]TorboxCachedItem `json:"data"`
}

type TorboxRequestDlResponse struct {
	Success bool            `json:"success"`
	Detail  string          `json:"detail"`
	Error   json.RawMessage `json:"error"`
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
	URLCache.ttl = 1 * time.Second
	TorrentInfoCache.ttl = 5 * time.Second

	maxActive := cfg.TorboxMaxActiveDownloads
	if maxActive < 1 {
		maxActive = 3
	}
	staleHours := cfg.TorboxStaleDownloadHours
	if staleHours < 1 {
		staleHours = 48
	}

	return &torboxProvider{
		client:        utils.NewOptimizedClient(15 * time.Second),
		apiKey:        cfg.TorboxAPIKey,
		enabled:       cfg.IsTorboxEnabled,
		cache:         cache,
		selections:    make(map[string]map[int]bool),
		recentAdds:    make(map[string]time.Time),
		recentIDs:     make(map[string]string),
		addTimestamps: []time.Time{},
		maxActive:     maxActive,
		staleAfter:    time.Duration(staleHours) * time.Hour,
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
	lower := strings.ToLower(magnet)
	idx := strings.Index(lower, "btih:")
	if idx == -1 {
		return ""
	}
	start := idx + 5
	end := start
	for end < len(magnet) {
		c := magnet[end]
		if c == '&' || c == '/' || c == '#' {
			break
		}
		end++
	}
	if end <= start {
		return ""
	}
	return normalizeInfoHash(magnet[start:end])
}

func normalizeInfoHash(hash string) string {
	return strings.ToLower(strings.TrimSpace(hash))
}

func (t *torboxProvider) checkRateLimit() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := time.Now()
	cutoff := now.Add(-60 * time.Second)
	kept := t.addTimestamps[:0]
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

func (t *torboxProvider) AddMagnet(ctx context.Context, magnet string) (*AddResult, error) {
	hash := extractInfoHash(magnet)
	if hash == "" {
		return nil, fmt.Errorf("invalid magnet link")
	}

	t.admissionMu.Lock()
	defer t.admissionMu.Unlock()
	return t.ensureRequestedTorrentLocked(ctx, magnet, hash)
}

func (t *torboxProvider) ensureRequestedTorrentLocked(ctx context.Context, magnet, hash string) (*AddResult, error) {
	items, err := t.getTorrentListFresh(ctx)
	if err != nil {
		return nil, fmt.Errorf("torbox admission list: %w", err)
	}

	if existing := findTorrentByHash(items, hash); existing != nil {
		if t.shouldReplaceRequestedTorrent(*existing, time.Now()) {
			utils.Logger.Info().
				Str("torrent_id", torrentIDOf(*existing)).
				Str("hash", hash).
				Str("state", torrentState(*existing)).
				Msg("Replacing stale/failed TorBox instance for current playback request.")
			if err := t.deleteTorrentLocked(ctx, torrentIDOf(*existing)); err != nil {
				return nil, fmt.Errorf("delete stale requested torrent: %w", err)
			}
			items, err = t.getTorrentListFresh(ctx)
			if err != nil {
				return nil, fmt.Errorf("torbox post-delete list: %w", err)
			}
		} else {
			id := torrentIDOf(*existing)
			t.rememberTorrent(hash, id, true)
			return &AddResult{ID: id, Hash: hash, Name: existing.Name, Cached: torrentFinished(*existing)}, nil
		}
	}

	if _, err := t.cleanupStaleItemsLocked(ctx, items, hash); err != nil {
		return nil, err
	}

	items, err = t.getTorrentListFresh(ctx)
	if err != nil {
		return nil, fmt.Errorf("torbox admission refresh: %w", err)
	}

	// A concurrent external actor may have added the same hash while cleanup was running.
	if existing := findTorrentByHash(items, hash); existing != nil {
		id := torrentIDOf(*existing)
		t.rememberTorrent(hash, id, true)
		return &AddResult{ID: id, Hash: hash, Name: existing.Name, Cached: torrentFinished(*existing)}, nil
	}

	evictionAttempts := 0
	for countActiveTorrents(items) >= t.maxActive {
		evictionAttempts++
		if evictionAttempts > len(items)+1 {
			return nil, fmt.Errorf("torbox capacity admission aborted after %d eviction attempts", evictionAttempts-1)
		}
		activeBefore := countActiveTorrents(items)
		victim := t.selectCapacityVictim(items, hash, time.Now())
		if victim == nil {
			return nil, fmt.Errorf("torbox active download limit %d reached and no safe victim is available", t.maxActive)
		}

		victimID := torrentIDOf(*victim)
		utils.Logger.Info().
			Str("victim_id", victimID).
			Str("victim_hash", normalizeInfoHash(victim.Hash)).
			Str("victim_name", victim.Name).
			Str("state", torrentState(*victim)).
			Float64("download_speed", victim.DownloadSpeed).
			Float64("progress", victim.Progress).
			Int("configured_active_limit", t.maxActive).
			Msg("TorBox active limit reached; evicting older/lower-priority download for newest playback request.")

		if err := t.deleteTorrentLocked(ctx, victimID); err != nil {
			return nil, fmt.Errorf("torbox capacity eviction failed: %w", err)
		}

		items, err = t.getTorrentListFresh(ctx)
		if err != nil {
			return nil, fmt.Errorf("torbox capacity verification list: %w", err)
		}
		if findTorrentByID(items, victimID) != nil && countActiveTorrents(items) >= activeBefore {
			return nil, fmt.Errorf("torbox delete for victim %s was acknowledged but not reflected by fresh mylist; refusing a repeat delete/add cycle", victimID)
		}

		if existing := findTorrentByHash(items, hash); existing != nil {
			id := torrentIDOf(*existing)
			t.rememberTorrent(hash, id, true)
			return &AddResult{ID: id, Hash: hash, Name: existing.Name, Cached: torrentFinished(*existing)}, nil
		}
	}

	result, err := t.createMagnetLocked(ctx, magnet, hash)
	if err != nil {
		return nil, err
	}
	t.rememberTorrent(hash, result.ID, true)
	return result, nil
}

func (t *torboxProvider) createMagnetLocked(ctx context.Context, magnet, requestedHash string) (*AddResult, error) {
	if !t.checkRateLimit() {
		return nil, fmt.Errorf("torbox addMagnet rate limit exceeded")
	}

	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		var body bytes.Buffer
		writer := multipart.NewWriter(&body)
		if err := writer.WriteField("magnet", magnet); err != nil {
			return nil, err
		}
		// Interactive Stremio playback must get an active slot immediately; never queue it.
		if err := writer.WriteField("as_queued", "false"); err != nil {
			return nil, err
		}
		if err := writer.Close(); err != nil {
			return nil, err
		}

		resp, err := t.do(ctx, http.MethodPost, "/torrents/createtorrent", &body, writer.FormDataContentType())
		if err != nil {
			lastErr = err
			if existing := t.findExistingAfterAmbiguousCreate(ctx, requestedHash); existing != nil {
				id := torrentIDOf(*existing)
				return &AddResult{ID: id, Hash: requestedHash, Name: existing.Name, Cached: torrentFinished(*existing)}, nil
			}
			continue
		}

		var data TorboxCreateResponse
		decodeErr := json.NewDecoder(resp.Body).Decode(&data)
		statusCode := resp.StatusCode
		resp.Body.Close()
		if decodeErr != nil {
			lastErr = fmt.Errorf("torbox create decode: %w", decodeErr)
			if existing := t.findExistingAfterAmbiguousCreate(ctx, requestedHash); existing != nil {
				id := torrentIDOf(*existing)
				return &AddResult{ID: id, Hash: requestedHash, Name: existing.Name, Cached: torrentFinished(*existing)}, nil
			}
			continue
		}
		if statusCode < 200 || statusCode >= 300 || !data.Success {
			lastErr = fmt.Errorf("torbox create failed status=%d detail=%q error=%s", statusCode, data.Detail, compactRawJSON(data.Error))
			if existing := t.findExistingAfterAmbiguousCreate(ctx, requestedHash); existing != nil {
				id := torrentIDOf(*existing)
				return &AddResult{ID: id, Hash: requestedHash, Name: existing.Name, Cached: torrentFinished(*existing)}, nil
			}
			continue
		}

		id := data.Data.TorrentID.String()
		if id == "" || id == "0" {
			id = data.Data.ID.String()
		}
		if id == "" || id == "0" {
			lastErr = fmt.Errorf("torbox create response missing torrent id")
			continue
		}

		hash := normalizeInfoHash(data.Data.Hash)
		if hash == "" {
			hash = requestedHash
		}
		isCached := strings.Contains(strings.ToLower(data.Detail), "cached")
		return &AddResult{ID: id, Hash: hash, Name: data.Data.Name, Cached: isCached}, nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("torbox create failed")
	}
	return nil, lastErr
}

func (t *torboxProvider) findExistingAfterAmbiguousCreate(ctx context.Context, hash string) *TorboxTorrentItem {
	items, err := t.getTorrentListFresh(ctx)
	if err != nil {
		return nil
	}
	return findTorrentByHash(items, hash)
}

func (t *torboxProvider) GetTorrentInfo(ctx context.Context, id string) (*TorrentInfo, error) {
	resp, err := t.do(ctx, http.MethodGet, "/torrents/mylist?id="+id, nil, "")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrResourceNotFound
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("gettorrentinfo status %d", resp.StatusCode)
	}

	var data TorboxMyListResponse
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return nil, err
	}
	items, err := decodeTorrentItems(data.Data)
	if err != nil {
		return nil, err
	}
	if len(items) == 0 {
		return nil, ErrResourceNotFound
	}
	return mapTBInfo(id, items[0], t.selections), nil
}

func mapTBInfo(id string, item TorboxTorrentItem, selections map[string]map[int]bool) *TorrentInfo {
	info := &TorrentInfo{ID: id, Filename: item.Name}
	if torrentFinished(item) {
		info.Status = "downloaded"
	} else {
		info.Status = mapTBStatus(torrentState(item))
	}

	selectedSet := selections[id]
	if selectedSet == nil {
		selectedSet = make(map[int]bool)
	}
	for i, f := range item.Files {
		fid := jsonNumberToInt(f.ID, i)
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
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "cached", "downloaded":
		return "downloaded"
	case "completed", "uploading", "seeding", "active":
		return "downloading"
	case "downloading", "metadl", "checkingresumedata", "stalled", "stalled (no seeds)", "queued", "paused":
		return "downloading"
	case "error", "failed", "failed (processing)", "missingfiles", "expired", "incomplete":
		return "error"
	default:
		return "downloading"
	}
}

func (t *torboxProvider) SelectFiles(ctx context.Context, id string, fileIDs []string) error {
	resp, err := t.do(ctx, http.MethodGet, "/torrents/mylist?id="+id, nil, "")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("selectfiles status %d", resp.StatusCode)
	}

	var data TorboxMyListResponse
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return err
	}
	items, err := decodeTorrentItems(data.Data)
	if err != nil {
		return err
	}
	if len(items) == 0 {
		return fmt.Errorf("torrent not found")
	}

	set := make(map[int]bool)
	if len(fileIDs) == 1 && fileIDs[0] == "all" {
		for i, f := range items[0].Files {
			set[jsonNumberToInt(f.ID, i)] = true
		}
	} else {
		for _, fid := range fileIDs {
			fileID, err := strconv.Atoi(fid)
			if err == nil {
				set[fileID] = true
			}
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
	t.admissionMu.Lock()
	defer t.admissionMu.Unlock()
	return t.deleteTorrentLocked(ctx, id)
}

func (t *torboxProvider) deleteTorrentLocked(ctx context.Context, id string) error {
	torrentID, err := strconv.ParseInt(id, 10, 64)
	if err != nil || torrentID <= 0 {
		return fmt.Errorf("invalid torbox torrent id %q", id)
	}

	payload, err := json.Marshal(map[string]interface{}{
		"torrent_id": torrentID,
		"operation":  "delete",
		"all":        false,
	})
	if err != nil {
		return err
	}

	resp, err := t.do(ctx, http.MethodPost, "/torrents/controltorrent", bytes.NewReader(payload), "application/json")
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	var data TorboxActionResponse
	decodeErr := json.NewDecoder(resp.Body).Decode(&data)
	if decodeErr != nil {
		return fmt.Errorf("torbox delete decode status=%d: %w", resp.StatusCode, decodeErr)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 || !data.Success {
		return fmt.Errorf("torbox delete failed status=%d detail=%q error=%s", resp.StatusCode, data.Detail, compactRawJSON(data.Error))
	}

	t.forgetTorrentID(id)
	return nil
}

func (t *torboxProvider) GetTorrents(ctx context.Context) ([]Torrent, error) {
	items, err := t.getTorrentList(ctx, false)
	if err != nil {
		return nil, err
	}
	var torrents []Torrent
	for _, item := range items {
		torrents = append(torrents, Torrent{
			ID:     torrentIDOf(item),
			Hash:   item.Hash,
			Name:   item.Name,
			Status: torrentState(item),
		})
	}
	return torrents, nil
}

func (t *torboxProvider) CheckCached(ctx context.Context, hashes []string) (map[string]CacheStatus, error) {
	if len(hashes) == 0 {
		return map[string]CacheStatus{}, nil
	}
	bodyMap := map[string]interface{}{"hashes": hashes}
	b, _ := json.Marshal(bodyMap)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.torbox.app/v1/api/torrents/checkcached?format=object&list_files=true", bytes.NewReader(b))
	req.Header.Set("Authorization", "Bearer "+t.apiKey)
	req.Header.Set("Content-Type", "application/json")
	resp, err := t.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
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
			cs := CacheStatus{Cached: true, TorrentID: val.ID.String(), Name: val.Name, Size: val.Size}
			for i, f := range val.Files {
				cs.Files = append(cs.Files, CacheFile{ID: jsonNumberToInt(f.ID, i), Name: f.Name, Size: f.Size})
			}
			if cs.TorrentID != "" && cs.TorrentID != "0" {
				t.rememberTorrent(normalizeInfoHash(h), cs.TorrentID, false)
			}
			result[h] = cs
		} else {
			result[h] = CacheStatus{Cached: false}
		}
	}
	return result, nil
}

func (t *torboxProvider) GetDownloadLinkForFile(ctx context.Context, torrentID, fileID string) (string, error) {
	return t.getDownloadLinkForFile(ctx, torrentID, fileID, true)
}

func (t *torboxProvider) getDownloadLinkForFile(ctx context.Context, torrentID, fileID string, allowHeal bool) (string, error) {
	fileName := "unknown"
	if info, err := t.GetTorrentInfo(ctx, torrentID); err == nil && info != nil {
		targetFID, _ := strconv.Atoi(fileID)
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
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	req.Header.Set("Authorization", "Bearer "+t.apiKey)
	resp, err := t.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("torbox requestdl transport: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		status := resp.StatusCode
		if !allowHeal {
			return "", fmt.Errorf("torbox requestdl status %d", status)
		}
		return t.healMissingTorrentOnce(ctx, torrentID, fileID, status)
	}

	var data TorboxRequestDlResponse
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return "", err
	}
	if !data.Success {
		// A logical API error is not proof that the torrent is gone. Verify presence before any re-add.
		if allowHeal {
			return t.healMissingTorrentOnce(ctx, torrentID, fileID, http.StatusOK)
		}
		return "", fmt.Errorf("torbox requestdl failed detail=%q error=%s", data.Detail, compactRawJSON(data.Error))
	}

	var link string
	dataStr := strings.TrimSpace(string(data.Data))
	if strings.HasPrefix(dataStr, "\"") {
		_ = json.Unmarshal(data.Data, &link)
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
	return "", fmt.Errorf("torbox requestdl returned no download url")
}

func (t *torboxProvider) healMissingTorrentOnce(ctx context.Context, torrentID, fileID string, requestStatus int) (string, error) {
	items, listErr := t.getTorrentListFresh(ctx)
	if listErr != nil {
		return "", fmt.Errorf("torbox requestdl status=%d; presence verification failed: %w", requestStatus, listErr)
	}
	if existing := findTorrentByID(items, torrentID); existing != nil {
		// The torrent still exists. Do not create a duplicate on rate limits, transient failures, or link errors.
		return "", fmt.Errorf("torbox requestdl status=%d while torrent %s is still present; refusing duplicate re-add", requestStatus, torrentID)
	}

	infoHash := t.recentHashForID(torrentID)
	if infoHash == "" {
		return "", fmt.Errorf("torbox requestdl status=%d and torrent %s is absent, but its infohash is unavailable for one-shot recovery", requestStatus, torrentID)
	}

	utils.Logger.Info().
		Str("old_torrent_id", torrentID).
		Str("hash", infoHash).
		Msg("TorBox torrent confirmed absent; performing one-shot self-healing re-add.")

	magnet := fmt.Sprintf("magnet:?xt=urn:btih:%s", infoHash)
	newInfo, err := t.AddAndSelect(ctx, magnet)
	if err != nil {
		return "", fmt.Errorf("torbox one-shot self-heal failed: %w", err)
	}
	if newInfo == nil || newInfo.ID == "" {
		return "", fmt.Errorf("torbox one-shot self-heal returned no torrent id")
	}
	return t.getDownloadLinkForFile(ctx, newInfo.ID, fileID, false)
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
			return &FileInfo{ID: f.ID, Path: f.Name, Bytes: f.Size}, nil
		}
	}
	return nil, nil
}

func (t *torboxProvider) CleanupStaleTorrents(ctx context.Context) (int, error) {
	if !t.IsEnabled() {
		return 0, nil
	}
	t.admissionMu.Lock()
	defer t.admissionMu.Unlock()

	items, err := t.getTorrentListFresh(ctx)
	if err != nil {
		return 0, err
	}
	return t.cleanupStaleItemsLocked(ctx, items, "")
}

func (t *torboxProvider) cleanupStaleItemsLocked(ctx context.Context, items []TorboxTorrentItem, protectedHash string) (int, error) {
	protectedHash = normalizeInfoHash(protectedHash)
	now := time.Now()
	purged := 0

	for _, item := range items {
		if protectedHash != "" && normalizeInfoHash(item.Hash) == protectedHash {
			continue
		}
		if !t.shouldPurgeStale(item, now) {
			continue
		}
		id := torrentIDOf(item)
		if id == "" || id == "0" {
			continue
		}

		utils.Logger.Info().
			Str("torrent_id", id).
			Str("name", item.Name).
			Str("state", torrentState(item)).
			Float64("progress", item.Progress).
			Dur("inactive_age", torrentInactiveAge(item, now)).
			Msg("Purging stale/incomplete TorBox torrent download.")

		if err := t.deleteTorrentLocked(ctx, id); err != nil {
			utils.Logger.Warn().Err(err).Str("torrent_id", id).Msg("Failed to delete stale TorBox torrent.")
			continue
		}
		purged++
	}

	if purged > 0 {
		utils.Logger.Info().Int("purged_count", purged).Msg("TorBox stale torrent cleanup completed.")
	}
	return purged, nil
}

func (t *torboxProvider) AddAndSelect(ctx context.Context, magnet string) (*TorrentInfo, error) {
	hash := extractInfoHash(magnet)
	if hash == "" {
		return nil, fmt.Errorf("invalid magnet link")
	}

	addRes, err := func() (*AddResult, error) {
		t.admissionMu.Lock()
		defer t.admissionMu.Unlock()
		return t.ensureRequestedTorrentLocked(ctx, magnet, hash)
	}()
	if err != nil {
		return nil, err
	}
	if addRes == nil || addRes.ID == "" {
		return nil, fmt.Errorf("addAndSelect failed")
	}
	if err := t.SelectFiles(ctx, addRes.ID, []string{"all"}); err != nil {
		return nil, err
	}
	return t.GetTorrentInfo(ctx, addRes.ID)
}

func (t *torboxProvider) getTorrentListFresh(ctx context.Context) ([]TorboxTorrentItem, error) {
	return t.getTorrentList(ctx, true)
}

func (t *torboxProvider) getTorrentList(ctx context.Context, bypassCache bool) ([]TorboxTorrentItem, error) {
	path := "/torrents/mylist"
	if bypassCache {
		path += "?bypass_cache=true"
	}
	resp, err := t.do(ctx, http.MethodGet, path, nil, "")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("torbox mylist status %d", resp.StatusCode)
	}

	var data TorboxMyListResponse
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return nil, err
	}
	if !data.Success && len(data.Data) == 0 {
		return nil, fmt.Errorf("torbox mylist failed detail=%q error=%s", data.Detail, compactRawJSON(data.Error))
	}
	return decodeTorrentItems(data.Data)
}

func decodeTorrentItems(raw json.RawMessage) ([]TorboxTorrentItem, error) {
	dataStr := strings.TrimSpace(string(raw))
	if dataStr == "" || dataStr == "null" {
		return nil, nil
	}
	if strings.HasPrefix(dataStr, "[") {
		var items []TorboxTorrentItem
		if err := json.Unmarshal(raw, &items); err != nil {
			return nil, err
		}
		return items, nil
	}
	if strings.HasPrefix(dataStr, "{") {
		var single TorboxTorrentItem
		if err := json.Unmarshal(raw, &single); err != nil {
			return nil, err
		}
		return []TorboxTorrentItem{single}, nil
	}
	return nil, fmt.Errorf("unexpected torbox mylist data shape")
}

func torrentIDOf(item TorboxTorrentItem) string {
	id := item.TorrentID.String()
	if id == "" || id == "0" {
		id = item.ID.String()
	}
	return id
}

func torrentState(item TorboxTorrentItem) string {
	state := strings.TrimSpace(item.DownloadState)
	if state == "" {
		state = strings.TrimSpace(item.Status)
	}
	return strings.ToLower(state)
}

func torrentFinished(item TorboxTorrentItem) bool {
	if item.DownloadFinished != nil {
		return *item.DownloadFinished
	}
	switch torrentState(item) {
	case "completed", "cached", "downloaded", "seeding", "uploading":
		return true
	}
	return item.Progress >= 100.0
}

func torrentActive(item TorboxTorrentItem) bool {
	if item.Active != nil {
		return *item.Active
	}
	switch torrentState(item) {
	case "downloading", "metadl", "checkingresumedata", "stalled", "stalled (no seeds)", "uploading", "seeding", "active":
		return true
	default:
		return false
	}
}

func countActiveTorrents(items []TorboxTorrentItem) int {
	count := 0
	for _, item := range items {
		if torrentActive(item) {
			count++
		}
	}
	return count
}

func findTorrentByHash(items []TorboxTorrentItem, hash string) *TorboxTorrentItem {
	hash = normalizeInfoHash(hash)
	for i := range items {
		if normalizeInfoHash(items[i].Hash) == hash {
			return &items[i]
		}
	}
	return nil
}

func findTorrentByID(items []TorboxTorrentItem, id string) *TorboxTorrentItem {
	for i := range items {
		if torrentIDOf(items[i]) == id {
			return &items[i]
		}
	}
	return nil
}

func (t *torboxProvider) shouldReplaceRequestedTorrent(item TorboxTorrentItem, now time.Time) bool {
	if torrentFinished(item) {
		return false
	}
	state := torrentState(item)
	switch state {
	case "expired", "error", "failed", "failed (processing)", "missingfiles", "incomplete":
		return true
	}
	return t.shouldPurgeStale(item, now)
}

func (t *torboxProvider) shouldPurgeStale(item TorboxTorrentItem, now time.Time) bool {
	if torrentFinished(item) {
		return false
	}
	state := torrentState(item)
	switch state {
	case "expired", "error", "failed", "failed (processing)", "missingfiles", "incomplete":
		return true
	}
	age := torrentInactiveAge(item, now)
	if age < t.staleAfter {
		return false
	}
	if !torrentActive(item) {
		return true
	}
	switch state {
	case "stalled", "stalled (no seeds)", "paused", "queued", "downloading", "metadl", "checkingresumedata":
		return true
	}
	return item.DownloadSpeed <= 0
}

func torrentInactiveAge(item TorboxTorrentItem, now time.Time) time.Duration {
	if ts, ok := parseTorboxTime(item.UpdatedAt); ok {
		return nonNegativeDuration(now.Sub(ts))
	}
	if ts, ok := parseTorboxTime(item.CreatedAt); ok {
		return nonNegativeDuration(now.Sub(ts))
	}
	return 0
}

func torrentCreatedTime(item TorboxTorrentItem) time.Time {
	if ts, ok := parseTorboxTime(item.CreatedAt); ok {
		return ts
	}
	if ts, ok := parseTorboxTime(item.UpdatedAt); ok {
		return ts
	}
	return time.Time{}
}

func parseTorboxTime(value string) (time.Time, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}, false
	}
	layouts := []string{time.RFC3339Nano, time.RFC3339, "2006-01-02 15:04:05"}
	for _, layout := range layouts {
		if ts, err := time.Parse(layout, value); err == nil {
			return ts, true
		}
	}
	return time.Time{}, false
}

func nonNegativeDuration(d time.Duration) time.Duration {
	if d < 0 {
		return 0
	}
	return d
}

func (t *torboxProvider) selectCapacityVictim(items []TorboxTorrentItem, protectedHash string, now time.Time) *TorboxTorrentItem {
	protectedHash = normalizeInfoHash(protectedHash)
	candidates := make([]TorboxTorrentItem, 0, len(items))
	for _, item := range items {
		if !torrentActive(item) {
			continue
		}
		if protectedHash != "" && normalizeInfoHash(item.Hash) == protectedHash {
			continue
		}
		candidates = append(candidates, item)
	}
	if len(candidates) == 0 {
		return nil
	}

	t.mu.Lock()
	recentSnapshot := make(map[string]time.Time, len(t.recentAdds))
	for hash, ts := range t.recentAdds {
		recentSnapshot[hash] = ts
	}
	t.mu.Unlock()

	sort.SliceStable(candidates, func(i, j int) bool {
		a, b := candidates[i], candidates[j]
		aHash := normalizeInfoHash(a.Hash)
		bHash := normalizeInfoHash(b.Hash)
		aRequestedAt, aHasRequest := recentSnapshot[aHash]
		bRequestedAt, bHasRequest := recentSnapshot[bHash]
		aRecent := aHasRequest && requestTimeIsRecent(aRequestedAt, now)
		bRecent := bHasRequest && requestTimeIsRecent(bRequestedAt, now)
		if aRecent != bRecent {
			return !aRecent // protect recently requested torrents from churn whenever an older victim exists
		}
		if aRecent && bRecent && !aRequestedAt.Equal(bRequestedAt) {
			return aRequestedAt.Before(bRequestedAt) // among recent requests, the newest request has the highest priority
		}

		aClass := capacityVictimClass(a)
		bClass := capacityVictimClass(b)
		if aClass != bClass {
			return aClass < bClass
		}
		if a.DownloadSpeed != b.DownloadSpeed {
			return a.DownloadSpeed < b.DownloadSpeed
		}

		aCreated := torrentCreatedTime(a)
		bCreated := torrentCreatedTime(b)
		if !aCreated.Equal(bCreated) {
			if aCreated.IsZero() {
				return false
			}
			if bCreated.IsZero() {
				return true
			}
			return aCreated.Before(bCreated)
		}
		if a.Progress != b.Progress {
			return a.Progress < b.Progress
		}
		return torrentIDOf(a) < torrentIDOf(b)
	})
	victim := candidates[0]
	return &victim
}

func capacityVictimClass(item TorboxTorrentItem) int {
	if torrentFinished(item) {
		return 0
	}
	switch torrentState(item) {
	case "expired", "error", "failed", "failed (processing)", "missingfiles", "incomplete", "stalled", "stalled (no seeds)", "paused":
		return 1
	}
	if item.DownloadSpeed <= 0 || item.Seeds <= 0 {
		return 2
	}
	return 3
}

func requestTimeIsRecent(ts time.Time, now time.Time) bool {
	age := now.Sub(ts)
	return age >= 0 && age < torboxRecentRequestGrace
}

func (t *torboxProvider) rememberTorrent(hash, id string, prioritize bool) {
	hash = normalizeInfoHash(hash)
	if hash == "" || id == "" || id == "0" {
		return
	}
	t.mu.Lock()
	t.recentIDs[hash] = id
	if prioritize {
		t.recentAdds[hash] = time.Now()
	}
	t.mu.Unlock()
}

func (t *torboxProvider) forgetTorrentID(id string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for hash, rid := range t.recentIDs {
		if rid == id {
			delete(t.recentIDs, hash)
			delete(t.recentAdds, hash)
		}
	}
	delete(t.selections, id)
}

func (t *torboxProvider) recentHashForID(id string) string {
	t.mu.Lock()
	defer t.mu.Unlock()
	for hash, rid := range t.recentIDs {
		if rid == id {
			return hash
		}
	}
	return ""
}

func jsonNumberToInt(n json.Number, fallback int) int {
	if n == "" {
		return fallback
	}
	if v, err := n.Int64(); err == nil {
		return int(v)
	}
	if v, err := strconv.Atoi(n.String()); err == nil {
		return v
	}
	return fallback
}

func compactRawJSON(raw json.RawMessage) string {
	if len(raw) == 0 {
		return "null"
	}
	return strings.TrimSpace(string(raw))
}
