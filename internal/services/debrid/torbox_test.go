package debrid

import (
	"encoding/json"
	"testing"
	"time"
)

func tbBool(v bool) *bool        { return &v }
func tbNum(v string) json.Number { return json.Number(v) }

func TestShouldPurgeStaleUsesUpdatedAt(t *testing.T) {
	now := time.Date(2026, 9, 4, 20, 0, 0, 0, time.UTC)
	p := &torboxProvider{staleAfter: 48 * time.Hour}
	item := TorboxTorrentItem{
		ID:            tbNum("1"),
		Hash:          "abc",
		DownloadState: "downloading",
		Active:        tbBool(true),
		CreatedAt:     now.Add(-72 * time.Hour).Format(time.RFC3339),
		UpdatedAt:     now.Add(-2 * time.Hour).Format(time.RFC3339),
		DownloadSpeed: 1024,
	}
	if p.shouldPurgeStale(item, now) {
		t.Fatal("recently updated active torrent must not be purged just because created_at is old")
	}
	item.UpdatedAt = now.Add(-49 * time.Hour).Format(time.RFC3339)
	if !p.shouldPurgeStale(item, now) {
		t.Fatal("unfinished download older than stale threshold should be purged")
	}
}

func TestTerminalIncompletePurgesImmediately(t *testing.T) {
	p := &torboxProvider{staleAfter: 48 * time.Hour}
	item := TorboxTorrentItem{ID: tbNum("2"), DownloadState: "incomplete", Active: tbBool(false)}
	if !p.shouldPurgeStale(item, time.Now()) {
		t.Fatal("TorBox incomplete state should be purged")
	}
}

func TestCapacityVictimProtectsCurrentAndRecentRequests(t *testing.T) {
	now := time.Date(2026, 9, 4, 20, 0, 0, 0, time.UTC)
	p := &torboxProvider{recentAdds: map[string]time.Time{"newer": now.Add(-2 * time.Minute)}}
	items := []TorboxTorrentItem{
		{ID: tbNum("10"), Hash: "protected", Active: tbBool(true), DownloadState: "downloading", DownloadSpeed: 10},
		{ID: tbNum("11"), Hash: "newer", Active: tbBool(true), DownloadState: "downloading", DownloadSpeed: 0, CreatedAt: now.Add(-5 * time.Minute).Format(time.RFC3339)},
		{ID: tbNum("12"), Hash: "old-slow", Active: tbBool(true), DownloadState: "downloading", DownloadSpeed: 1, CreatedAt: now.Add(-3 * time.Hour).Format(time.RFC3339)},
	}
	victim := p.selectCapacityVictim(items, "protected", now)
	if victim == nil || victim.Hash != "old-slow" {
		t.Fatalf("expected old-slow victim, got %#v", victim)
	}
}

func TestCapacityVictimFallsBackToOldestRecentWhenAllRecent(t *testing.T) {
	now := time.Date(2026, 9, 4, 20, 0, 0, 0, time.UTC)
	p := &torboxProvider{recentAdds: map[string]time.Time{
		"a": now.Add(-9 * time.Minute),
		"b": now.Add(-2 * time.Minute),
	}}
	items := []TorboxTorrentItem{
		{ID: tbNum("21"), Hash: "a", Active: tbBool(true), DownloadState: "downloading", DownloadSpeed: 3, CreatedAt: now.Add(-20 * time.Minute).Format(time.RFC3339)},
		{ID: tbNum("22"), Hash: "b", Active: tbBool(true), DownloadState: "downloading", DownloadSpeed: 4, CreatedAt: now.Add(-10 * time.Minute).Format(time.RFC3339)},
	}
	victim := p.selectCapacityVictim(items, "", now)
	if victim == nil || victim.Hash != "a" {
		t.Fatalf("expected oldest recent request a to be evicted first, got %#v", victim)
	}
}

func TestFindTorrentByHashDeduplicatesCase(t *testing.T) {
	items := []TorboxTorrentItem{{ID: tbNum("31"), Hash: "ABCDEF"}}
	if got := findTorrentByHash(items, "abcdef"); got == nil || torrentIDOf(*got) != "31" {
		t.Fatal("hash lookup should be case-insensitive")
	}
}
