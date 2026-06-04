package tracker

import (
	"crypto/tls"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/kiskey/stremio-mvshows-go/internal/config"
	"github.com/kiskey/stremio-mvshows-go/internal/utils"
	"golang.org/x/net/http2"
)

var (
	cachedTrackers []string
	mu             sync.RWMutex
)

var fallbackTrackers = []string{
	"udp://tracker.opentrackr.org:1337/announce",
	"udp://tracker.openbittorrent.com:6969/announce",
	"udp://tracker.dler.org:6969/announce",
	"udp://open.stealth.si:80/announce",
	"udp://opentracker.i2p.rocks:6969/announce",
}

// createOptimizedTrackerHTTPClient configures an transport optimized for low latency and high concurrency
func createOptimizedTrackerHTTPClient(timeout time.Duration) *http.Client {
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

// FetchAndCacheTrackers retrieves the best trackers list from public URL and updates the cache.
func FetchAndCacheTrackers(cfg *config.Config) {
	httpClient := createOptimizedTrackerHTTPClient(10 * time.Second)
	client := resty.NewWithClient(httpClient)
	resp, err := client.R().Get(cfg.TrackerURL)
	if err != nil {
		utils.Logger.Error().Err(err).Msg("Failed to fetch trackers list. Retaining fallback/previous trackers.")
		mu.Lock()
		if len(cachedTrackers) == 0 {
			cachedTrackers = append([]string(nil), fallbackTrackers...)
		}
		mu.Unlock()
		return
	}

	body := strings.TrimSpace(resp.String())
	if body == "" || resp.StatusCode() != 200 {
		utils.Logger.Warn().Int("status", resp.StatusCode()).Msg("Tracker endpoint returned empty/non-200. Retaining previous trackers.")
		mu.Lock()
		if len(cachedTrackers) == 0 {
			cachedTrackers = append([]string(nil), fallbackTrackers...)
		}
		mu.Unlock()
		return
	}

	lines := strings.Split(body, "\n")
	var trackers []string
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line != "" && !strings.HasPrefix(line, "#") {
			trackers = append(trackers, line)
		}
	}

	mu.Lock()
	if len(trackers) > 0 {
		cachedTrackers = trackers
		utils.Logger.Info().Int("count", len(trackers)).Msg("Trackers cached successfully.")
	} else if len(cachedTrackers) == 0 {
		cachedTrackers = append([]string(nil), fallbackTrackers...)
		utils.Logger.Warn().Msg("No valid trackers found in response. Using hardcoded fallbacks.")
	}
	mu.Unlock()
}

// GetTrackers returns a copy of currently cached tracker list safely.
func GetTrackers() []string {
	mu.RLock()
	defer mu.RUnlock()
	if len(cachedTrackers) == 0 {
		return append([]string(nil), fallbackTrackers...)
	}
	return append([]string(nil), cachedTrackers...)
}
