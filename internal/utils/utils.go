package utils

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"regexp"
	"strings"
	"time"
	"unicode"
)

// NewOptimizedClient creates an HTTP client with high connection reuse, HTTP2, and forced IPv4 loop resolution
func NewOptimizedClient(timeout time.Duration) *http.Client {
	return &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				// Force IPv4 (tcp4) to completely bypass IPv6 DNS/handshake hangs on unprivileged LXC/Docker/Wireguard networks
				return (&net.Dialer{
					Timeout:   5 * time.Second,
					KeepAlive: 30 * time.Second,
				}).DialContext(ctx, "tcp4", addr)
			},
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 20,
			IdleConnTimeout:     90 * time.Second,
			ForceAttemptHTTP2:   true,
			TLSHandshakeTimeout: 5 * time.Second,
		},
	}
}

func Sleep(ms int) {
	time.Sleep(time.Duration(ms) * time.Millisecond)
}

func FormatSize(bytes int64) string {
	if bytes == 0 {
		return "N/A"
	}
	gb := float64(bytes) / 1e9
	return fmt.Sprintf("%.2f GB", gb)
}

var epPatternRegex = regexp.MustCompile(`(?i)(S\d+)?[\s\-_]*\bEP[\s\-_]*[\(\[]?\s*(\d+)\s*[\)\]]?\b`)
var urlRegex = regexp.MustCompile(`\b(https?://\S+|www\.\S+\.\w+|[\w.-]+@[\w.-]+)\b`)
var bracketRegex = regexp.MustCompile(`\[.*?[^\w\s-].*?\]`)

func SanitizeName(name string) string {
	s := name

	// 1. Normalize special unicode spaces (e.g. \u00a0, \u200b) to standard spaces
	s = strings.ReplaceAll(s, "\u00a0", " ")
	s = strings.ReplaceAll(s, "\u200b", " ")

	// 2. Collapse spacing on custom EP representations
	s = epPatternRegex.ReplaceAllString(s, "${1}E${2}")

	// 3. Remove non-ASCII scripts (Chinese, Cyrillic, Japanese, etc.)
	var b strings.Builder
	for _, r := range s {
		if r > unicode.MaxASCII {
			b.WriteRune(' ')
			continue
		}
		b.WriteRune(r)
	}
	s = b.String()

	// 4. Remove residual URLs/domains (e.g. www.BTHDTV.com)
	s = urlRegex.ReplaceAllString(s, " ")

	// 5. Remove residual empty/garbage brackets
	s = bracketRegex.ReplaceAllString(s, " ")

	s = strings.Join(strings.Fields(s), " ")

	// 6. Trim leftover leading/trailing punctuation
	s = strings.TrimLeft(s, " .-_[]()/\\")
	s = strings.TrimRight(s, " .-_[]()/\\")
	return s
}

var QualityOrder = map[string]int{
	"4k":    1,
	"2160p": 1,
	"1080p": 2,
	"720p":  3,
	"480p":  4,
	"360p":  5,
	"sd":    6,
}

func GetQuality(resolution string) string {
	if resolution == "" {
		return "sd"
	}
	res := strings.ToLower(resolution)
	if strings.Contains(res, "2160") || strings.Contains(res, "4k") {
		return "4k"
	}
	if strings.Contains(res, "1080") {
		return "1080p"
	}
	if strings.Contains(res, "720") {
		return "720p"
	}
	if strings.Contains(res, "480") {
		return "480p"
	}
	if strings.Contains(res, "360") {
		return "360p"
	}
	return "sd"
}
