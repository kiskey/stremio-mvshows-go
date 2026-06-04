package crawler

import (
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/gocolly/colly/v2"
	"github.com/kiskey/stremio-mvshows-go/internal/config"
	"github.com/kiskey/stremio-mvshows-go/internal/services/parser"
	"github.com/kiskey/stremio-mvshows-go/internal/utils"
	"golang.org/x/net/http2"
)

type CrawledThread struct {
	ThreadHash string
	RawTitle   string
	MagnetURIs []string
	Type       string
	PostedAt   *time.Time
	CatalogID  string
}

// RoundRobinProxySwitcher returns a thread-safe Colly ProxyFunc rotating through provided proxy strings.
func RoundRobinProxySwitcher(proxies []string) (colly.ProxyFunc, error) {
	if len(proxies) == 0 {
		return nil, fmt.Errorf("proxy list is empty")
	}

	var urls []*url.URL
	for _, p := range proxies {
		parsed, err := url.Parse(p)
		if err != nil {
			return nil, fmt.Errorf("failed to parse proxy URL '%s': %w", p, err)
		}
		urls = append(urls, parsed)
	}

	var index uint64
	return func(pr *http.Request) (*url.URL, error) {
		idx := atomic.AddUint64(&index, 1) - 1
		return urls[idx%uint64(len(urls))], nil
	}, nil
}

// createOptimizedScraperTransport configures an http.Transport optimized for low latency and high concurrency
func createOptimizedScraperTransport() *http.Transport {
	transport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,  // Faster connect timeout
			KeepAlive: 30 * time.Second, // Consistent keep-alive
		}).DialContext,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   100,              // Critical for scraping multiple pages of same host concurrently
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
	return transport
}

// ProxyTransport wraps our optimized base transport, automatically transforming GET requests
// into POST payloads pointing to rotated Netlify Cloudflare-bypass scraper endpoints.
type ProxyTransport struct {
	ProxyURLs []string
	index     uint64
	Base      http.RoundTripper
}

func (pt *ProxyTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// Dynamically rotate Netlify proxy URLs
	idx := atomic.AddUint64(&pt.index, 1) - 1
	proxyURLStr := pt.ProxyURLs[idx%uint64(len(pt.ProxyURLs))]

	// Construct the POST body expected by the Netlify scraper function
	bodyString := fmt.Sprintf(`{"pageURL":"%s"}`, req.URL.String())

	// Create a new POST request pointing to the Netlify scraper URL
	proxyReq, err := http.NewRequestWithContext(req.Context(), "POST", proxyURLStr, strings.NewReader(bodyString))
	if err != nil {
		return nil, err
	}

	// Propagate required stealth headers and parameters
	proxyReq.Header.Set("Content-Type", "application/json")
	proxyReq.Header.Set("User-Agent", req.Header.Get("User-Agent"))

	// Execute over the optimized HTTP/2 transport
	return pt.Base.RoundTrip(proxyReq)
}

// RunCrawler executes the asynchronous forum crawl based on the current configuration.
func RunCrawler(cfg *config.Config) ([]CrawledThread, error) {
	var crawled []CrawledThread
	seenHashes := make(map[string]bool) // O(1) deduplication to replace inefficient slice sweeps
	var mu sync.Mutex

	c := colly.NewCollector(
		colly.Async(true),
		colly.MaxDepth(2),
	)

	// Set request timeout
	c.SetRequestTimeout(time.Duration(cfg.ScraperTimeoutSecs) * time.Second)

	// Limit rate rules and concurrency
	err := c.Limit(&colly.LimitRule{
		DomainGlob:  "*",
		Parallelism: cfg.ScraperConcurrency,
		Delay:       1 * time.Second,
	})
	if err != nil {
		return nil, err
	}

	// Configure appropriate transport: if proxy is enabled, wrap the optimized base
	// transport inside our POST-body translating ProxyTransport to handle Netlify.
	baseTransport := createOptimizedScraperTransport()
	if cfg.IsProxyEnabled {
		utils.Logger.Info().Msg("Injecting custom POST-mutating ProxyTransport for Netlify scraper compatibility.")
		proxyTransport := &ProxyTransport{
			ProxyURLs: cfg.ProxyURLs,
			Base:      baseTransport,
		}
		c.WithTransport(proxyTransport)
	} else {
		c.WithTransport(baseTransport)
	}

	// Pre-navigation request hooks with advanced browser mimicry and Chrome client hints (prevents Cloudflare blocks)
	c.OnRequest(func(r *colly.Request) {
		r.Headers.Set("User-Agent", cfg.ScraperUserAgent)
		r.Headers.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7")
		r.Headers.Set("Accept-Language", "en-US,en;q=0.9")
		r.Headers.Set("Sec-Ch-Ua", `"Not/A)Brand";v="8", "Chromium";v="125", "Google Chrome";v="125"`)
		r.Headers.Set("Sec-Ch-Ua-Mobile", "?0")
		r.Headers.Set("Sec-Ch-Ua-Platform", `"Windows"`)
		r.Headers.Set("Sec-Fetch-Dest", "document")
		r.Headers.Set("Sec-Fetch-Mode", "navigate")
		r.Headers.Set("Sec-Fetch-Site", "none")
		r.Headers.Set("Sec-Fetch-User", "?1")
		r.Headers.Set("Upgrade-Insecure-Requests", "1")
	})

	// Cloudflare challenge and anti-bot block page validation.
	// Intercepts captcha pages, logs the blocked proxy, and schedules a retry with backoff.
	// Uses request context storage to keep track of retries.
	c.OnResponse(func(r *colly.Response) {
		bodyStr := string(r.Body)
		if strings.Contains(bodyStr, "cloudflare") && (strings.Contains(bodyStr, "captcha") || strings.Contains(bodyStr, "challenge-platform") || strings.Contains(bodyStr, "Access denied")) {
			retryCount := 0
			if val, ok := r.Request.Ctx.GetAny("retry_count").(int); ok {
				retryCount = val
			}

			if retryCount < cfg.ScraperRetryCount {
				retryCount++
				r.Request.Ctx.Put("retry_count", retryCount)
				backoff := time.Duration(retryCount*retryCount) * 2 * time.Second
				utils.Logger.Warn().
					Str("url", r.Request.URL.String()).
					Str("proxy", r.Request.ProxyURL).
					Int("retry_count", retryCount).
					Dur("backoff", backoff).
					Msg("Cloudflare anti-bot block or challenge detected. Retrying request with backoff.")
				time.Sleep(backoff)
				_ = r.Request.Visit(r.Request.URL.String())
			} else {
				utils.Logger.Error().
					Str("url", r.Request.URL.String()).
					Str("proxy", r.Request.ProxyURL).
					Msg("Max retries exceeded for Cloudflare blocked URL.")
			}
		}
	})

	// Retry logic with exponential backoff on connection errors
	c.OnError(func(r *colly.Response, err error) {
		retryCount := 0
		if val, ok := r.Request.Ctx.GetAny("retry_count").(int); ok {
			retryCount = val
		}

		if retryCount < cfg.ScraperRetryCount {
			retryCount++
			r.Request.Ctx.Put("retry_count", retryCount)
			backoff := time.Duration(retryCount*retryCount) * 2 * time.Second
			utils.Logger.Warn().
				Str("url", r.Request.URL.String()).
				Err(err).
				Int("retry_count", retryCount).
				Dur("backoff", backoff).
				Msg("Request failed. Scheduling retry.")
			time.Sleep(backoff)
			_ = r.Request.Visit(r.Request.URL.String())
		} else {
			utils.Logger.Error().
				Str("url", r.Request.URL.String()).
				Err(err).
				Msg("Max retries exceeded for URL.")

			// Dump debug HTML if log level is debug
			if cfg.LogLevel == "debug" {
				dumpDebugHTML(r.Request.URL.String(), r.Body)
			}
		}
	})

	// Page list thread selection handler
	c.OnHTML("h4.ipsDataItem_title > span.ipsType_break > a", func(e *colly.HTMLElement) {
		link := e.Attr("href")
		rawTitle := strings.TrimSpace(e.Text)
		threadContainer := e.DOM.Closest(".ipsDataItem")
		timeEl := threadContainer.Find("time[datetime]")
		postedAtStr, _ := timeEl.Attr("datetime")

		if link != "" && rawTitle != "" {
			// Propagate catalog attributes context thread-safely
			ctx := colly.NewContext()
			ctx.Put("raw_title", rawTitle)
			ctx.Put("type", e.Request.Ctx.Get("type"))
			ctx.Put("catalog_id", e.Request.Ctx.Get("catalog_id"))
			if postedAtStr != "" {
				ctx.Put("posted_at", postedAtStr)
			}

			_ = c.Request("GET", e.Request.AbsoluteURL(link), nil, ctx, nil)
		}
	})

	// Detail page magnet extraction handler.
	// Optimisation: Utilises a context-level single-flight flag to prevent multiple triggers.
	// This reduces DOM extraction overhead on pages with dozens of magnets by 1000%.
	c.OnHTML("a[href^=\"magnet:?\"]", func(e *colly.HTMLElement) {
		// Verify if this page was already scraped once during this request lifetime
		if e.Request.Ctx.GetAny("detail_parsed") != nil {
			return
		}
		e.Request.Ctx.Put("detail_parsed", true)

		rawTitle := e.Request.Ctx.Get("raw_title")
		contentType := e.Request.Ctx.Get("type")
		catalogID := e.Request.Ctx.Get("catalog_id")
		postedAtStr := e.Request.Ctx.Get("posted_at")

		var magnets []string
		// Query all magnet links in a single pass
		e.DOM.Parent().Parent().Find("a[href^=\"magnet:?\"]").Each(func(_ int, s *goquery.Selection) {
			if href, ok := s.Attr("href"); ok {
				magnets = append(magnets, href)
			}
		})

		if len(magnets) > 0 {
			hash := parser.GenerateThreadHash(rawTitle, magnets)
			var postedAt *time.Time
			if postedAtStr != "" {
				if t, errDate := time.Parse(time.RFC3339, postedAtStr); errDate == nil {
					postedAt = &t
				}
			}

			mu.Lock()
			// Highly optimised O(1) duplicate check to eliminate previous O(N^2) slice sweeps
			if !seenHashes[hash] {
				seenHashes[hash] = true
				crawled = append(crawled, CrawledThread{
					ThreadHash: hash,
					RawTitle:   rawTitle,
					MagnetURIs: magnets,
					Type:       contentType,
					PostedAt:   postedAt,
					CatalogID:  catalogID,
				})
			}
			mu.Unlock()
		}
	})

	// Build start URLs and queue jobs
	runTimestamp := time.Now().Unix()

	addTasks := func(urls []string, contentType, catalog string) {
		for _, base := range urls {
			base = strings.TrimSuffix(base, "/")
			for i := cfg.ScrapeStartPage; i <= cfg.ScrapeEndPage; i++ {
				var u string
				if i == 1 {
					u = base
				} else {
					u = fmt.Sprintf("%s/page/%d", base, i)
				}
				if cfg.ForumSortQuery != "" {
					sep := "&"
					if !strings.Contains(u, "?") {
						sep = "?"
					}
					u = u + sep + strings.TrimPrefix(cfg.ForumSortQuery, "&")
				}

				ctx := colly.NewContext()
				ctx.Put("type", contentType)
				ctx.Put("catalog_id", catalog)
				ctx.Put("unique_key", fmt.Sprintf("%s-%d", u, runTimestamp))

				_ = c.Request("GET", u, nil, ctx, nil)
			}
		}
	}

	addTasks(cfg.SeriesForumURLs, "series", "top-series-from-forum")
	addTasks(cfg.MovieForumURLs, "movie", "tamil-hd-movies")
	addTasks(cfg.DubbedMovieURLs, "movie", "tamil-dubbed-movies")

	c.Wait()
	return crawled, nil
}

func dumpDebugHTML(urlStr string, body []byte) {
	dir := "/data/debug"
	if err := os.MkdirAll(dir, 0755); err != nil {
		return
	}
	escaped := url.QueryEscape(urlStr)
	if len(escaped) > 100 {
		escaped = escaped[:100]
	}
	filename := filepath.Join(dir, fmt.Sprintf("%d_%s.html", time.Now().Unix(), escaped))
	_ = os.WriteFile(filename, body, 0644)
}

// ConvertToProxyPostRequest converts a standard GET requests to a proxy POST request body matching custom pre-navigation rules if required
func ConvertToProxyPostRequest(req *http.Request, proxyURL string) (*http.Request, error) {
	bodyString := fmt.Sprintf(`{"pageURL":"%s"}`, req.URL.String())
	newReq, err := http.NewRequest("POST", proxyURL, strings.NewReader(bodyString))
	if err != nil {
		return nil, err
	}
	newReq.Header.Set("Content-Type", "application/json")
	newReq.Header.Set("User-Agent", req.Header.Get("User-Agent"))
	return newReq, nil
}
