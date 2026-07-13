// Version: 1.1.0
// Change log: Fixed fragile Parent().Parent() selector with stable ancestor search containers and introduced deep magnet deduplication inside detail extraction rules. Added RunTargetedCrawler to perform safe proxy-aware single-page thread detail scrapes without affecting scheduling catalogs.

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
	idx := atomic.AddUint64(&pt.index, 1) - 1
	proxyURLStr := pt.ProxyURLs[idx%uint64(len(pt.ProxyURLs))]

	bodyString := fmt.Sprintf(`{"pageURL":"%s"}`, req.URL.String())

	proxyReq, err := http.NewRequestWithContext(req.Context(), "POST", proxyURLStr, strings.NewReader(bodyString))
	if err != nil {
		return nil, err
	}

	proxyReq.Header.Set("Content-Type", "application/json")
	proxyReq.Header.Set("User-Agent", req.Header.Get("User-Agent"))

	return pt.Base.RoundTrip(proxyReq)
}

// RunCrawler executes the asynchronous forum crawl based on the current configuration.
func RunCrawler(cfg *config.Config, incremental bool) ([]CrawledThread, error) {
	var crawled []CrawledThread
	seenHashes := make(map[string]bool)
	var mu sync.Mutex

	c := colly.NewCollector(
		colly.Async(true),
		colly.MaxDepth(2),
	)

	c.SetRequestTimeout(time.Duration(cfg.ScraperTimeoutSecs) * time.Second)

	err := c.Limit(&colly.LimitRule{
		DomainGlob:  "*",
		Parallelism: cfg.ScraperConcurrency,
		Delay:       1 * time.Second,
	})
	if err != nil {
		return nil, err
	}

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

			if cfg.LogLevel == "debug" {
				dumpDebugHTML(r.Request.URL.String(), r.Body)
			}
		}
	})

	c.OnHTML("h4.ipsDataItem_title > span.ipsType_break > a", func(e *colly.HTMLElement) {
		link := e.Attr("href")
		rawTitle := strings.TrimSpace(e.Text)
		threadContainer := e.DOM.Closest(".ipsDataItem")
		timeEl := threadContainer.Find("time[datetime]")
		postedAtStr, _ := timeEl.Attr("datetime")

		if link != "" && rawTitle != "" {
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
	c.OnHTML("a[href^=\"magnet:?\"]", func(e *colly.HTMLElement) {
		if e.Request.Ctx.GetAny("detail_parsed") != nil {
			return
		}
		e.Request.Ctx.Put("detail_parsed", true)

		rawTitle := e.Request.Ctx.Get("raw_title")
		contentType := e.Request.Ctx.Get("type")
		catalogID := e.Request.Ctx.Get("catalog_id")
		postedAtStr := e.Request.Ctx.Get("posted_at")

		var magnets []string
		seenMags := make(map[string]bool)

		// Rely on a stable ancestor container selector to dodge brittle layout assumptions [report.md]
		container := e.DOM.Closest(".ipsPad")
		if container.Length() == 0 {
			container = e.DOM.Closest(".ipsType_normal")
		}
		if container.Length() == 0 {
			container = e.DOM.Closest("article")
		}
		if container.Length() == 0 {
			container = e.DOM.Parent()
			if container.Parent().Length() > 0 {
				container = container.Parent()
			}
		}

		container.Find("a[href^=\"magnet:?\"]").Each(func(_ int, s *goquery.Selection) {
			if href, ok := s.Attr("href"); ok {
				// Deduplicate collected magnet links cleanly [report.md]
				if href != "" && !seenMags[href] {
					seenMags[href] = true
					magnets = append(magnets, href)
				}
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

	endPage := cfg.ScrapeEndPage
	if incremental {
		endPage = cfg.IncrementalEndPage
	}

	sortQuery := cfg.ForumSortQuery
	if incremental {
		sortQuery = cfg.IncrementalSortQuery
	}

	var startUrls []string
	addURLs := func(list []string, contentType, catalog string) {
		for _, rawURL := range list {
			startUrls = append(startUrls, rawURL)
		}
	}
	addURLs(cfg.SeriesForumURLs, "series", "top-series-from-forum")
	addURLs(cfg.MovieForumURLs, "movie", "tamil-hd-movies")
	addURLs(cfg.DubbedMovieURLs, "movie", "tamil-dubbed-movies")

	runTimestamp := time.Now().Unix()

	buildURL := func(base string, pageIndex int) string {
		var u string
		if pageIndex == 1 {
			u = base
		} else {
			u = fmt.Sprintf("%s/page/%d", base, pageIndex)
		}
		if sortQuery != "" {
			sep := "&"
			if !strings.Contains(u, "?") {
				sep = "?"
			}
			u = u + sep + strings.TrimPrefix(sortQuery, "&")
		}
		return u
	}

	for _, base := range startUrls {
		base = strings.TrimSuffix(base, "/")
		contentType := "movie"
		catalog := "tamil-hd-movies"
		for _, urlVal := range cfg.SeriesForumURLs {
			if strings.HasPrefix(base, strings.TrimSuffix(urlVal, "/")) {
				contentType = "series"
				catalog = "top-series-from-forum"
			}
		}
		for _, urlVal := range cfg.DubbedMovieURLs {
			if strings.HasPrefix(base, strings.TrimSuffix(urlVal, "/")) {
				contentType = "movie"
				catalog = "tamil-dubbed-movies"
			}
		}

		for i := cfg.ScrapeStartPage; i <= endPage; i++ {
			u := buildURL(base, i)
			ctx := colly.NewContext()
			ctx.Put("type", contentType)
			ctx.Put("catalog_id", catalog)
			ctx.Put("unique_key", fmt.Sprintf("%s-%d", u, runTimestamp))

			_ = c.Request("GET", u, nil, ctx, nil)
		}
	}

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

// RunTargetedCrawler executes a single-page target crawl on a specific thread URL, extracting its magnets and page title.
func RunTargetedCrawler(cfg *config.Config, threadURL, contentType, catalogID string) ([]CrawledThread, error) {
	var crawled []CrawledThread
	var mu sync.Mutex

	c := colly.NewCollector()

	c.SetRequestTimeout(time.Duration(cfg.ScraperTimeoutSecs) * time.Second)

	baseTransport := createOptimizedScraperTransport()
	if cfg.IsProxyEnabled {
		utils.Logger.Info().Msg("Targeted Scraper: Injecting custom POST-mutating ProxyTransport for Cloudflare bypass.")
		proxyTransport := &ProxyTransport{
			ProxyURLs: cfg.ProxyURLs,
			Base:      baseTransport,
		}
		c.WithTransport(proxyTransport)
	} else {
		c.WithTransport(baseTransport)
	}

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
					Msg("Targeted Scraper: Cloudflare anti-bot block or challenge detected. Retrying request.")
				time.Sleep(backoff)
				_ = r.Request.Visit(r.Request.URL.String())
			} else {
				utils.Logger.Error().
					Str("url", r.Request.URL.String()).
					Str("proxy", r.Request.ProxyURL).
					Msg("Targeted Scraper: Max retries exceeded for Cloudflare blocked URL.")
			}
		}
	})

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
				Msg("Targeted Scraper: Request failed. Scheduling retry.")
			time.Sleep(backoff)
			_ = r.Request.Visit(r.Request.URL.String())
		} else {
			utils.Logger.Error().
				Str("url", r.Request.URL.String()).
				Err(err).
				Msg("Targeted Scraper: Max retries exceeded for URL.")
		}
	})

	c.OnHTML("a[href^=\"magnet:?\"]", func(e *colly.HTMLElement) {
		if e.Request.Ctx.GetAny("detail_parsed") != nil {
			return
		}
		e.Request.Ctx.Put("detail_parsed", true)

		// Find and extract clean title from h1 header or title tag
		rawTitle := e.DOM.ParentsUntil("html").Find("h1.ipsType_pageTitle").Text()
		rawTitle = strings.TrimSpace(rawTitle)
		if rawTitle == "" {
			rawTitle = e.DOM.ParentsUntil("html").Find("title").Text()
			rawTitle = strings.TrimSpace(strings.ReplaceAll(rawTitle, " - TamilMV", ""))
		}

		if rawTitle == "" {
			rawTitle = "Unknown Recouped Title"
		}

		var magnets []string
		seenMags := make(map[string]bool)

		container := e.DOM.Closest(".ipsPad")
		if container.Length() == 0 {
			container = e.DOM.Closest(".ipsType_normal")
		}
		if container.Length() == 0 {
			container = e.DOM.Closest("article")
		}
		if container.Length() == 0 {
			container = e.DOM.Parent()
			if container.Parent().Length() > 0 {
				container = container.Parent()
			}
		}

		container.Find("a[href^=\"magnet:?\"]").Each(func(_ int, s *goquery.Selection) {
			if href, ok := s.Attr("href"); ok {
				if href != "" && !seenMags[href] {
					seenMags[href] = true
					magnets = append(magnets, href)
				}
			}
		})

		if len(magnets) > 0 {
			hash := parser.GenerateThreadHash(rawTitle, magnets)
			
			var postedAt *time.Time
			timeEl := e.DOM.ParentsUntil("html").Find("time[datetime]").First()
			if postedAtStr, _ := timeEl.Attr("datetime"); postedAtStr != "" {
				if t, errDate := time.Parse(time.RFC3339, postedAtStr); errDate == nil {
					postedAt = &t
				}
			}
			if postedAt == nil {
				now := time.Now()
				postedAt = &now
			}

			mu.Lock()
			crawled = append(crawled, CrawledThread{
				ThreadHash: hash,
				RawTitle:   rawTitle,
				MagnetURIs: magnets,
				Type:       contentType,
				PostedAt:   postedAt,
				CatalogID:  catalogID,
			})
			mu.Unlock()
		}
	})

	_ = c.Visit(threadURL)
	c.Wait()

	return crawled, nil
}
