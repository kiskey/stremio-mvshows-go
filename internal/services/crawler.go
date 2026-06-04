package crawler

import (
	"errors"
	"fmt"
	"io"
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
	"github.com/sevvian/smvshows-go/internal/config"
	"github.com/sevvian/smvshows-go/internal/database"
	"github.com/sevvian/smvshows-go/internal/services/parser"
	"github.com/sevvian/smvshows-go/internal/utils"
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
		return nil, errors.New("proxy list is empty")
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

// RunCrawler executes the asynchronous forum crawl based on the current configuration.
func RunCrawler(cfg *config.Config) ([]CrawledThread, error) {
	var crawled []CrawledThread
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

	// Dynamic proxy rotation
	if cfg.IsProxyEnabled {
		switcher, errProxy := RoundRobinProxySwitcher(cfg.ProxyURLs)
		if errProxy != nil {
			utils.Logger.Error().Err(errProxy).Msg("Failed to configure proxy switcher. Continuing without proxies.")
		} else {
			c.SetProxyFunc(switcher)
		}
	}

	// Pre-navigation request hooks
	c.OnRequest(func(r *colly.Request) {
		r.Headers.Set("User-Agent", cfg.ScraperUserAgent)
		r.Headers.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8")
		r.Headers.Set("Accept-Language", "en-US,en;q=0.5")
	})

	// Retry logic with exponential backoff on errors
	c.OnError(func(r *colly.Response, err error) {
		if r.Request.RetryCount < cfg.ScraperRetryCount {
			r.Request.RetryCount++
			backoff := time.Duration(r.Request.RetryCount*r.Request.RetryCount) * 2 * time.Second
			utils.Logger.Warn().
				Str("url", r.Request.URL.String()).
				Err(err).
				Int("retry_count", r.Request.RetryCount).
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

		var postedAt *time.Time
		if postedAtStr != "" {
			if t, errDate := time.Parse(time.RFC3339, postedAtStr); errDate == nil {
				postedAt = &t
			}
		}

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

	// Detail page magnet extraction handler
	c.OnHTML("a[href^=\"magnet:?\"]", func(e *colly.HTMLElement) {
		rawTitle := e.Request.Ctx.Get("raw_title")
		contentType := e.Request.Ctx.Get("type")
		catalogID := e.Request.Ctx.Get("catalog_id")
		postedAtStr := e.Request.Ctx.Get("posted_at")

		var magnets []string
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
			// Prevent duplicate additions in the same scraping window
			isDup := false
			for _, t := range crawled {
				if t.ThreadHash == hash {
					isDup = true
					break
				}
			}
			if !isDup {
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

type ProxyTransport struct {
	ProxyURLs []string
	index     uint64
	Base      http.RoundTripper
}

func (pt *ProxyTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	idx := atomic.AddUint64(&pt.index, 1) - 1
	proxyURLStr := pt.ProxyURLs[idx%uint64(len(pt.ProxyURLs))]

	proxyURL, err := url.Parse(proxyURLStr)
	if err != nil {
		return nil, err
	}

	transport := &http.Transport{
		Proxy: http.ProxyURL(proxyURL),
	}
	return transport.RoundTrip(req)
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
