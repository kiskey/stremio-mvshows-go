// Version: 1.0.1
// Change log: Added SCRAPE_INCREMENTAL_END_PAGE, INCREMENTAL_SORT_QUERY, and FORCE_FULL_SCRAPE configuration fields to support smart incremental crawling.

package config

import (
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
)

type Config struct {
	Port                 int
	LogLevel             string
	SeriesForumURLs      []string
	MovieForumURLs       []string
	DubbedMovieURLs      []string
	ScrapeStartPage      int
	ScrapeEndPage        int
	ScraperConcurrency   int
	ScraperRetryCount    int
	ScraperTimeoutSecs   int
	ScraperUserAgent     string
	MainWorkflowCron     string
	TMDBAPIKey           string
	DebridService        string
	RealDebridAPIKey     string
	TorboxAPIKey         string
	ProxyURLs            []string
	AddonID              string
	AddonName            string
	AddonDescription     string
	AddonVersion         string
	PlaceholderPoster    string
	TrackerURL           string
	AppHost              string
	ForumSortQuery       string
	DBAutoVacuumCron     string
	DBAutoVacuumEnabled  bool
	IsRDEnabled          bool
	IsTorboxEnabled      bool
	IsProxyEnabled       bool
	// Cache Expiry Configuration
	CacheExpiryDays      int
	CacheExpiryEnabled   bool
	// Incremental Scraping Configuration
	IncrementalEndPage   int
	IncrementalSortQuery string
	ForceFullScrape      bool
}

// Load reads settings from the environment and optional .env file.
func Load() *Config {
	// Attempt loading .env. Ignore error if file is missing (e.g. in production containers)
	_ = godotenv.Load()

	// Parse fallback keys for forum URLs to maintain 100% backward compatibility
	seriesURLs := splitEnv("SERIES_FORUM_URLS")
	if len(seriesURLs) == 0 {
		seriesURLs = splitEnv("FORUM_URLS")
	}
	if len(seriesURLs) == 0 {
		seriesURLs = splitEnv("FORUM_URL")
	}

	c := &Config{
		Port:                 getEnvInt("PORT", 3000),
		LogLevel:             getEnv("LOG_LEVEL", "info"),
		SeriesForumURLs:      seriesURLs,
		MovieForumURLs:       splitEnv("MOVIE_FORUM_URLS"),
		DubbedMovieURLs:      splitEnv("DUBBED_MOVIE_FORUM_URLS"),
		ScrapeStartPage:      getEnvInt("SCRAPE_START_PAGE", 1),
		ScrapeEndPage:        getEnvInt("SCRAPE_END_PAGE", 20),
		ScraperConcurrency:   getEnvInt("SCRAPER_CONCURRENCY", 5),
		ScraperRetryCount:    getEnvInt("SCRAPER_RETRY_COUNT", 3),
		ScraperTimeoutSecs:   getEnvInt("SCRAPER_TIMEOUT_SECS", 30),
		ScraperUserAgent:     getEnv("SCRAPER_USER_AGENT", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"),
		MainWorkflowCron:     getEnv("MAIN_WORKFLOW_CRON", "0 */6 * * *"),
		TMDBAPIKey:           os.Getenv("TMDB_API_KEY"),
		DebridService:        strings.ToLower(strings.TrimSpace(os.Getenv("DEBRID_SERVICE"))),
		RealDebridAPIKey:     os.Getenv("REALDEBRID_API_KEY"),
		TorboxAPIKey:         os.Getenv("TORBOX_API_KEY"),
		ProxyURLs:            splitEnv("PROXY_URLS"),
		AddonID:              getEnv("ADDON_ID", "org.stremio.torrent.nodejs.example"),
		AddonName:            getEnv("ADDON_NAME", "TamilMV WebSeries"),
		AddonDescription:     getEnv("ADDON_DESCRIPTION", "A Stremio addon providing webseries streams."),
		AddonVersion:         getEnv("ADDON_VERSION", "1.0.0"),
		PlaceholderPoster:    getEnv("PLACEHOLDER_POSTER", "https://upload.wikimedia.org/wikipedia/en/thumb/d/da/Aha_%28streaming_service.svg/250px-Aha_%28streaming_service.svg.png"),
		TrackerURL:           getEnv("TRACKER_URL", "https://ngosang.github.io/trackerslist/trackers_best.txt"),
		AppHost:              getEnv("APP_HOST", "http://127.0.0.1:3000"),
		ForumSortQuery:       strings.TrimSpace(os.Getenv("FORUM_SORT_QUERY")),
		DBAutoVacuumCron:     os.Getenv("DB_AUTO_VACUUM_CRON"),
		DBAutoVacuumEnabled:  os.Getenv("DB_AUTO_VACUUM_ENABLED") == "true",
		// Cache Expiry Defaults: expire cache if unaccessed for 5 days
		CacheExpiryDays:      getEnvInt("CACHE_EXPIRY_DAYS", 5),
		CacheExpiryEnabled:   getEnv("CACHE_EXPIRY_ENABLED", "true") == "true",
		// Incremental Scraping Configuration
		IncrementalEndPage:   getEnvInt("SCRAPE_INCREMENTAL_END_PAGE", 3),
		IncrementalSortQuery: getEnv("INCREMENTAL_SORT_QUERY", "&sortby=last_post&sortdirection=desc"),
		ForceFullScrape:      os.Getenv("FORCE_FULL_SCRAPE") == "true",
	}

	c.IsRDEnabled = c.RealDebridAPIKey != ""
	c.IsTorboxEnabled = c.TorboxAPIKey != ""
	c.IsProxyEnabled = len(c.ProxyURLs) > 0

	// Auto-detect debrid provider if DEBRID_SERVICE is omitted or blank
	if c.DebridService == "" {
		if c.IsRDEnabled {
			c.DebridService = "realdebrid"
		} else if c.IsTorboxEnabled {
			c.DebridService = "torbox"
		} else {
			c.DebridService = "none"
		}
	}

	// Validate API keys are present for chosen service
	if c.DebridService != "none" {
		if c.DebridService == "torbox" && !c.IsTorboxEnabled {
			log.Println("WARNING: DEBRID_SERVICE is set to 'torbox' but TORBOX_API_KEY is missing. Disabling debrid.")
			c.DebridService = "none"
		} else if c.DebridService == "realdebrid" && !c.IsRDEnabled {
			log.Println("WARNING: DEBRID_SERVICE is set to 'realdebrid' but REALDEBRID_API_KEY is missing. Disabling debrid.")
			c.DebridService = "none"
		}
	}

	// Fail-safe warnings
	if len(c.SeriesForumURLs) == 0 && len(c.MovieForumURLs) == 0 && len(c.DubbedMovieURLs) == 0 {
		log.Println("WARNING: No forum URLs are configured. Scraper runs will skip thread gathering.")
	}

	return c
}

func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if val := os.Getenv(key); val != "" {
		if i, err := strconv.Atoi(val); err == nil {
			return i
		}
	}
	return fallback
}

func splitEnv(key string) []string {
	val := os.Getenv(key)
	if val == "" {
		return nil
	}
	parts := strings.Split(val, ",")
	res := make([]string, 0, len(parts))
	for _, p := range parts {
		if clean := strings.TrimSpace(p); clean != "" {
			res = append(res, clean)
		}
	}
	return res
}
