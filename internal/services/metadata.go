package metadata

import (
	"crypto/tls"
	"fmt"
	"math"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/kiskey/stremio-mvshows-go/internal/config"
	"github.com/kiskey/stremio-mvshows-go/internal/utils"
	"golang.org/x/net/http2"
)

type TmdbResult struct {
	TmdbID      string                 `json:"tmdb_id"`
	ImdbID      string                 `json:"imdb_id"`
	Title       string                 `json:"title"`
	Year        int                    `json:"year"`
	Poster      string                 `json:"poster"`
	Description string                 `json:"description"`
	RawData     map[string]interface{} `json:"raw_data"`
}

type TMDBClient struct {
	client *resty.Client
	apiKey string
}

var yearArtifactRegexp = regexp.MustCompile(`(?i)[\(\[]\d{4}[\)\]]`)

// createOptimizedTMDBHTTPClient configures an transport optimized for low latency and high concurrency
func createOptimizedTMDBHTTPClient(timeout time.Duration) *http.Client {
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

func NewTMDBClient(cfg *config.Config) *TMDBClient {
	httpClient := createOptimizedTMDBHTTPClient(8 * time.Second)
	restyClient := resty.NewWithClient(httpClient).
		SetBaseURL("https://api.themoviedb.org/3").
		SetRetryCount(3).
		SetRetryWaitTime(2 * time.Second)

	return &TMDBClient{
		client: restyClient,
		apiKey: cfg.TMDBAPIKey,
	}
}

// Search looks up content by title and year, implementing cross-type fallbacks and advanced fuzzy scoring.
func (t *TMDBClient) Search(title string, year int, contentType string) (*TmdbResult, error) {
	if t.apiKey == "" {
		return nil, fmt.Errorf("TMDB API key is not configured")
	}

	// 1. Strip year artifacts like "(2026)" from the title query to prevent TMDB query poisoning
	cleanTitle := yearArtifactRegexp.ReplaceAllString(title, "")
	cleanTitle = strings.TrimSpace(cleanTitle)

	// 2. Execute Primary Search
	bestCand, bestScore, err := t.executeSearch(cleanTitle, year, contentType)
	matchType := contentType

	// 3. Cross-Content-Type Fallback
	// If a TV Show was accidentally posted in a Movie forum, auto-correct and retry!
	if err != nil || bestCand == nil || bestScore < 40.0 {
		altType := "movie"
		if strings.ToLower(contentType) == "movie" {
			altType = "series"
		}
		
		utils.Logger.Debug().
			Str("title", cleanTitle).
			Str("orig_type", contentType).
			Str("alt_type", altType).
			Msg("Primary TMDB search yielded 0 results. Auto-correcting and executing cross-type fallback.")
		
		altCand, altScore, altErr := t.executeSearch(cleanTitle, year, altType)
		if altErr == nil && altCand != nil && altScore >= 40.0 {
			bestCand = altCand
			bestScore = altScore
			matchType = altType
		} else {
			return nil, fmt.Errorf("no metadata match met similarity threshold on any content type")
		}
	}

	candID := fmt.Sprintf("%.0f", bestCand["id"].(float64))
	return t.GetByID(candID, matchType)
}

func (t *TMDBClient) executeSearch(title string, year int, contentType string) (map[string]interface{}, float64, error) {
	endpoint := "/search/movie"
	if strings.ToLower(contentType) == "series" {
		endpoint = "/search/tv"
	}

	params := map[string]string{
		"api_key":       t.apiKey,
		"query":         title,
		"include_adult": "false",
	}
	if year > 0 {
		if strings.ToLower(contentType) == "series" {
			params["first_air_date_year"] = strconv.Itoa(year)
		} else {
			params["primary_release_year"] = strconv.Itoa(year)
		}
	}

	var responseMap map[string]interface{}
	resp, err := t.client.R().
		SetQueryParams(params).
		SetResult(&responseMap).
		Get(endpoint)

	if err != nil {
		return nil, 0, err
	}
	if resp.StatusCode() != 200 {
		return nil, 0, fmt.Errorf("TMDB returned status code %d", resp.StatusCode())
	}

	results, ok := responseMap["results"].([]interface{})
	if !ok || len(results) == 0 {
		if year > 0 {
			// Retry without year restriction for fuzzy matching
			delete(params, "primary_release_year")
			delete(params, "first_air_date_year")
			resp, err = t.client.R().SetQueryParams(params).SetResult(&responseMap).Get(endpoint)
			if err == nil && resp.StatusCode() == 200 {
				results, _ = responseMap["results"].([]interface{})
			}
		}
	}

	if len(results) == 0 {
		return nil, 0, fmt.Errorf("no results found on TMDB")
	}

	var bestCandidate map[string]interface{}
	bestScore := -1.0

	for _, item := range results {
		cand, ok := item.(map[string]interface{})
		if !ok {
			continue
		}

		candTitle := getMapString(cand, "title")
		if candTitle == "" {
			candTitle = getMapString(cand, "name")
		}

		candDate := getMapString(cand, "release_date")
		if candDate == "" {
			candDate = getMapString(cand, "first_air_date")
		}

		candYear := 0
		if len(candDate) >= 4 {
			candYear, _ = strconv.Atoi(candDate[:4])
		}

		score := calculateScore(title, year, candTitle, candYear)
		if score > bestScore {
			bestScore = score
			bestCandidate = cand
		}
	}

	return bestCandidate, bestScore, nil
}

func (t *TMDBClient) GetByID(id string, contentType string) (*TmdbResult, error) {
	if t.apiKey == "" {
		return nil, fmt.Errorf("TMDB API key is not configured")
	}

	mediaType := "movie"
	if strings.ToLower(contentType) == "series" {
		mediaType = "tv"
	}

	var data map[string]interface{}
	endpoint := fmt.Sprintf("/%s/%s", mediaType, id)
	resp, err := t.client.R().
		SetQueryParam("api_key", t.apiKey).
		SetResult(&data).
		Get(endpoint)

	if err != nil {
		return nil, err
	}
	if resp.StatusCode() != 200 {
		return nil, fmt.Errorf("TMDB direct fetch returned status %d", resp.StatusCode())
	}

	imdbID := ""
	var extData map[string]interface{}
	extEndpoint := fmt.Sprintf("/%s/%s/external_ids", mediaType, id)
	extResp, extErr := t.client.R().
		SetQueryParam("api_key", t.apiKey).
		SetResult(&extData).
		Get(extEndpoint)

	if extErr == nil && extResp.StatusCode() == 200 {
		imdbID = getMapString(extData, "imdb_id")
	}

	title := getMapString(data, "title")
	if title == "" {
		title = getMapString(data, "name")
	}

	dateStr := getMapString(data, "release_date")
	if dateStr == "" {
		dateStr = getMapString(data, "first_air_date")
	}

	year := 0
	if len(dateStr) >= 4 {
		year, _ = strconv.Atoi(dateStr[:4])
	}

	posterPath := getMapString(data, "poster_path")
	posterURL := ""
	if posterPath != "" {
		posterURL = "https://image.tmdb.org/t/p/w500" + posterPath
	}

	res := &TmdbResult{
		TmdbID:      fmt.Sprintf("%s:%s", mediaType, id),
		ImdbID:      imdbID,
		Title:       title,
		Year:        year,
		Poster:      posterURL,
		Description: getMapString(data, "overview"),
		RawData:     data,
	}

	return res, nil
}

// ── Advanced Fuzzy Scoring Helpers (Imported from Matcher) ──

var stopWords = map[string]bool{
	"the": true, "a": true, "an": true, "and": true, "or": true,
	"of": true, "in": true, "on": true, "at": true, "to": true,
	"for": true, "with": true, "by": true, "from": true, "aka": true,
	"la": true, "le": true, "les": true, "el": true, "un": true, "une": true,
}

var metadataWords = map[string]bool{
	"1080p": true, "720p": true, "2160p": true, "480p": true, "360p": true,
	"4k": true, "uhd": true, "bluray": true, "bdrip": true, "brrip": true,
	"webdl": true, "webrip": true, "hdrip": true, "dvdrip": true, "pdtv": true,
	"hdtv": true, "cam": true, "camrip": true, "hdcam": true, "ts": true,
	"hdts": true, "tc": true, "predvd": true, "dvdscr": true, "screener": true,
	"scr": true, "hq": true, "v2": true, "v3": true, "hc": true, "clean": true,
	"imax": true, "h264": true, "x264": true, "h265": true, "x265": true,
	"hevc": true, "aac": true, "aac3": true, "dts": true, "dd51": true,
	"truehd": true, "ac3": true, "mp3": true, "xvid": true, "divx": true,
	"av1": true, "vp9": true, "hdr10": true, "hdr": true, "dv": true,
	"dolby": true, "vision": true, "atmos": true, "dts-hd": true, "ma": true,
	"dual": true, "audio": true, "dubbed": true, "dub": true, "multi": true,
	"hindi": true, "tamil": true, "telugu": true, "malayalam": true,
	"kannada": true, "bengali": true, "marathi": true, "punjabi": true,
	"english": true, "spanish": true, "french": true, "italian": true,
	"russian": true, "korean": true, "japanese": true, "chinese": true,
	"51": true, "71": true, "20": true, "10bit": true, "remux": true,
	"3d": true, "sdr": true, "gb": true, "mb": true, "kb": true,
	"web": true, "dl": true, "hd": true,
	"complete": true, "repack": true, "proper": true, "vostfr": true,
	"subs": true, "sub": true, "esub": true, "vof": true, "vff": true,
	"vf": true, "season": true, "series": true, "episode": true, "pack": true,
}

var homoglyphClasses = map[rune][]rune{
	'0': {'0', 'o'}, 'o': {'0', 'o'},
	'1': {'1', 'i', 'l', '!'}, 'i': {'1', 'i', 'l', '!'}, 'l': {'1', 'i', 'l', '!'},
	'3': {'3', 'e'}, 'e': {'3', 'e'},
	'4': {'4', 'a', '@'}, 'a': {'4', 'a', '@'},
	'5': {'5', 's'}, 's': {'5', 's'},
	'7': {'7', 't', 'v', 'l'}, 't': {'7', 't'}, 'v': {'7', 'v'},
	'8': {'8', 'b'}, 'b': {'8', 'b'},
	'9': {'9', 'g'}, 'g': {'9', 'g'},
}

func getMapString(m map[string]interface{}, key string) string {
	if val, ok := m[key]; ok && val != nil {
		if str, ok := val.(string); ok {
			return str
		}
	}
	return ""
}

func isNumber(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

func cleanWord(w string) string {
	return strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			return r
		}
		return -1
	}, strings.ToLower(w))
}

func stripLeadingArticles(s string) string {
	s = strings.TrimSpace(s)
	articles := []string{"the ", "a ", "an ", "le ", "la ", "les ", "l'"}
	for _, art := range articles {
		if strings.HasPrefix(s, art) {
			return strings.TrimPrefix(s, art)
		}
	}
	return s
}

func isTechnicalToken(s string) bool {
	if metadataWords[s] || stopWords[s] || isNumber(s) {
		return true
	}
	if len(s) >= 2 {
		first := s[0]
		if (first == 's' || first == 'e' || first == 'p') && isNumber(s[1:]) {
			return true
		}
		if len(s) >= 3 && (s[:2] == "se" || s[:2] == "ep") && isNumber(s[2:]) {
			return true
		}
		if len(s) >= 4 && s[:3] == "epi" && isNumber(s[3:]) {
			return true
		}
		if len(s) >= 5 && (s[:4] == "seas" || s[:4] == "part") && isNumber(s[4:]) {
			return true
		}
		if len(s) >= 7 && s[:6] == "season" && isNumber(s[6:]) {
			return true
		}
		if len(s) >= 8 && s[:7] == "episode" && isNumber(s[7:]) {
			return true
		}
	}
	return false
}

func passTitleGuardrail(targetTitle, parsedTitle string) bool {
	cleanTarget := strings.Trim(strings.ToLower(targetTitle), " .-_[]()/\\")
	cleanParsed := strings.Trim(strings.ToLower(parsedTitle), " .-_[]()/\\")

	if cleanTarget == cleanParsed {
		return true
	}

	targetNoArt := stripLeadingArticles(cleanTarget)
	parsedNoArt := stripLeadingArticles(cleanParsed)
	if targetNoArt == parsedNoArt {
		return true
	}

	targetWords := strings.Fields(targetNoArt)
	parsedWords := strings.Fields(parsedNoArt)

	// Multi-Word Franchise Leakage Guardrail
	if len(targetWords) > 1 && len(parsedWords) > len(targetWords) {
		startsSame := true
		for i := 0; i < len(targetWords); i++ {
			if cleanWord(parsedWords[i]) != cleanWord(targetWords[i]) {
				startsSame = false
				break
			}
		}

		if startsSame {
			extraWords := parsedWords[len(targetWords):]
			for _, w := range extraWords {
				cw := cleanWord(w)
				if cw != "" && !isTechnicalToken(cw) {
					return false // ❌ REJECTED: Substantive Proper-Noun Detected
				}
			}
		}
	}

	// Single-Word Title Guardrail
	if len(targetWords) == 1 {
		singleWord := cleanWord(targetWords[0])
		if len(parsedWords) > 1 {
			firstWord := cleanWord(parsedWords[0])
			if firstWord == singleWord {
				return true
			}

			for _, w := range parsedWords {
				cw := cleanWord(w)
				if cw != "" && cw != singleWord && !isTechnicalToken(cw) {
					return false // ❌ REJECTED
				}
			}
		}
	}
	return true
}

func getHomoglyphRepresentations(r rune) []rune {
	if classes, ok := homoglyphClasses[r]; ok {
		return classes
	}
	return []rune{r}
}

// OverlapCoefficient computes Bigram overlap evaluating homoglyphs to resolve typos/transliteration errors
func OverlapCoefficient(s1, s2 string) float64 {
	if s1 == s2 {
		return 1.0
	}
	if len(s1) < 2 || len(s2) < 2 {
		return 0.0
	}

	bg1 := make(map[string]struct{}, len(s1)*2)
	runes1 := []rune(s1)
	for i := 0; i < len(runes1)-1; i++ {
		repsA := getHomoglyphRepresentations(runes1[i])
		repsB := getHomoglyphRepresentations(runes1[i+1])
		for _, charA := range repsA {
			for _, charB := range repsB {
				bg1[string(charA)+string(charB)] = struct{}{}
			}
		}
	}

	bg2 := make(map[string]struct{}, len(s2)*2)
	runes2 := []rune(s2)
	intersection := 0
	for i := 0; i < len(runes2)-1; i++ {
		repsA := getHomoglyphRepresentations(runes2[i])
		repsB := getHomoglyphRepresentations(runes2[i+1])
		for _, charA := range repsA {
			for _, charB := range repsB {
				bigram := string(charA) + string(charB)
				if _, ok := bg2[bigram]; !ok {
					bg2[bigram] = struct{}{}
					if _, exists := bg1[bigram]; exists {
						intersection++
					}
				}
			}
		}
	}

	if len(bg1) == 0 || len(bg2) == 0 {
		return 0.0
	}

	minSize := len(bg1)
	if len(bg2) < minSize {
		minSize = len(bg2)
	}

	return float64(intersection) / float64(minSize)
}

func calculateScore(targetTitle string, targetYear int, candidateTitle string, candidateYear int) float64 {
	// Guardrail Check: Block franchise/sequel pollution
	if !passTitleGuardrail(targetTitle, candidateTitle) {
		return 0.0
	}

	// 1. Calculate year score (up to 40% weight)
	yearScore := 0.0
	if targetYear > 0 && candidateYear > 0 {
		diff := math.Abs(float64(targetYear - candidateYear))
		if diff == 0 {
			yearScore = 40.0
		} else if diff == 1 {
			yearScore = 25.0
		} else if diff <= 3 {
			yearScore = 10.0
		}
	} else {
		// No target year to compare against, give moderate default weight
		yearScore = 20.0
	}

	// 2. Calculate title similarity score via advanced Bigram Homoglyph Coefficient (up to 60% weight)
	normTarget := normalizeForCompare(targetTitle)
	normCand := normalizeForCompare(candidateTitle)

	titleScore := OverlapCoefficient(normTarget, normCand) * 60.0

	return yearScore + titleScore
}

var nonWordPunctRegexp = regexp.MustCompile(`[^\w\s]`)

func normalizeForCompare(s string) string {
	s = strings.ToLower(s)
	s = nonWordPunctRegexp.ReplaceAllString(s, "")
	s = strings.ReplaceAll(s, "  ", " ")
	return strings.TrimSpace(s)
}
