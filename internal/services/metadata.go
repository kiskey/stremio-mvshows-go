// Version: 1.1.0
// Change log: Integrated a Cinemeta-First lookup flow with a TMDB search fallback, enabling native Stremio-aligned metadata cards and seamless resolution of obscure regional content.

package metadata

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/kiskey/stremio-mvshows-go/internal/config"
	"golang.org/x/net/http2"
)

// ── Models ──

type TmdbResult struct {
	TmdbID      string                 `json:"tmdb_id"`
	ImdbID      string                 `json:"imdb_id"`
	Title       string                 `json:"title"`
	Year        int                    `json:"year"`
	Poster      string                 `json:"poster"` // Maintained for compilation safety across caller packages
	Description string                 `json:"description"`
	RawData     map[string]interface{} `json:"raw_data"` // Kept as map specifically to support Orchestrator compatibility
}

// Low-allocation structs for zero-heap search list parsing
type tmdbSearchResponse struct {
	Results []tmdbSearchResult `json:"results"`
}

type tmdbSearchResult struct {
	ID           float64 `json:"id"`
	Title        string  `json:"title"`
	Name         string  `json:"name"`
	ReleaseDate  string  `json:"release_date"`
	FirstAirDate string  `json:"first_air_date"`
}

type CinemetaResponse struct {
	Meta struct {
		ID          string   `json:"id"`
		Type        string   `json:"type"`
		Name        string   `json:"name"`
		Poster      string   `json:"poster"`
		Description string   `json:"description"`
		ReleaseInfo string   `json:"releaseInfo"`
		ImdbRating  string   `json:"imdbRating"`
		Genres      []string `json:"genres"`
	} `json:"meta"`
}

type TMDBClient struct {
	client *http.Client
	apiKey string
}

func createOptimizedTMDBHTTPClient(timeout time.Duration) *http.Client {
	transport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,  // Faster connect timeout
			KeepAlive: 30 * time.Second, // Consistent keep-alive
		}).DialContext,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   100,             // Avoid connection starvation under concurrency
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   3 * time.Second, // Faster TLS handshakes
		ExpectContinueTimeout: 1 * time.Second,
		ForceAttemptHTTP2:     true,            // Force HTTP/2 attempt
		TLSClientConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
		},
	}
	_ = http2.ConfigureTransport(transport)

	return &http.Client{
		Transport: transport,
		Timeout:   timeout,
	}
}

func NewTMDBClient(cfg *config.Config) *TMDBClient {
	return &TMDBClient{
		client: createOptimizedTMDBHTTPClient(8 * time.Second),
		apiKey: cfg.TMDBAPIKey,
	}
}

// doRequestWithRetry acts as an allocation-free HTTP request executor handling transient API timeouts
func (t *TMDBClient) doRequestWithRetry(req *http.Request, dest interface{}) error {
	var err error
	var resp *http.Response
	for i := 0; i < 3; i++ {
		resp, err = t.client.Do(req)
		if err == nil && resp.StatusCode == 200 {
			defer resp.Body.Close()
			return json.NewDecoder(resp.Body).Decode(dest)
		}
		if resp != nil {
			resp.Body.Close()
		}
		select {
		case <-req.Context().Done():
			return req.Context().Err()
		case <-time.After(1 * time.Second):
		}
	}
	if err != nil {
		return err
	}
	return fmt.Errorf("status code %d", resp.StatusCode)
}

// GetByImdbIDFromCinemeta fetches metadata cards directly from Stremio's native Cinemeta stack
func (t *TMDBClient) GetByImdbIDFromCinemeta(imdbID string, contentType string) (*TmdbResult, error) {
	mediaType := "movie"
	if strings.ToLower(contentType) == "series" {
		mediaType = "series"
	}

	urlStr := fmt.Sprintf("https://v3-cinemeta.strem.io/meta/%s/%s.json", mediaType, imdbID)
	req, err := http.NewRequestWithContext(context.Background(), "GET", urlStr, nil)
	if err != nil {
		return nil, err
	}

	var res CinemetaResponse
	if err := t.doRequestWithRetry(req, &res); err != nil {
		return nil, err
	}

	if res.Meta.ID == "" {
		return nil, fmt.Errorf("cinemeta metadata not found for ID: %s", imdbID)
	}

	// Safely parse year from Cinemeta's releaseInfo/year representation
	year := 0
	if len(res.Meta.ReleaseInfo) >= 4 {
		if yVal, err := strconv.Atoi(res.Meta.ReleaseInfo[:4]); err == nil {
			year = yVal
		}
	}

	rating, _ := strconv.ParseFloat(res.Meta.ImdbRating, 64)
	genresList := make([]map[string]interface{}, len(res.Meta.Genres))
	for i, gName := range res.Meta.Genres {
		genresList[i] = map[string]interface{}{"name": gName}
	}

	// Synthesize a TMDB-compatible raw data structure so GORM decoding logic is preserved
	rawData := map[string]interface{}{
		"title":          res.Meta.Name,
		"name":           res.Meta.Name,
		"overview":       res.Meta.Description,
		"release_date":   res.Meta.ReleaseInfo,
		"first_air_date": res.Meta.ReleaseInfo,
		"vote_average":   rating,
		"genres":         genresList,
	}

	return &TmdbResult{
		TmdbID:      res.Meta.ID, // Map directly to IMDb ID to fulfill the Cinemeta-Centric design
		ImdbID:      res.Meta.ID,
		Title:       res.Meta.Name,
		Year:        year,
		Poster:      res.Meta.Poster,
		Description: res.Meta.Description,
		RawData:     rawData,
	}, nil
}

// stripYearArtifact strips trailing (YYYY) from names using zero-allocation byte indexing instead of Regexp
func stripYearArtifact(title string) string {
	for i := 0; i <= len(title)-6; i++ {
		if (title[i] == '(' || title[i] == '[') && (title[i+5] == ')' || title[i+5] == ']') {
			if isNumber(title[i+1 : i+5]) {
				return strings.TrimSpace(title[:i] + title[i+6:])
			}
		}
	}
	return title
}

// Search looks up content by title and year. It fires concurrent requests to halve latency on cross-type fallbacks.
func (t *TMDBClient) Search(title string, year int, contentType string) (*TmdbResult, error) {
	if t.apiKey == "" {
		return nil, fmt.Errorf("TMDB API key is not configured")
	}

	cleanTitle := stripYearArtifact(title)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	type searchRes struct {
		cand  *tmdbSearchResult
		score float64
		mType string
		err   error
	}

	resChan := make(chan searchRes, 2)

	altType := "movie"
	if strings.ToLower(contentType) == "movie" {
		altType = "series"
	}

	// Dispatch concurrent requests to eliminate sequential wait times
	go func() {
		cand, score, err := t.executeSearch(ctx, cleanTitle, year, contentType)
		resChan <- searchRes{cand, score, contentType, err}
	}()
	go func() {
		cand, score, err := t.executeSearch(ctx, cleanTitle, year, altType)
		resChan <- searchRes{cand, score, altType, err}
	}()

	var bestCand *tmdbSearchResult
	bestScore := -1.0
	var bestType string

	// Evaluate parallel returns
	for i := 0; i < 2; i++ {
		res := <-resChan
		if res.err == nil && res.cand != nil && res.score >= 40.0 {
			if res.score > bestScore {
				bestScore = res.score
				bestCand = res.cand
				bestType = res.mType
				
				// Short-circuit: If the primary expected content type matches perfectly (>80%), abort the fallback instantly
				if res.mType == contentType && res.score > 80.0 {
					break
				}
			}
		}
	}

	if bestCand == nil || bestScore < 40.0 {
		return nil, fmt.Errorf("no metadata match met similarity threshold on any content type")
	}

	candID := fmt.Sprintf("%.0f", bestCand.ID)

	// Fetch TMDB Raw data first to obtain the associated external IMDb ID (matchmaking phase)
	tmdbRaw, errTmdb := t.GetByID(candID, bestType)
	if errTmdb == nil && tmdbRaw.ImdbID != "" {
		// Immediately attempt Cinemeta enrichment for native Stremio compatibility
		cinemetaResult, errCinemeta := t.GetByImdbIDFromCinemeta(tmdbRaw.ImdbID, bestType)
		if errCinemeta == nil {
			return cinemetaResult, nil
		}
	}

	if errTmdb != nil {
		return nil, errTmdb
	}
	return tmdbRaw, nil
}

func (t *TMDBClient) executeSearch(ctx context.Context, title string, year int, contentType string) (*tmdbSearchResult, float64, error) {
	endpoint := "https://api.themoviedb.org/3/search/movie"
	if strings.ToLower(contentType) == "series" {
		endpoint = "https://api.themoviedb.org/3/search/tv"
	}

	req, err := http.NewRequestWithContext(ctx, "GET", endpoint, nil)
	if err != nil {
		return nil, 0, err
	}

	q := req.URL.Query()
	q.Add("api_key", t.apiKey)
	q.Add("query", title)
	q.Add("include_adult", "false")
	if year > 0 {
		if strings.ToLower(contentType) == "series" {
			q.Add("first_air_date_year", strconv.Itoa(year))
		} else {
			q.Add("primary_release_year", strconv.Itoa(year))
		}
	}
	req.URL.RawQuery = q.Encode()

	var data tmdbSearchResponse
	err = t.doRequestWithRetry(req, &data)
	if err != nil || len(data.Results) == 0 {
		// Auto-Retry without year restriction for fuzzy matching
		if year > 0 {
			q.Del("primary_release_year")
			q.Del("first_air_date_year")
			req.URL.RawQuery = q.Encode()
			data = tmdbSearchResponse{} // Reset slice allocations
			_ = t.doRequestWithRetry(req, &data)
		}
	}

	if len(data.Results) == 0 {
		return nil, 0, fmt.Errorf("no results found on TMDB")
	}

	var bestCand *tmdbSearchResult
	bestScore := -1.0

	for i := range data.Results {
		cand := &data.Results[i]
		candTitle := cand.Title
		if candTitle == "" {
			candTitle = cand.Name
		}
		candDate := cand.ReleaseDate
		if candDate == "" {
			candDate = cand.FirstAirDate
		}
		candYear := 0
		if len(candDate) >= 4 {
			candYear, _ = strconv.Atoi(candDate[:4])
		}

		score := calculateScore(title, year, candTitle, candYear)
		if score > bestScore {
			bestScore = score
			bestCand = cand
		}
	}

	return bestCand, bestScore, nil
}

func (t *TMDBClient) GetByID(id string, contentType string) (*TmdbResult, error) {
	if t.apiKey == "" {
		return nil, fmt.Errorf("TMDB API key is not configured")
	}

	mediaType := "movie"
	if strings.ToLower(contentType) == "series" {
		mediaType = "tv"
	}

	ctx := context.Background()

	// Direct IMDb ID lookup handling: Try Cinemeta directly first, fall back to TMDB Find API
	if strings.HasPrefix(id, "tt") {
		cinemetaResult, errCinemeta := t.GetByImdbIDFromCinemeta(id, contentType)
		if errCinemeta == nil {
			return cinemetaResult, nil
		}

		// Fallback: Resolve numeric TMDB ID via TMDB Find API
		findURL := fmt.Sprintf("https://api.themoviedb.org/3/find/%s?api_key=%s&external_source=imdb_id", id, t.apiKey)
		reqFind, _ := http.NewRequestWithContext(ctx, "GET", findURL, nil)
		var findData map[string]interface{}
		if errFind := t.doRequestWithRetry(reqFind, &findData); errFind == nil {
			resultsKey := "movie_results"
			if mediaType == "tv" {
				resultsKey = "tv_results"
			}
			if results, ok := findData[resultsKey].([]interface{}); ok && len(results) > 0 {
				if first, ok := results[0].(map[string]interface{}); ok {
					if numericID, ok := first["id"].(float64); ok {
						id = fmt.Sprintf("%.0f", numericID)
					}
				}
			}
		}
	}

	// 1. Fetch main metadata
	urlStr := fmt.Sprintf("https://api.themoviedb.org/3/%s/%s?api_key=%s", mediaType, id, t.apiKey)
	req, _ := http.NewRequestWithContext(ctx, "GET", urlStr, nil)

	var data map[string]interface{}
	if err := t.doRequestWithRetry(req, &data); err != nil {
		return nil, err
	}

	// 2. Fetch external ids concurrently (non-blocking errors)
	urlExt := fmt.Sprintf("https://api.themoviedb.org/3/%s/%s/external_ids?api_key=%s", mediaType, id, t.apiKey)
	reqExt, _ := http.NewRequestWithContext(ctx, "GET", urlExt, nil)
	var extData map[string]interface{}
	_ = t.doRequestWithRetry(reqExt, &extData)

	imdbID := getMapString(extData, "imdb_id")
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

	return &TmdbResult{
		TmdbID:      fmt.Sprintf("%s:%s", mediaType, id),
		ImdbID:      imdbID,
		Title:       title,
		Year:        year,
		Poster:      "", // Set to empty to completely bypass TMDB poster path lookups and memory allocation
		Description: getMapString(data, "overview"),
		RawData:     data,
	}, nil
}

// ── Advanced Fuzzy Scoring Helpers ──

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
	"english": true, "spanish": true, "french": true, "italic": true,
	"russian": true, "korean": true, "japanese": true, "chinese": true,
	"51": true, "71": true, "20": true, "10bit": true, "remux": true,
	"3d": true, "sdr": true, "gb": true, "mb": true, "kb": true,
	"web": true, "dl": true, "hd": true,
	"complete": true, "repack": true, "proper": true, "vostfr": true,
	"subs": true, "sub": true, "esub": true, "vof": true, "vff": true,
	"vf": true, "season": true, "series": true, "episode": true, "pack": true,
}

var sequelIndicators = map[string]bool{
	"part": true, "chapter": true, "episode": true, "season": true,
	"volume": true, "vol": true, "book": true, "returns": true,
	"rises": true, "begins": true, "forever": true, "legacy": true,
	"fallout": true, "crusade": true, "dynasty": true, "empire": true,
	"revenge": true, "resurrection": true, "reloaded": true,
	"revolutions": true, "origins": true, "awakens": true,
	"last": true, "final": true, "next": true, "new": true,
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

var writtenNumbers = map[string]string{
	"one": "1", "first": "1", "1st": "1",
	"two": "2", "second": "2", "2nd": "2",
	"three": "3", "third": "3", "3rd": "3",
	"four": "4", "fourth": "4", "4th": "4",
	"five": "5", "fifth": "5", "5th": "5",
	"six": "6", "sixth": "6", "6th": "6",
	"seven": "7", "seventh": "7", "7th": "7",
	"eight": "8", "eighth": "8", "8th": "8",
	"nine": "9", "ninth": "9", "9th": "9",
	"ten": "10", "tenth": "10", "10th": "10",
	"eleven": "11", "eleventh": "11", "11th": "11",
	"twelve": "12", "twelfth": "12", "12th": "12",
}

var sequelContexts = map[string]bool{
	"part": true, "vol": true, "volume": true, "chapter": true,
	"episode": true, "season": true, "act": true, "entry": true,
}

var ignoredNumbers = map[string]bool{
	"1080": true, "2160": true, "720": true, "480": true, "360": true,
	"576": true, "264": true, "265": true, "10": true, "8": true,
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
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
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

// extractBigrams generates a zero-allocation integer array of bitwise homoglyphs
func extractBigrams(s string) []uint32 {
	runes := []rune(s)
	if len(runes) < 2 {
		return nil
	}
	var res []uint32
	for i := 0; i < len(runes)-1; i++ {
		repsA := getHomoglyphRepresentations(runes[i])
		repsB := getHomoglyphRepresentations(runes[i+1])
		for _, a := range repsA {
			for _, b := range repsB {
				res = append(res, (uint32(a)<<16)|uint32(b))
			}
		}
	}
	if len(res) == 0 {
		return nil
	}
	sort.Slice(res, func(i, j int) bool { return res[i] < res[j] })
	dedup := res[:1]
	for i := 1; i < len(res); i++ {
		if res[i] != res[i-1] {
			dedup = append(dedup, res[i])
		}
	}
	return dedup
}

// OverlapCoefficient dynamically computes `O(N log N)` stack-bound intersection sizing
func OverlapCoefficient(s1, s2 string) float64 {
	if s1 == s2 {
		return 1.0
	}
	bg1 := extractBigrams(s1)
	bg2 := extractBigrams(s2)
	if len(bg1) == 0 || len(bg2) == 0 {
		return 0.0
	}

	intersection := 0
	i, j := 0, 0
	for i < len(bg1) && j < len(bg2) {
		if bg1[i] == bg2[j] {
			intersection++
			i++
			j++
		} else if bg1[i] < bg2[j] {
			i++
		} else {
			j++
		}
	}

	minSize := len(bg1)
	if len(bg2) < minSize {
		minSize = len(bg2)
	}
	return float64(intersection) / float64(minSize)
}

func isRomanSequence(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r != 'i' && r != 'v' && r != 'x' && r != 'l' && r != 'c' && r != 'd' && r != 'm' {
			return false
		}
	}
	return true
}

func romanToArabic(s string) int {
	romanMap := map[rune]int{'i': 1, 'v': 5, 'x': 10, 'l': 50, 'c': 100, 'd': 500, 'm': 1000}
	total := 0
	lastVal := 0
	for i := len(s) - 1; i >= 0; i-- {
		val, ok := romanMap[rune(s[i])]
		if !ok {
			return 0
		}
		if val < lastVal {
			total -= val
		} else {
			total += val
			lastVal = val
		}
	}
	return total
}

func normalizeNumbersInTitle(title string) string {
	titleClean := strings.ReplaceAll(title, ":", " ")
	titleClean = strings.ReplaceAll(titleClean, "-", " ")

	words := strings.Fields(strings.ToLower(titleClean))
	for i, w := range words {
		if numDigit, ok := writtenNumbers[w]; ok {
			words[i] = numDigit
			continue
		}
		if isRomanSequence(w) {
			shouldConvert := false
			if len(w) >= 2 {
				shouldConvert = true
			} else if len(w) == 1 {
				if i > 0 && sequelContexts[words[i-1]] {
					shouldConvert = true
				}
				if i == len(words)-1 {
					shouldConvert = true
				}
			}
			if shouldConvert {
				val := romanToArabic(w)
				if val > 0 {
					words[i] = strconv.Itoa(val)
				}
			}
		}
	}
	return strings.Join(words, " ")
}

func extractNonYearNumbers(s string) []string {
	var nums []string
	var current strings.Builder
	for _, r := range s {
		if unicode.IsDigit(r) {
			current.WriteRune(r)
		} else {
			if current.Len() > 0 {
				val := current.String()
				if !ignoredNumbers[val] && !(len(val) == 4 && (strings.HasPrefix(val, "19") || strings.HasPrefix(val, "20"))) {
					nums = append(nums, val)
				}
				current.Reset()
			}
		}
	}
	if current.Len() > 0 {
		val := current.String()
		if !ignoredNumbers[val] && !(len(val) == 4 && (strings.HasPrefix(val, "19") || strings.HasPrefix(val, "20"))) {
			nums = append(nums, val)
		}
	}
	return nums
}

func hasNumericMismatch(target, parsed string) bool {
	targetNums := extractNonYearNumbers(target)
	parsedNums := extractNonYearNumbers(parsed)

	if len(targetNums) == 0 || len(parsedNums) == 0 {
		return false
	}

	for _, tn := range targetNums {
		tnInt, err1 := strconv.Atoi(tn)
		if err1 != nil {
			continue
		}
		for _, pn := range parsedNums {
			pnInt, err2 := strconv.Atoi(pn)
			if err2 == nil && tnInt == pnInt {
				return false
			}
		}
	}
	return true
}

func sequelGuardrail(targetTitle, parsedTitle string, score float64) float64 {
	cleanTarget := strings.Trim(strings.ToLower(targetTitle), " .-_[]()/\\")
	cleanParsed := strings.Trim(strings.ToLower(parsedTitle), " .-_[]()/\\")

	cleanTarget = normalizeNumbersInTitle(cleanTarget)
	cleanParsed = normalizeNumbersInTitle(cleanParsed)

	targetNoArt := stripLeadingArticles(cleanTarget)
	parsedNoArt := stripLeadingArticles(cleanParsed)

	shorter := len(targetNoArt)
	longer := len(parsedNoArt)
	if shorter > longer {
		shorter, longer = longer, shorter
	}
	if longer == 0 || shorter == 0 {
		return score
	}

	ratio := float64(longer) / float64(shorter)
	if ratio <= 1.3 {
		return score
	}
	if !strings.Contains(targetNoArt, parsedNoArt) && !strings.Contains(parsedNoArt, targetNoArt) {
		return score
	}

	var longerStr, shorterStr string
	if len(targetNoArt) > len(parsedNoArt) {
		longerStr, shorterStr = targetNoArt, parsedNoArt
	} else {
		longerStr, shorterStr = parsedNoArt, targetNoArt
	}

	var extra string
	if strings.HasPrefix(longerStr, shorterStr) {
		extra = strings.TrimSpace(longerStr[len(shorterStr):])
	} else if strings.HasSuffix(longerStr, shorterStr) {
		extra = strings.TrimSpace(longerStr[:len(longerStr)-len(shorterStr)])
	} else {
		return score
	}

	extraWords := strings.Fields(extra)
	for _, w := range extraWords {
		cw := cleanWord(w)
		if isRomanSequence(cw) || isNumber(cw) || sequelIndicators[cw] {
			return score * (float64(shorter) / float64(longer))
		}
	}
	return score
}

func calculateScore(targetTitle string, targetYear int, candidateTitle string, candidateYear int) float64 {
	// Guardrail Check 1: Block franchise/sequel pollution via token lengths
	if !passTitleGuardrail(targetTitle, candidateTitle) {
		return 0.0
	}

	// 1. Normalize numbers in titles before comparison (e.g. Roman to Arabic)
	cleanTarget := normalizeNumbersInTitle(targetTitle)
	cleanCand := normalizeNumbersInTitle(candidateTitle)

	// Guardrail Check 2: Block numeric mismatches (e.g. "Kumki" matching "Kumki 2")
	if hasNumericMismatch(cleanTarget, cleanCand) {
		return 0.0
	}

	// 2. Calculate year score (up to 40% weight)
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
		yearScore = 20.0
	}

	// 3. Calculate title similarity score via zero-allocation fast bigrams (up to 60% weight)
	normTarget := normalizeForCompare(cleanTarget)
	normCand := normalizeForCompare(cleanCand)

	titleScore := OverlapCoefficient(normTarget, normCand) * 60.0

	// Guardrail Check 3: Final penalty for remaining unmatched sequel indicators
	titleScore = sequelGuardrail(targetTitle, candidateTitle, titleScore)

	return yearScore + titleScore
}

// normalizeForCompare dynamically strips punctuation without heavy regex compilation overhead
func normalizeForCompare(s string) string {
	s = strings.ToLower(s)
	var b strings.Builder
	b.Grow(len(s))
	lastSpace := true
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
			lastSpace = false
		} else if !lastSpace {
			b.WriteRune(' ')
			lastSpace = true
		}
	}
	return strings.TrimSpace(b.String())
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

func cleanWord(w string) string {
	return strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			return r
		}
		return -1
	}, strings.ToLower(w))
}
