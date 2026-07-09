// Version: 1.4.1
// Change log: Corrected unescaped backtick lexical syntax errors and replaced Perl lookarounds with pure Go RE2-compliant regular expressions to prevent compiler and runtime panics [report.md].

package metadata

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/kiskey/stremio-mvshows-go/internal/config"
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

type CinemetaSearchItem struct {
	ID          string   `json:"id"`
	Type        string   `json:"type"`
	Name        string   `json:"name"`
	Poster      string   `json:"poster,omitempty"`
	ReleaseInfo string   `json:"releaseInfo,omitempty"`
	ImdbRating  string   `json:"imdbRating,omitempty"`
	Genres      []string `json:"genres,omitempty"`
	Similarity  float64  `json:"similarity"`
}

type CinemetaCatalogResponse struct {
	Metas []CinemetaSearchItem `json:"metas"`
}

type TMDBClient struct {
	client *http.Client
	apiKey string
}

// Pure Go RE2-compliant regular expressions resolving unescaped backtick syntax errors and Perl lookaround compilation failures [report.md]
var (
	matchingBracketsRe    = regexp.MustCompile(`[()\[\]{}]`)
	matchingSpacesPunctRe = regexp.MustCompile(`\s+[,<>\/\\;:'"|` + "`" + `~!?@$%^*\_\-=]\s+`)
	matchingSuffixPunctRe = regexp.MustCompile(`[':\?,]([sm]\s|\s|$)`)
)

func createOptimizedTMDBHTTPClient(timeout time.Duration) *http.Client {
	transport := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return (&net.Dialer{
				Timeout:   5 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext(ctx, "tcp4", addr)
		},
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   3 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ForceAttemptHTTP2:     true,
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
		TmdbID:      res.Meta.ID,
		ImdbID:      res.Meta.ID,
		Title:       res.Meta.Name,
		Year:        year,
		Poster:      res.Meta.Poster,
		Description: res.Meta.Description,
		RawData:     rawData,
	}, nil
}

func (t *TMDBClient) SearchCinemeta(query string) ([]CinemetaSearchItem, error) {
	escapedQuery := url.PathEscape(query)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	type chanResult struct {
		items []CinemetaSearchItem
		err   error
	}

	ch := make(chan chanResult, 2)

	fetchCatalog := func(mType string) {
		urlStr := fmt.Sprintf("https://v3-cinemeta.strem.io/catalog/%s/top/search=%s.json", mType, escapedQuery)
		req, err := http.NewRequestWithContext(ctx, "GET", urlStr, nil)
		if err != nil {
			ch <- chanResult{nil, err}
			return
		}

		var res CinemetaCatalogResponse
		if err := t.doRequestWithRetry(req, &res); err != nil {
			ch <- chanResult{[]CinemetaSearchItem{}, nil}
			return
		}
		ch <- chanResult{res.Metas, nil}
	}

	go fetchCatalog("movie")
	go fetchCatalog("series")

	var allItems []CinemetaSearchItem
	for i := 0; i < 2; i++ {
		res := <-ch
		if res.err == nil && len(res.items) > 0 {
			allItems = append(allItems, res.items...)
		}
	}

	for i := range allItems {
		item := &allItems[i]
		candYear := 0
		if len(item.ReleaseInfo) >= 4 {
			candYear, _ = strconv.Atoi(item.ReleaseInfo[:4])
		}
		score := calculateScore(query, 0, item.Name, candYear)
		item.Similarity = score
	}

	sort.Slice(allItems, func(i, j int) bool {
		return allItems[i].Similarity > allItems[j].Similarity
	})

	if len(allItems) > 10 {
		allItems = allItems[:10]
	}

	return allItems, nil
}

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

// SearchWithAliases executes variant matching and tries &/and, moved articles, etc.
func (t *TMDBClient) SearchWithAliases(title string, year int, contentType string) (*TmdbResult, error) {
	variants := []string{title}

	if strings.HasPrefix(strings.ToLower(title), "the ") {
		variants = append(variants, strings.TrimPrefix(title, "The ")+", The")
	}
	if strings.Contains(title, "&") {
		variants = append(variants, strings.ReplaceAll(title, "&", "and"))
	}
	if strings.Contains(title, "and") {
		variants = append(variants, strings.ReplaceAll(title, "and", "&"))
	}

	var bestResult *TmdbResult
	bestScore := -1.0

	for _, variant := range variants {
		result, err := t.Search(variant, year, contentType)
		if err == nil && result != nil {
			score := calculateScore(title, year, result.Title, result.Year)
			if score > bestScore {
				bestScore = score
				bestResult = result
			}
		}
	}

	if bestResult != nil && bestScore >= 40.0 {
		return bestResult, nil
	}

	// Direct search fallback
	return t.Search(title, year, contentType)
}

func (t *TMDBClient) Search(title string, year int, contentType string) (*TmdbResult, error) {
	if t.apiKey == "" {
		return nil, fmt.Errorf("TMDB API key is not configured")
	}

	cleanTitle := stripYearArtifact(title)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cand, score, err := t.executeSearch(ctx, cleanTitle, year, contentType)
	if err != nil || cand == nil || score < 40.0 {
		return nil, fmt.Errorf("no metadata match met similarity threshold on requested content type: %s", contentType)
	}

	candID := fmt.Sprintf("%.0f", cand.ID)

	tmdbRaw, errTmdb := t.GetByID(candID, contentType)
	if errTmdb == nil && tmdbRaw.ImdbID != "" {
		cinemetaResult, errCinemeta := t.GetByImdbIDFromCinemeta(tmdbRaw.ImdbID, contentType)
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
		if year > 0 {
			q.Del("primary_release_year")
			q.Del("first_air_date_year")
			req.URL.RawQuery = q.Encode()
			data = tmdbSearchResponse{}
			_ = t.doRequestWithRetry(req, &data)
		}
	}

	if len(data.Results) == 0 {
		return nil, 0, fmt.Errorf("no results found on TMDB")
	}

	var bestCamp *tmdbSearchResult
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
			bestCamp = cand
		}
	}

	return bestCamp, bestScore, nil
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

	if strings.HasPrefix(id, "tt") {
		cinemetaResult, errCinemeta := t.GetByImdbIDFromCinemeta(id, contentType)
		if errCinemeta == nil {
			return cinemetaResult, nil
		}

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

	urlStr := fmt.Sprintf("https://api.themoviedb.org/3/%s/%s?api_key=%s", mediaType, id, t.apiKey)
	req, _ := http.NewRequestWithContext(ctx, "GET", urlStr, nil)

	var data map[string]interface{}
	if err := t.doRequestWithRetry(req, &data); err != nil {
		return nil, err
	}

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
		TmdbID:      id,
		ImdbID:      imdbID,
		Title:       title,
		Year:        year,
		Poster:      "",
		Description: getMapString(data, "overview"),
		RawData:     data,
	}, nil
}

// NormalizeTitleForMatching normalizes raw strings for match evaluation using compiled package patterns [report.md]
func NormalizeTitleForMatching(title string) string {
	if title == "" {
		return ""
	}
	s := title
	s = strings.ReplaceAll(s, "&", "and")

	s = matchingBracketsRe.ReplaceAllString(s, " ")
	s = matchingSpacesPunctRe.ReplaceAllString(s, " ")
	s = matchingSuffixPunctRe.ReplaceAllString(s, "$1")

	s = strings.Join(strings.Fields(s), " ")
	s = strings.ToLower(s)

	articles := []string{"the ", "a ", "an ", "le ", "la ", "les "}
	for _, art := range articles {
		if strings.HasPrefix(s, art) {
			s = s[len(art):]
			break
		}
	}
	return strings.TrimSpace(s)
}

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

func getHomoglyphRepresentations(r rune) []rune {
	if classes, ok := homoglyphClasses[r]; ok {
		return classes
	}
	return []rune{r}
}

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

func calculateScore(targetTitle string, targetYear int, candidateTitle string, candidateYear int) float64 {
	normTarget := NormalizeTitleForMatching(targetTitle)
	normCandidate := NormalizeTitleForMatching(candidateTitle)

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

	titleScore := OverlapCoefficient(normTarget, normCandidate) * 60.0
	return yearScore + titleScore
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
