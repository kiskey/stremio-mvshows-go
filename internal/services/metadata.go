package metadata

import (
	"encoding/json"
	"fmt"
	"math"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/sevvian/smvshows-go/internal/config"
	"github.com/sevvian/smvshows-go/internal/utils"
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

func NewTMDBClient(cfg *config.Config) *TMDBClient {
	return &TMDBClient{
		client: resty.New().
			SetBaseURL("https://api.themoviedb.org/3").
			SetTimeout(8 * time.Second).
			SetRetryCount(3).
			SetRetryWaitTime(2 * time.Second),
		apiKey: cfg.TMDBAPIKey,
	}
}

// Search looks up content by title and year. It performs automatic fuzzy scoring to find the best metadata match.
func (t *TMDBClient) Search(title string, year int, contentType string) (*TmdbResult, error) {
	if t.apiKey == "" {
		return nil, fmt.Errorf("TMDB API key is not configured")
	}

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
		return nil, err
	}
	if resp.StatusCode() != 200 {
		return nil, fmt.Errorf("TMDB returned status code %d: %s", resp.StatusCode(), resp.String())
	}

	results, ok := responseMap["results"].([]interface{})
	if !ok || len(results) == 0 {
		// If query with year returned nothing, retry without year restriction for fuzzy matching
		if year > 0 {
			utils.Logger.Debug().Str("title", title).Msg("No results with year constraint. Retrying search globally without year constraint.")
			delete(params, "primary_release_year")
			delete(params, "first_air_date_year")
			resp, err = t.client.R().SetQueryParams(params).SetResult(&responseMap).Get(endpoint)
			if err == nil && resp.StatusCode() == 200 {
				results, _ = responseMap["results"].([]interface{})
			}
		}
	}

	if len(results) == 0 {
		return nil, fmt.Errorf("no results found on TMDB")
	}

	// Score candidates and pick the highest scoring result
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

	if bestCandidate == nil || bestScore < 40.0 { // 40% similarity threshold
		return nil, fmt.Errorf("no metadata match met similarity threshold")
	}

	candID := fmt.Sprintf("%.0f", bestCandidate["id"].(float64))
	return t.GetByID(candID, contentType)
}

// GetByID retrieves a detailed movie/tv payload and resolves the associated IMDb ID.
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
		return nil, fmt.Errorf("TMDB direct fetch returned status %d: %s", resp.StatusCode(), resp.String())
	}

	// Resolve the IMDb ID from /external_ids endpoint
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

// ── Private Scoring Helpers ──

func getMapString(m map[string]interface{}, key string) string {
	if val, ok := m[key]; ok && val != nil {
		if str, ok := val.(string); ok {
			return str
		}
	}
	return ""
}

func calculateScore(targetTitle string, targetYear int, candidateTitle string, candidateYear int) float64 {
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

	// 2. Calculate title similarity score (up to 60% weight)
	normTarget := normalizeForCompare(targetTitle)
	normCand := normalizeForCompare(candidateTitle)

	titleScore := 0.0
	if normTarget == normCand {
		titleScore = 60.0
	} else if strings.Contains(normTarget, normCand) || strings.Contains(normCand, normTarget) {
		titleScore = 45.0
	} else {
		// Basic word intersection comparison
		wordsTarget := strings.Fields(normTarget)
		wordsCand := strings.Fields(normCand)
		matches := 0
		for _, w1 := range wordsTarget {
			for _, w2 := range wordsCand {
				if w1 == w2 {
					matches++
					break
				}
			}
		}
		if len(wordsTarget) > 0 {
			ratio := float64(matches) / float64(len(wordsTarget))
			titleScore = ratio * 60.0
		}
	}

	return yearScore + titleScore
}

func normalizeForCompare(s string) string {
	s = strings.ToLower(s)
	// Strip special punctuation
	re := regexpMustCompile(`[^\w\s]`)
	s = re.ReplaceAllString(s, "")
	// Strip diacritics / normalization where needed
	s = strings.ReplaceAll(s, "  ", " ")
	return strings.TrimSpace(s)
}

func regexpMustCompile(pattern string) *regexpWrapper {
	// Wrap for simple static compiling to avoid external dependencies
	re := strings.NewReplacer("[^\\w\\s]", "")
	return &regexpWrapper{rep: re}
}

type regexpWrapper struct {
	rep *strings.Replacer
}

func (rw *regexpWrapper) ReplaceAllString(s, repl string) string {
	return rw.rep.Replace(s)
}
