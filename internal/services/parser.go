package parser

import (
	"crypto/sha256"
	"encoding/hex"
	"net/url"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

type ParsedMagnet struct {
	Type         string // "MOVIE", "SEASON_PACK", "EPISODE_PACK", "SINGLE_EPISODE"
	Infohash     string
	Season       int
	Episode      int
	EpisodeStart int
	EpisodeEnd   int
	Quality      string
	Language     string
}

type ParseResult struct {
	Title        string
	Season       int
	Episode      int
	EpisodeStart int
	EpisodeEnd   int
	Year         int
	Language     string
	Quality      string
	IsPack       bool
}

type CandidateFile struct {
	ID   int
	Path string
	Size int64
}

// GenerateThreadHash sorts the magnet URIs before hashing them with the title
func GenerateThreadHash(title string, magnetURIs []string) string {
	sorted := make([]string, len(magnetURIs))
	copy(sorted, magnetURIs)
	sort.Strings(sorted)
	data := title + strings.Join(sorted, "")
	h := sha256.Sum256([]byte(data))
	return hex.EncodeToString(h[:])
}

// SanitizeName cleans up bracketed text, non-ASCII patterns, and normalizes space
func SanitizeName(name string) string {
	name = strings.ToLower(name)
	// Remove content inside brackets, braces, and parentheses
	reBrackets := regexp.MustCompile(`\[[^\]]*\]|\([^\)]*\)|\{[^\}]*\}`)
	name = reBrackets.ReplaceAllString(name, " ")

	// Replace dots, underscores, dashes, and path separators with spaces
	name = strings.NewReplacer(".", " ", "_", " ", "-", " ", "/", " ", "\\", " ").Replace(name)

	// Collapse multiple spaces into one
	reSpaces := regexp.MustCompile(`\s+`)
	name = reSpaces.ReplaceAllString(name, " ")

	return strings.TrimSpace(name)
}

// ParseTitle is the entrypoint to analyze raw thread names
func ParseTitle(rawTitle string) *ParseResult {
	return RobustParseInfo(rawTitle, 1)
}

// RobustParseInfo analyzes the title to extract metadata
func RobustParseInfo(title string, fallbackSeason int) *ParseResult {
	clean := SanitizeName(title)

	res := &ParseResult{
		Title:    extractCleanTitle(title),
		Year:     extractYear(clean),
		Quality:  extractQuality(clean),
		Language: extractLanguage(clean),
		Season:   fallbackSeason,
	}

	season, epStart, epEnd, singleEp, isPack := extractSeasonAndEpisodes(clean)
	if season > 0 {
		res.Season = season
	}
	res.EpisodeStart = epStart
	res.EpisodeEnd = epEnd
	res.Episode = singleEp
	res.IsPack = isPack

	return res
}

// ParseMagnet processes a magnet URI and extracts infohash + name-based attributes
func ParseMagnet(magnetURI string, contentType string) *ParsedMagnet {
	infohash := extractInfohash(magnetURI)
	if infohash == "" {
		return nil
	}

	u, err := url.Parse(magnetURI)
	if err != nil {
		return nil
	}
	dn := u.Query().Get("dn")
	if dn == "" {
		return &ParsedMagnet{
			Type:     "SINGLE_EPISODE",
			Infohash: infohash,
			Quality:  "SD",
			Language: "ta",
		}
	}

	dn, _ = url.QueryUnescape(dn)
	// Remove common web prefixes
	rePrefix := regexp.MustCompile(`(?i)^www\.[a-z0-9-]+\.[a-z]{2,4}\s*-\s*`)
	dn = rePrefix.ReplaceAllString(dn, "")

	parsed := RobustParseInfo(dn, 1)

	pm := &ParsedMagnet{
		Infohash:     infohash,
		Quality:      parsed.Quality,
		Language:     parsed.Language,
		Season:       parsed.Season,
		Episode:      parsed.Episode,
		EpisodeStart: parsed.EpisodeStart,
		EpisodeEnd:   parsed.EpisodeEnd,
	}

	if strings.ToLower(contentType) == "movie" {
		pm.Type = "MOVIE"
		pm.Season = 0
		pm.Episode = 0
		return pm
	}

	// For series, determine the structural pack type
	if parsed.IsPack {
		if parsed.EpisodeStart > 0 && parsed.EpisodeEnd > 0 {
			pm.Type = "EPISODE_PACK"
		} else {
			pm.Type = "SEASON_PACK"
		}
	} else {
		pm.Type = "SINGLE_EPISODE"
	}

	return pm
}

// FindBestSeriesFile matches candidates for a target season and episode
func FindBestSeriesFile(candidates []CandidateFile, targetSeason, targetEpisode, fallbackSeason int) (CandidateFile, bool) {
	var matches []CandidateFile

	for _, cand := range candidates {
		// Filter out samples, trailers, extra features, and bonus files
		if isExtraOrSpecial(cand.Path) {
			continue
		}

		cleanPath := SanitizeName(cand.Path)
		season, epStart, epEnd, singleEp, isPack := extractSeasonAndEpisodes(cleanPath)
		if season == 0 {
			season = fallbackSeason
		}

		if season != targetSeason {
			continue
		}

		// Check if it's a direct match or within a range
		if singleEp == targetEpisode {
			matches = append(matches, cand)
		} else if isPack && epStart > 0 && epEnd > 0 && targetEpisode >= epStart && targetEpisode <= epEnd {
			matches = append(matches, cand)
		} else if isAbsoluteEpisodeFallback(cleanPath, targetEpisode) {
			matches = append(matches, cand)
		}
	}

	if len(matches) > 0 {
		// If multiple matches are found (e.g. multi-quality files in same torrent),
		// choose the largest one.
		sort.Slice(matches, func(i, j int) bool {
			return matches[i].Size > matches[j].Size
		})
		return matches[0], true
	}

	// Falls back to direct sequential index comparison for multi-file folders if paths are uniform
	if len(candidates) >= targetEpisode && targetEpisode > 0 {
		// Filter out non-video formats to be safe
		var videos []CandidateFile
		for _, cand := range candidates {
			if isVideo(cand.Path) && !isExtraOrSpecial(cand.Path) {
				videos = append(videos, cand)
			}
		}
		if len(videos) >= targetEpisode {
			// Sort videos alphabetically by path to map indices deterministically
			sort.Slice(videos, func(i, j int) bool {
				return strings.Compare(videos[i].Path, videos[j].Path) < 0
			})
			return videos[targetEpisode-1], true
		}
	}

	return CandidateFile{}, false
}

// ── Private Extractor Helpers ──

func extractInfohash(magnet string) string {
	re := regexp.MustCompile(`(?i)btih:([a-f0-9]{40})`)
	m := re.FindStringSubmatch(magnet)
	if len(m) > 1 {
		return strings.ToLower(m[1])
	}
	// Support 32-character base32 hashes if encountered
	re32 := regexp.MustCompile(`(?i)btih:([a-z2-7]{32})`)
	m32 := re32.FindStringSubmatch(magnet)
	if len(m32) > 1 {
		return strings.ToLower(m32[1]) // Standard client libraries handle conversion as needed
	}
	return ""
}

func extractCleanTitle(title string) string {
	// Cut off starting from year, season markers, resolution tags, or audio details
	reCut := regexp.MustCompile(`(?i)(19\d\d|20\d\d|s\d+|ep\d+|season|episode|1080p|720p|2160p|4k|webrip|bluray|hdtv|dual|multi|tamil|telugu|malayalam|hindi|kannada)`)
	idx := reCut.FindStringIndex(title)
	var clean string
	if idx != nil {
		clean = title[:idx[0]]
	} else {
		clean = title
	}
	// Clean brackets and extra whitespaces
	clean = regexp.MustCompile(`[\[\]\(\)\-\._]`).ReplaceAllString(clean, " ")
	clean = regexp.MustCompile(`\s+`).ReplaceAllString(clean, " ")
	return strings.TrimSpace(clean)
}

func extractYear(cleanTitle string) int {
	re := regexp.MustCompile(`\b(19\d\d|20\d\d)\b`)
	matches := re.FindAllString(cleanTitle, -1)
	if len(matches) > 0 {
		y, _ := strconv.Atoi(matches[len(matches)-1]) // Return the last matching year found
		return y
	}
	return 0
}

func extractQuality(cleanTitle string) string {
	if strings.Contains(cleanTitle, "2160p") || strings.Contains(cleanTitle, "4k") || strings.Contains(cleanTitle, "uhd") {
		return "4K"
	}
	if strings.Contains(cleanTitle, "1080p") || strings.Contains(cleanTitle, "fhd") {
		return "1080p"
	}
	if strings.Contains(cleanTitle, "720p") || strings.Contains(cleanTitle, "hd") {
		return "720p"
	}
	if strings.Contains(cleanTitle, "480p") || strings.Contains(cleanTitle, "sd") {
		return "480p"
	}
	return "1080p" // Safe normalized default
}

func extractLanguage(cleanTitle string) string {
	// Maps common regional/audio descriptors to ISO 639-1 language codes
	langs := map[string]string{
		"tamil":     "ta",
		"telugu":    "te",
		"malayalam": "ml",
		"kannada":   "kn",
		"hindi":     "hi",
		"english":   "en",
	}
	for kw, code := range langs {
		if strings.Contains(cleanTitle, kw) {
			return code
		}
	}
	return "ta" // Fallback to Tamil
}

func extractSeasonAndEpisodes(cleanTitle string) (season, epStart, epEnd, singleEp int, isPack bool) {
	// Try parsing Season first, e.g. "s01", "season 1"
	reSeason := regexp.MustCompile(`\b(s|season)\s*(\d+)\b`)
	if m := reSeason.FindStringSubmatch(cleanTitle); len(m) > 2 {
		season, _ = strconv.Atoi(m[2])
	}

	// Look for episode ranges, e.g. "e01 05", "e01 5", "ep01 05", "e01 to e05"
	reRange := regexp.MustCompile(`\b(?:e|ep|episode)\s*(\d+)\s*(?:-|to|\s)\s*(?:e|ep|episode)?\s*(\d+)\b`)
	if m := reRange.FindStringSubmatch(cleanTitle); len(m) > 2 {
		epStart, _ = strconv.Atoi(m[1])
		epEnd, _ = strconv.Atoi(m[2])
		isPack = true
		return
	}

	// Look for single episodes, e.g. "e01", "ep01", "episode 5"
	reSingle := regexp.MustCompile(`\b(?:e|ep|episode)\s*(\d+)\b`)
	if m := reSingle.FindStringSubmatch(cleanTitle); len(m) > 1 {
		singleEp, _ = strconv.Atoi(m[1])
		return
	}

	// Look for bare absolute episode number patterns e.g. "bigg boss 8 day 15"
	reDay := regexp.MustCompile(`\b(?:day|ep|episode)\s*(\d+)\b`)
	if m := reDay.FindStringSubmatch(cleanTitle); len(m) > 1 {
		singleEp, _ = strconv.Atoi(m[1])
		return
	}

	// Default pack check: if season exists without single episode, it is a Season Pack
	if season > 0 {
		isPack = true
	}

	return
}

func isAbsoluteEpisodeFallback(cleanPath string, targetEpisode int) bool {
	// Extracts any standalone numbers and matches them against target episode
	reStandAlone := regexp.MustCompile(`\b\d+\b`)
	matches := reStandAlone.FindAllString(cleanPath, -1)
	for _, m := range matches {
		if val, err := strconv.Atoi(m); err == nil && val == targetEpisode {
			return true
		}
	}
	return false
}

func isExtraOrSpecial(path string) bool {
	p := strings.ToLower(path)
	extras := []string{"sample", "trailer", "bonus", "behind the scenes", "featurette", "extra", "promo"}
	for _, ext := range extras {
		if strings.Contains(p, ext) {
			return true
		}
	}
	return false
}

func isVideo(path string) bool {
	ext := strings.ToLower(filepath.Ext(path))
	videoExts := map[string]bool{
		".mp4": true, ".mkv": true, ".avi": true, ".mov": true, ".flv": true, ".webm": true, ".ts": true,
	}
	return videoExts[ext]
}
