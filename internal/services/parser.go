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

	rtp "github.com/ovrlord-app/releasetitleparser"
)

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

// IsPack detects full/partial/multi-season packs from rtp.SeriesInfo
func IsPack(info *rtp.SeriesInfo) bool {
	if info == nil {
		return false
	}
	// If multiple episodes exist, or if SeasonNumber is parsed but no EpisodeNumbers exist, it is a Pack!
	return len(info.EpisodeNumbers) == 0 || len(info.EpisodeNumbers) > 1
}

// ParseTitle is a high-performance proxy to RobustParseInfo
func ParseTitle(rawTitle string) *ParseResult {
	return RobustParseInfo(rawTitle, 0)
}

// RobustParseInfo analyzes the title using standard regexes and releasetitleparser helpers
func RobustParseInfo(title string, fallbackSeason int) *ParseResult {
	clean := SanitizeName(title)

	res := &ParseResult{
		Title:    extractCleanTitle(title),
		Year:     extractYear(clean),
		Quality:  extractQuality(clean),
		Language: extractLanguage(clean),
		Season:   fallbackSeason,
	}

	// Try standard regex parsing of season and episode ranges first
	season, epStart, epEnd, singleEp, isPack := extractSeasonAndEpisodes(clean)
	if season > 0 {
		res.Season = season
	}
	res.EpisodeStart = epStart
	res.EpisodeEnd = epEnd
	res.Episode = singleEp
	res.IsPack = isPack

	// Overlay rtp parser checks for extra scene validation
	seriesInfo := rtp.ParseSeriesTitle(clean)
	if seriesInfo != nil {
		if seriesInfo.SeasonNumber > 0 {
			res.Season = seriesInfo.SeasonNumber
		}
		if len(seriesInfo.EpisodeNumbers) == 1 {
			res.Episode = seriesInfo.EpisodeNumbers[0]
			res.IsPack = false
		} else if len(seriesInfo.EpisodeNumbers) > 1 {
			res.EpisodeStart = seriesInfo.EpisodeNumbers[0]
			res.EpisodeEnd = seriesInfo.EpisodeNumbers[len(seriesInfo.EpisodeNumbers)-1]
			res.IsPack = true
		} else if seriesInfo.SeasonNumber > 0 {
			res.IsPack = true
		}
	}

	return res
}

// ParseFilePath parses individual file paths for nested episode matching
func ParseFilePath(path string, fallbackSeason int) (season, episode int, ok bool) {
	clean := SanitizeName(path)
	res := RobustParseInfo(clean, fallbackSeason)
	if res.Season > 0 {
		if res.Episode > 0 {
			return res.Season, res.Episode, true
		}
		if res.EpisodeStart > 0 {
			return res.Season, res.EpisodeStart, true
		}
	}
	return 0, 0, false
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
	dn = strings.TrimSpace(dn)

	parsed := RobustParseInfo(dn, 0)

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

	// For series, determine the structural pack type using ovrlord-app's releasetitleparser
	clean := SanitizeName(dn)
	seriesInfo := rtp.ParseSeriesTitle(clean)

	if seriesInfo != nil && (seriesInfo.SeasonNumber != 0 || len(seriesInfo.EpisodeNumbers) > 0) {
		season := seriesInfo.SeasonNumber
		if season == 0 {
			season = parsed.Season
		}

		if IsPack(seriesInfo) {
			if len(seriesInfo.EpisodeNumbers) > 1 {
				pm.Type = "EPISODE_PACK"
				pm.Season = season
				pm.EpisodeStart = seriesInfo.EpisodeNumbers[0]
				pm.EpisodeEnd = seriesInfo.EpisodeNumbers[len(seriesInfo.EpisodeNumbers)-1]
			} else {
				pm.Type = "SEASON_PACK"
				pm.Season = season
			}
		} else {
			pm.Type = "SINGLE_EPISODE"
			pm.Season = season
			if len(seriesInfo.EpisodeNumbers) > 0 {
				pm.Episode = seriesInfo.EpisodeNumbers[0]
			}
		}
	} else {
		// Fallback to regex-based parsed results
		if parsed.IsPack {
			if parsed.EpisodeStart > 0 && parsed.EpisodeEnd > 0 {
				pm.Type = "EPISODE_PACK"
			} else {
				pm.Type = "SEASON_PACK"
			}
		} else {
			pm.Type = "SINGLE_EPISODE"
		}
	}

	return pm
}

// FindBestSeriesFile matches candidates for a target season and episode, prioritising based on range, size, and sequential fallback
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
		// Sort matches by size so that we always select the highest-quality video stream (avoiding samples/extras)
		sort.Slice(matches, func(i, j int) bool {
			return matches[i].Size > matches[j].Size
		})
		return matches[0], true
	}

	// Falls back to direct sequential index comparison for absolute-numbered folder packs
	if len(candidates) >= targetEpisode && targetEpisode > 0 {
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
	return ""
}

func extractCleanTitle(title string) string {
	reCut := regexp.MustCompile(`(?i)(19\d\d|20\d\d|s\d+|ep\d+|season|episode|1080p|720p|2160p|4k|webrip|bluray|hdtv|dual|multi|tamil|telugu|malayalam|hindi|kannada)`)
	idx := reCut.FindStringIndex(title)
	var clean string
	if idx != nil {
		clean = title[:idx[0]]
	} else {
		clean = title
	}
	clean = regexp.MustCompile(`[\[\]\(\)\-\._]`).ReplaceAllString(clean, " ")
	clean = regexp.MustCompile(`\s+`).ReplaceAllString(clean, " ")
	return strings.TrimSpace(clean)
}

func extractYear(cleanTitle string) int {
	re := regexp.MustCompile(`\b(19\d\d|20\d\d)\b`)
	matches := re.FindAllString(cleanTitle, -1)
	if len(matches) > 0 {
		y, _ := strconv.Atoi(matches[len(matches)-1])
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
	return "1080p"
}

func extractLanguage(cleanTitle string) string {
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
	return "ta"
}

func extractSeasonAndEpisodes(cleanTitle string) (season, epStart, epEnd, singleEp int, isPack bool) {
	reSeason := regexp.MustCompile(`\b(s|season)\s*(\d+)\b`)
	if m := reSeason.FindStringSubmatch(cleanTitle); len(m) > 2 {
		season, _ = strconv.Atoi(m[2])
	}

	reRange := regexp.MustCompile(`\b(?:e|ep|episode)\s*(\d+)\s*(?:-|to|\s)\s*(?:e|ep|episode)?\s*(\d+)\b`)
	if m := reRange.FindStringSubmatch(cleanTitle); len(m) > 2 {
		epStart, _ = strconv.Atoi(m[1])
		epEnd, _ = strconv.Atoi(m[2])
		isPack = true
		return
	}

	reSingle := regexp.MustCompile(`\b(?:e|ep|episode)\s*(\d+)\b`)
	if m := reSingle.FindStringSubmatch(cleanTitle); len(m) > 1 {
		singleEp, _ = strconv.Atoi(m[1])
		return
	}

	reDay := regexp.MustCompile(`\b(?:day|ep|episode)\s*(\d+)\b`)
	if m := reDay.FindStringSubmatch(cleanTitle); len(m) > 1 {
		singleEp, _ = strconv.Atoi(m[1])
		return
	}

	if season > 0 {
		isPack = true
	}

	return
}

func isAbsoluteEpisodeFallback(cleanPath string, targetEpisode int) bool {
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
