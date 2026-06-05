// Version: 1.0.9
// Change log: Promoted fileSizeRegex to package global, replaced the local ParseMagnet prefix regex compile with the global pattern, and migrated the custom rune array allocation in filterTorrentNoise to a zero-slice strings.Map translation.

package parser

import (
	"crypto/sha256"
	"encoding/hex"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"unicode"

	rtp "github.com/ovrlord-app/releasetitleparser"
)

type ParseResult struct {
	Title        string
	Season       int
	Episode      int
	Year         int
	Language     string
	Quality      string
	IsPack       bool
	EpisodeStart int
	EpisodeEnd   int
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

var languageToISO = map[rtp.Language]string{
	rtp.LanguageEnglish:       "en",
	rtp.LanguageSpanish:       "es",
	rtp.LanguageGerman:        "de",
	rtp.LanguageFrench:        "fr",
	rtp.LanguageItalian:       "it",
	rtp.LanguageRussian:       "ru",
	rtp.LanguageJapanese:      "ja",
	rtp.LanguageChinese:       "zh",
	rtp.LanguageKorean:        "ko",
	rtp.LanguagePortuguese:    "pt",
	rtp.LanguagePortugueseBR:  "pt-BR",
	rtp.LanguageDutch:         "nl",
	rtp.LanguageDanish:        "da",
	rtp.LanguageNorwegian:     "no",
	rtp.LanguageSwedish:       "sv",
	rtp.LanguageFinnish:       "fi",
	rtp.LanguagePolish:        "pl",
	rtp.LanguageCzech:         "cs",
	rtp.LanguageSlovak:        "sk",
	rtp.LanguageHungarian:     "hu",
	rtp.LanguageRomanian:      "ro",
	rtp.LanguageBulgarian:     "bg",
	rtp.LanguageUkrainian:     "uk",
	rtp.LanguageGreek:         "el",
	rtp.LanguageTurkish:       "tr",
	rtp.LanguageArabic:        "ar",
	rtp.LanguageHindi:         "hi",
	rtp.LanguageThai:          "th",
	rtp.LanguageVietnamese:    "vi",
	rtp.LanguageHebrew:        "he",
	rtp.LanguagePersian:       "fa",
	rtp.LanguageBengali:       "bn",
	rtp.LanguageLatvian:       "lv",
	rtp.LanguageLithuanian:    "lt",
	rtp.LanguageSpanishLatino: "es-MX",
	rtp.LanguageTamil:         "ta",
	rtp.LanguageTelugu:        "te",
	rtp.LanguageMalayalam:     "ml",
	rtp.LanguageKannada:       "kn",
	rtp.LanguageAlbanian:      "sq",
	rtp.LanguageAfrikaans:     "af",
	rtp.LanguageMarathi:       "mr",
	rtp.LanguageTagalog:       "tl",
	rtp.LanguageIcelandic:     "is",
	rtp.LanguageFlemish:       "nl-BE",
	rtp.LanguageUrdu:          "ur",
	rtp.LanguageMongolian:     "mn",
	rtp.LanguageGeorgian:      "ka",
	rtp.LanguageRomansh:       "rm",
	rtp.LanguageOriginal:      "original",
	rtp.LanguageCatalan:       "ca",
	rtp.LanguageAzerbaijani:   "az",
	rtp.LanguageUzbek:         "uz",
}

// Pre-compiled regular expressions at package-level to completely avoid hot-path compile penalties
var epPatternRegex = regexp.MustCompile(`(?i)(S\d+)?[\s\-_]*\bEP[\s\-_]*[\(\[]?\s*(\d+)\s*[\)\]]?\b`)
var urlRegex = regexp.MustCompile(`\b(https?://\S+|www\.\S+\.\w+|[\w.-]+@[\w.-]+)\b`)
var bracketRegex = regexp.MustCompile(`\[.*?[^\w\s-].*?\]`)
var rangeRegex = regexp.MustCompile(`(?i)\b(?:e|ep|episode)?\s*(\d+)\s*(?:-|to)\s*(?:e|ep|episode)?\s*(\d+)\b`)
var seasonFolderRegex = regexp.MustCompile(`(?i)\b(?:s|season|series)\s*0*(\d+)\b`)
var rePrefixRegex = regexp.MustCompile(`(?i)^www\.[a-z0-9-]+\.[a-z]{2,4}\s*-\s*`)
var infohashRegex = regexp.MustCompile(`(?i)btih:([a-f0-9]{40})`)
var fileSizeRegex = regexp.MustCompile(`\b\d+(\.\d+)?[gmk]b\b`)

// Patterns that identify the boundary of series/episode identifiers to truncate trailing metadata noise
var truncationRegexes = []*regexp.Regexp{
	regexp.MustCompile(`(?i)\b(?:s|season|series)[\s\-_]*\d+.*`),
	regexp.MustCompile(`(?i)\b(?:e|ep|episode)[\s\-_]*[\(\[]?\s*\d+.*`),
	regexp.MustCompile(`(?i)\b(?:complete|season\s*pack|full\s*season|all\s*episodes)\b.*`),
}

// parserJunkWords defines common torrent-specific words and tags to aggressively strip from display titles.
var parserJunkWords = map[string]bool{
	// Qualities/Resolutions
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
	// Audio/Subtitle/Language
	"dual": true, "audio": true, "dubbed": true, "dub": true, "multi": true,
	"hindi": true, "tamil": true, "telugu": true, "malayalam": true,
	"kannada": true, "bengali": true, "marathi": true, "punjabi": true,
	"english": true, "spanish": true, "french": true, "italic": true,
	"russian": true, "korean": true, "japanese": true, "chinese": true,
	"esub": true, "sub": true, "subs": true, "sott": true,
	// Channels/Bit Depth
	"51": true, "71": true, "20": true, "10bit": true, "8bit": true,
	// Release Types/Generic Tags
	"remux": true, "3d": true, "sdr": true,
	"web": true, "dl": true, "hd": true, "web-dl": true, "brip": true, "rip": true, "true": true,
	// Season/Episode indicators, often found as trailing junk
	"s": true, "e": true, "ep": true, "season": true, "episode": true, "pack": true, "complete": true, "full": true, "series": true, "episodes": true,
	"proper": true, "repack": true, "extended": true, "cut": true,
}

// parserStopWords are common articles/prepositions to ignore for cleaning purposes.
var parserStopWords = map[string]bool{
	"the": true, "a": true, "an": true, "and": true, "or": true,
	"of": true, "in": true, "on": true, "at": true, "to": true,
	"for": true, "with": true, "by": true, "from": true, "aka": true,
	"la": true, "le": true, "les": true, "el": true, "un": true, "une": true,
}

func normalizeEpisodePatterns(s string) string {
	return epPatternRegex.ReplaceAllString(s, "${1}E${2}")
}

func getISO(lang rtp.Language) string {
	if iso, ok := languageToISO[lang]; ok {
		return iso
	}
	return "en"
}

func getQuality(res int) string {
	switch res {
	case 2160:
		return "4K"
	case 1080:
		return "1080p"
	case 720:
		return "720p"
	case 480:
		return "480p"
	case 360:
		return "360p"
	default:
		return "sd"
	}
}

// collapseSpaces replaces multiple consecutive whitespace characters with a single space.
// Bypasses Strings.Fields slices allocation.
func collapseSpaces(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	inSpace := false
	for _, r := range s {
		if unicode.IsSpace(r) {
			if !inSpace {
				b.WriteRune(' ')
				inSpace = true
			}
		} else {
			b.WriteRune(r)
			inSpace = false
		}
	}
	return b.String()
}

func SanitizeName(name string) string {
	s := name

	// 1. Replace non-breaking spaces (\u00a0, \u200b) to standard spaces
	s = strings.ReplaceAll(s, "\u00a0", " ")
	s = strings.ReplaceAll(s, "\u200b", " ")

	// 2. Normalize episode patterns (e.g. S02 EP(15) -> S02E15)
	s = normalizeEpisodePatterns(s)

	// 3. Remove non-ASCII scripts (Chinese, Cyrillic, Japanese, etc.)
	var b strings.Builder
	for _, r := range s {
		if r > unicode.MaxASCII {
			b.WriteRune(' ')
			continue
		}
		b.WriteRune(r)
	}
	s = b.String()

	// 4. Remove residual URLs/domains (e.g. www.BTHDTV.com)
	s = urlRegex.ReplaceAllString(s, " ")

	// 5. Remove residual empty/garbage brackets
	s = bracketRegex.ReplaceAllString(s, " ")

	// 6. Collapse spaces cleanly using our zero-allocation single-pass helper
	s = collapseSpaces(s)
	
	// 7. Trim leftover leading/trailing punctuation
	s = strings.TrimLeft(s, " .-_[]()/\\")
	s = strings.TrimRight(s, " .-_[]()/\\")
	return s
}

// parseEpisodeRange extracts episode start/end indexes from strings like EP (01-08) or 01 to 07 safely.
func parseEpisodeRange(s string) (int, int, bool) {
	matches := rangeRegex.FindAllStringSubmatch(s, -1)
	for _, match := range matches {
		if len(match) >= 3 {
			start, err1 := strconv.Atoi(match[1])
			end, err2 := strconv.Atoi(match[2])
			if err1 == nil && err2 == nil {
				// Guardrail to exclude matching 4-digit release years (e.g. 2001-2008)
				if start < 1000 && end < 1000 && start <= end {
					return start, end, true
				}
			}
		}
	}
	return 0, 0, false
}

// truncateSeriesJunk trims season, episode, and complete-pack selectors and everything after them from search titles.
func truncateSeriesJunk(s string) string {
	for _, re := range truncationRegexes {
		if loc := re.FindStringIndex(s); loc != nil {
			s = s[:loc[0]]
		}
	}
	return strings.Trim(s, " .-_[]()/\\")
}

// replacePunctuation maps custom punctuation, colons, and brackets into spaces cleanly.
func replacePunctuation(r rune) rune {
	if r == '(' || r == ')' || r == '[' || r == ']' || r == '-' || r == '+' || r == '/' || r == ':' || r == ',' || r == '&' || r == '.' || r == '*' || r == '!' || r == '?' {
		return ' '
	}
	return r
}

// filterTorrentNoise aggressively cleans a title string by removing common torrent junk words and patterns.
func filterTorrentNoise(title string) string {
	// First, ensure consistent spacing and convert to lowercase for uniform processing
	title = collapseSpaces(strings.ToLower(title))
	
	// Remove common file size indicators (e.g., 2.6gb, 1.4gb, 6gb) using pre-compiled regex
	title = fileSizeRegex.ReplaceAllString(title, " ")

	// Punctuation-to-Space Isolation Sweep:
	// Assembly-optimized strings.Map replaces runes with spaces without temporary heap allocations.
	title = strings.Map(replacePunctuation, title)

	// Split into words based on spaces
	words := strings.Fields(title)
	filteredWords := make([]string, 0, len(words))

	for _, w := range words {
		// Skip if it's a known junk word or stop word. 
		// Critical Fix: Removed parserIsNumber and single-character constraints to preserve numeric/short titles (e.g. "45", "180", "12B")
		if parserJunkWords[w] || parserStopWords[w] {
			continue
		}
		
		filteredWords = append(filteredWords, w)
	}

	// Rejoin the filtered words and collapse any new multiple spaces introduced by filtering
	return collapseSpaces(strings.Join(filteredWords, " "))
}

func RobustParseInfo(title string, fallbackSeason int) *ParseResult {
	clean := SanitizeName(title)

	// Pre-clean season/episode truncation junk from the search target before analysis
	searchTitle := truncateSeriesJunk(clean)

	info := rtp.ParseSeriesTitle(searchTitle)
	
	res := &ParseResult{
		Title:    searchTitle,
		Season:   fallbackSeason,
		Episode:  0,
		Language: "en",
		Quality:  "sd",
	}

	if info != nil {
		lang := "en"
		if len(info.Languages) > 0 {
			lang = getISO(info.Languages[0])
		}
		episode := 0
		if len(info.EpisodeNumbers) > 0 {
			episode = info.EpisodeNumbers[0]
		}
		res = &ParseResult{
			Title:    info.SeriesTitle,
			Season:   info.SeasonNumber,
			Episode:  episode,
			Year:     info.SeriesTitleInfo.Year,
			Language: lang,
			Quality:  getQuality(info.Quality.Quality.Resolution),
			IsPack:   len(info.EpisodeNumbers) == 0 || len(info.EpisodeNumbers) > 1,
		}
		if len(info.EpisodeNumbers) > 1 {
			res.EpisodeStart = info.EpisodeNumbers[0]
			res.EpisodeEnd = info.EpisodeNumbers[len(info.EpisodeNumbers)-1]
		}
	} else {
		movie := rtp.ParseMovieTitle(searchTitle)
		if movie != nil {
			lang := "en"
			if len(movie.Languages) > 0 {
				lang = getISO(movie.Languages[0])
			}
			res = &ParseResult{
				Title:    movie.PrimaryMovieTitle(),
				Season:   0,
				Episode:  0,
				Year:     movie.Year,
				Language: lang,
				Quality:  getQuality(movie.Quality.Quality.Resolution),
			}
			// Apply aggressive cleaning for movie titles
			res.Title = filterTorrentNoise(res.Title)
			return res
		}
	}

	// Try custom episode range extraction if EpisodeStart and EpisodeEnd are not yet set
	if res.EpisodeStart == 0 && res.EpisodeEnd == 0 {
		if start, end, found := parseEpisodeRange(clean); found {
			res.EpisodeStart = start
			res.EpisodeEnd = end
			res.Episode = start
			res.IsPack = true
		}
	}

	// If the season is still 0, try to parse the season number from the clean title
	if res.Season == 0 {
		if sMatch := seasonFolderRegex.FindStringSubmatch(clean); len(sMatch) >= 2 {
			if sVal, err := strconv.Atoi(sMatch[1]); err == nil {
				res.Season = sVal
			}
		}
	}
	if res.Season == 0 {
		res.Season = 1 // default season fallback
	}

	// Apply aggressive cleaning to the final extracted title
	res.Title = filterTorrentNoise(res.Title)

	return res
}

func ParseFilePath(path string, fallbackSeason int) *ParseResult {
	fileName := path
	if idx := strings.LastIndexAny(path, "/\\"); idx != -1 {
		fileName = path[idx+1:]
	}

	cleanPath := normalizeEpisodePatterns(fileName)
	info := rtp.ParseSeriesPath(cleanPath)
	if info != nil && (info.SeasonNumber != 0 || len(info.EpisodeNumbers) > 0) {
		episode := 0
		if len(info.EpisodeNumbers) > 0 {
			episode = info.EpisodeNumbers[0]
		}
		season := info.SeasonNumber
		if season == 0 {
			season = fallbackSeason
		}
		res := &ParseResult{
			Title:   info.SeriesTitle,
			Season:  season,
			Episode: episode,
			IsPack:  len(info.EpisodeNumbers) == 0 || len(info.EpisodeNumbers) > 1,
		}
		if len(info.EpisodeNumbers) > 1 {
			res.EpisodeStart = info.EpisodeNumbers[0]
			res.EpisodeEnd = info.EpisodeNumbers[len(info.EpisodeNumbers)-1]
		}
		return res
	}
	return &ParseResult{
		Season:  fallbackSeason,
		Episode: 0,
	}
}

func IsPack(info *rtp.ParsedEpisodeInfo) bool {
	return info != nil && (info.FullSeason || info.IsPartialSeason || info.IsMultiSeason)
}

func isExtraOrSpecial(path string) bool {
	p := strings.ToLower(path)
	return strings.Contains(p, "special") ||
		strings.Contains(p, "bonus") ||
		strings.Contains(p, "trailer") ||
		strings.Contains(p, "featurette") ||
		strings.Contains(p, "recap") ||
		strings.Contains(p, "sample") ||
		strings.Contains(p, "extra") ||
		strings.Contains(p, "behind the scenes") ||
		strings.Contains(p, "interview")
}

func isExtraOrSpecialRelaxed(path string) bool {
	p := strings.ToLower(path)
	return strings.Contains(p, "bonus") ||
		strings.Contains(p, "trailer") ||
		strings.Contains(p, "featurette") ||
		strings.Contains(p, "recap") ||
		strings.Contains(p, "sample") ||
		strings.Contains(p, "behind the scenes") ||
		strings.Contains(p, "interview")
}

func matchRange(path string, targetEpisode int) bool {
	fileName := path
	if idx := strings.LastIndexAny(path, "/\\"); idx != -1 {
		fileName = path[idx+1:]
	}

	matches := rangeRegex.FindAllStringSubmatchIndex(fileName, -1)
	for _, match := range matches {
		if len(match) >= 6 {
			startNumStart := match[2]
			startNumEnd := match[3]
			endNumStart := match[4]
			endNumEnd := match[5]

			if startNumStart > 0 && isDecimalDot(fileName, startNumStart-1) {
				continue
			}
			if endNumEnd < len(fileName) && isDecimalDot(fileName, endNumEnd) {
				continue
			}

			startStr := fileName[startNumStart:startNumEnd]
			endStr := fileName[endNumStart:endNumEnd]

			start, err1 := strconv.Atoi(startStr)
			end, err2 := strconv.Atoi(endStr)
			if err1 == nil && err2 == nil {
				if start <= end && targetEpisode >= start && targetEpisode <= end {
					return true
				}
			}
		}
	}
	return false
}

func isDecimalDot(s string, i int) bool {
	if i <= 0 || i >= len(s)-1 {
		return false
	}
	if s[i] != '.' {
		return false
	}
	left := s[i-1]
	right := s[i+1]
	return left >= '0' && left <= '9' && right >= '0' && right <= '9'
}

func FindBestSeriesFile(candidates []CandidateFile, targetSeason, targetEpisode, fallbackSeason int) (CandidateFile, bool) {
	var bestCandidate CandidateFile
	var found bool
	var maxWeight int64 = -1

	checkExtra := isExtraOrSpecial
	if targetSeason == 0 {
		checkExtra = isExtraOrSpecialRelaxed
	}

	for _, c := range candidates {
		if checkExtra(c.Path) {
			continue
		}

		cleanPath := normalizeEpisodePatterns(c.Path)
		info := ParseFilePath(cleanPath, fallbackSeason)

		matched := false
		if info.Season == targetSeason && info.Episode == targetEpisode {
			matched = true
		}

		parsedInfo := ParseFilePath(c.Path, fallbackSeason)
		if parsedInfo.Season == targetSeason && parsedInfo.Episode == targetEpisode {
			matched = true
		}

		if !matched && info.Season == targetSeason && matchRange(c.Path, targetEpisode) {
			matched = true
		}

		if matched {
			if c.Size > maxWeight {
				bestCandidate = c
				maxWeight = c.Size
				found = true
			}
		}
	}

	if found {
		return bestCandidate, true
	}

	var seasonMatches []CandidateFile
	for _, c := range candidates {
		if checkExtra(c.Path) {
			continue
		}

		matches := seasonFolderRegex.FindAllStringSubmatch(c.Path, -1)
		isDifferentSeason := false
		for _, match := range matches {
			if len(match) >= 2 {
				sNum, err := strconv.Atoi(match[1])
				if err == nil && sNum != targetSeason {
					isDifferentSeason = true
					break
				}
			}
		}
		if isDifferentSeason {
			continue
		}

		seasonMatches = append(seasonMatches, c)
	}

	if len(seasonMatches) > 0 {
		sort.Slice(seasonMatches, func(i, j int) bool {
			return strings.Compare(strings.ToLower(seasonMatches[i].Path), strings.ToLower(seasonMatches[j].Path)) < 0
		})

		if targetEpisode > 0 && targetEpisode <= len(seasonMatches) {
			candidate := seasonMatches[targetEpisode-1]

			candParsed := ParseFilePath(candidate.Path, fallbackSeason)
			if candParsed.Episode != 0 && candParsed.Episode != targetEpisode {
				if !matchRange(candidate.Path, targetEpisode) {
					return CandidateFile{}, false
				}
			}
			return candidate, true
		}
	}

	return CandidateFile{}, false
}

func GenerateThreadHash(title string, magnetURIs []string) string {
	sorted := make([]string, len(magnetURIs))
	copy(sorted, magnetURIs)
	sort.Strings(sorted)
	data := title + strings.Join(sorted, "")
	h := sha256.Sum256([]byte(data))
	return hex.EncodeToString(h[:])
}

func ParseTitle(rawTitle string) *ParseResult {
	return RobustParseInfo(rawTitle, 0)
}

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
			Quality:  "sd",
			Language: "ta",
		}
	}

	dn, _ = url.QueryUnescape(dn)
	dn = rePrefixRegex.ReplaceAllString(dn, "") // Use pre-compiled regex
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

	// Try to extract range from raw magnet display name first (most reliable for direct episode ranges)
	if start, end, found := parseEpisodeRange(dn); found {
		pm.Type = "EPISODE_PACK"
		pm.EpisodeStart = start
		pm.EpisodeEnd = end
		pm.Episode = start
	} else {
		// Fallback to releasetitleparser structure parsing
		clean := SanitizeName(dn)
		seriesInfo := rtp.ParseSeriesTitle(clean)

		if seriesInfo != nil && (seriesInfo.SeasonNumber != 0 || len(seriesInfo.EpisodeNumbers) > 0) {
			season := seriesInfo.SeasonNumber
			if season == 0 {
				season = parsed.Season
			}

			pm.Season = season
			if len(seriesInfo.EpisodeNumbers) > 1 {
				pm.Type = "EPISODE_PACK"
				pm.EpisodeStart = seriesInfo.EpisodeNumbers[0]
				pm.EpisodeEnd = seriesInfo.EpisodeNumbers[len(seriesInfo.EpisodeNumbers)-1]
			} else if len(seriesInfo.EpisodeNumbers) == 0 {
				pm.Type = "SEASON_PACK"
			} else {
				pm.Type = "SINGLE_EPISODE"
				pm.Episode = seriesInfo.EpisodeNumbers[0]
			}
		} else {
			// Fallback to regex-based parsed results
			if parsed.IsPack {
				if parsed.EpisodeStart > 0 && parsed.EpisodeEnd > 0 {
					pm.Type = "EPISODE_PACK"
					pm.EpisodeStart = parsed.EpisodeStart
					pm.EpisodeEnd = parsed.EpisodeEnd
				} else {
					pm.Type = "SEASON_PACK"
				}
			} else {
				pm.Type = "SINGLE_EPISODE"
			}
		}
	}

	// Double check for complete season keyword triggers
	dnLower := strings.ToLower(dn)
	if strings.Contains(dnLower, "complete") || strings.Contains(dnLower, "season pack") || strings.Contains(dnLower, "full season") || strings.Contains(dnLower, "all episodes") {
		pm.Type = "SEASON_PACK"
		pm.Episode = 0
		pm.EpisodeStart = 0
		pm.EpisodeEnd = 0
	}

	// Ensure season is always set
	if pm.Season == 0 {
		pm.Season = 1
	}

	return pm
}

func extractInfohash(magnet string) string {
	m := infohashRegex.FindStringSubmatch(magnet) // Use pre-compiled regex
	if len(m) > 1 {
		return strings.ToLower(m[1])
	}
	return ""
}
