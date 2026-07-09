// Version: 1.4.1
// Change log: Fixed parseForumTitle to isolate the Year Anchor BEFORE calling bracket cleanups, and ensured ParseMagnet utilizes stripAllPrefixes for comprehensive title stripping.

package parser

import (
	"crypto/sha256"
	"encoding/hex"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
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

type BadgeFilter struct {
	ID        string
	GroupID   string
	Name      string
	Positive  *regexp.Regexp
	Negatives []*regexp.Regexp
}

type bracketPair struct {
	start int
	end   int
}

type ParsedRelease struct {
	ReleaseTitle    string
	CleanTitle      string
	Year            int
	SeasonNumber    int
	EpisodeNumbers  []int
	IsSeasonPack    bool
	EpisodeStart    int
	EpisodeEnd      int
	Quality         QualityInfo
	Source          string
	Resolution      string
	Languages       []string
	PrimaryLanguage string
	ReleaseGroup    string
	Edition         EditionInfo
	SpecialTags     []string
	VideoCodec      string
	AudioCodec      string
	AudioChannels   string
	IsValid         bool
	ValidationError string
}

type QualityInfo struct {
	Source     string
	Resolution string
	Modifier   string
	FullString string
}

type EditionInfo struct {
	IsIMAX         bool
	IsExtended     bool
	IsDirectorsCut bool
	IsUnrated      bool
	IsRemastered   bool
	IsCriterion    bool
	IsTheatrical   bool
	IsUncut        bool
	EditionString  string
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

var epPatternRegex = regexp.MustCompile(`(?i)(S\d+)?[\s\-_]*\bEP[\s\-_]*[\(\[]?\s*(\d+)\s*[\)\]]?\b`)
var urlRegex = regexp.MustCompile(`\b(https?://\S+|www\.\S+\.\w+|[\w.-]+@[\w.-]+)\b`)
var bracketRegex = regexp.MustCompile(`\[.*?[^\w\s-].*?\]`)
var rangeRegex = regexp.MustCompile(`(?i)\b(?:e|ep|episode)?\s*(\d+)\s*(?:-|to)\s*(?:e|ep|episode)?\s*(\d+)\b`)
var seasonFolderRegex = regexp.MustCompile(`(?i)\b(?:s|season|series)\s*0*(\d+)\b`)
var rePrefixRegex = regexp.MustCompile(`(?i)^www\.[a-z0-9-]+\.[a-z]{2,4}\s*-\s*`)
var infohashRegex = regexp.MustCompile(`(?i)btih:([a-f0-9]{40})`)
var fileSizeRegex = regexp.MustCompile(`\b\d+(\.\d+)?[gmk]b\b`)
var channelRegex = regexp.MustCompile(`\b(?:ddp)?\d\.\d(?:\.\d)?\b`)
var sizeCaptureRegex = regexp.MustCompile(`(?i)\b\d+(?:\.\d+)?\s*(?:GB|MB|KB)\b`)

var wrappedYearRegex = regexp.MustCompile(`[\(\[]((?:19|20)\d{2})[\)\]]`)
var plainYearRegex = regexp.MustCompile(`\b((?:19|20)\d{2})\b`)

var regionalLanguagePatterns = []struct {
	Lang string
	Pat  *regexp.Regexp
}{
	{"ta", regexp.MustCompile(`(?i)\b(tamil|tam|ta)\b`)},
	{"te", regexp.MustCompile(`(?i)\b(telugu|tel|te)\b`)},
	{"hi", regexp.MustCompile(`(?i)\b(hindi|hin|hi)\b`)},
	{"ml", regexp.MustCompile(`(?i)\b(malayalam|mal|ml)\b`)},
	{"kn", regexp.MustCompile(`(?i)\b(kannada|kan|kn)\b`)},
	{"en", regexp.MustCompile(`(?i)\b(english|eng|en)\b`)},
}

var truncationRegexes = []*regexp.Regexp{
	regexp.MustCompile(`(?i)\b(?:s|season|series)?[\s\-_]*\d+[\s\-_]*(?:e|ep|episode)?\s*[\(\[]?\s*\d+.*`),
	regexp.MustCompile(`(?i)\b(?:s|season|series)[\s\-_]*\d+.*`),
	regexp.MustCompile(`(?i)\b(?:e|ep|episode)[\s\-_]*[\(\[]?\s*\d+.*`),
	regexp.MustCompile(`(?i)\b(?:complete|season\s*pack|full\s*season|all\s*episodes)\b.*`),
	regexp.MustCompile(`[\s\-_]{2,}.*`),
}

var CompiledFilters []BadgeFilter
var compileOnce sync.Once

var (
	parseCache   = make(map[string]*ParseResult)
	parseCacheMu sync.RWMutex
)

func StripTrackersFromMagnet(magnet string) string {
	infohash := extractInfohash(magnet)
	if infohash == "" {
		return magnet
	}
	dn := ExtractMagnetDisplayName(magnet)
	if dn != "" {
		return "magnet:?xt=urn:btih:" + infohash + "&dn=" + url.QueryEscape(dn)
	}
	return "magnet:?xt=urn:btih:" + infohash
}

func CompileFilters() {
	compileOnce.Do(func() {
		CompiledFilters = make([]BadgeFilter, len(filtersDef))
		for i, f := range filtersDef {
			var negatives []*regexp.Regexp
			for _, negPat := range f.Negatives {
				negatives = append(negatives, regexp.MustCompile(negPat))
			}

			CompiledFilters[i] = BadgeFilter{
				ID:        f.ID,
				GroupID:   f.GroupID,
				Name:      f.Name,
				Positive:  regexp.MustCompile(f.Positive),
				Negatives: negatives,
			}
		}
	})
}

func foldRune(r rune) rune {
	switch r {
	case 'à', 'á', 'â', 'ã', 'ä', 'å', 'ā', 'ă', 'ą', 'ǎ', 'ǻ', 'α':
		return 'a'
	case 'æ':
		return 'e'
	case 'ç', 'ć', 'ĉ', 'ċ', 'č':
		return 'c'
	case 'è', 'é', 'ê', 'ë', 'ē', 'ĕ', 'ė', 'ę', 'ě', 'ε':
		return 'e'
	case 'ĝ', 'ğ', 'ġ', 'ģ':
		return 'g'
	case 'ĥ', 'ħ':
		return 'h'
	case 'ì', 'í', 'î', 'ï', 'ĩ', 'ī', 'ĭ', 'į', 'ǐ', 'ι':
		return 'i'
	case 'ĵ':
		return 'j'
	case 'ķ', 'ĸ':
		return 'k'
	case 'ĺ', 'ļ', 'ľ', 'ŀ', 'ł':
		return 'l'
	case 'ñ', 'ń', 'ņ', 'ň', 'ŉ', 'ŋ':
		return 'n'
	case 'ò', 'ó', 'ô', 'õ', 'ö', 'ō', 'ŏ', 'ő', 'ǒ', 'ǿ', 'ο', 'ω':
		return 'o'
	case 'ŕ', 'ŗ', 'ř':
		return 'r'
	case 'ś', 'ŝ', 'ş', 'š', 'ș':
		return 's'
	case 'ţ', 'ť', 'ț':
		return 't'
	case 'ù', 'ú', 'û', 'ü', 'ũ', 'ū', 'ŭ', 'ů', 'ű', 'ų', 'ǔ', 'ǖ', 'ǘ', 'ǚ', 'ǜ':
		return 'u'
	case 'ŵ':
		return 'w'
	case 'ý', 'ÿ', 'ŷ':
		return 'y'
	case 'ź', 'ż', 'ž':
		return 'z'
	}
	return r
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
	s = strings.ReplaceAll(s, "\u00a0", " ")
	s = strings.ReplaceAll(s, "\u200b", " ")
	s = normalizeEpisodePatterns(s)

	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		b.WriteRune(foldRune(r))
	}
	s = b.String()

	b.Reset()
	b.Grow(len(s))
	for _, r := range s {
		if r > unicode.MaxASCII {
			b.WriteRune(' ')
			continue
		}
		b.WriteRune(r)
	}
	s = b.String()

	s = urlRegex.ReplaceAllString(s, " ")
	s = bracketRegex.ReplaceAllString(s, " ")
	s = collapseSpaces(s)
	s = strings.TrimLeft(s, " .-_[]()/\\")
	s = strings.TrimRight(s, " .-_[]()/\\")
	return s
}

func parseEpisodeRange(s string) (int, int, bool) {
	matches := rangeRegex.FindAllStringSubmatch(s, -1)
	for _, match := range matches {
		if len(match) >= 3 {
			start, err1 := strconv.Atoi(match[1])
			end, err2 := strconv.Atoi(match[2])
			if err1 == nil && err2 == nil {
				if start < 1000 && end < 1000 && start <= end {
					return start, end, true
				}
			}
		}
	}
	return 0, 0, false
}

func truncateSeriesJunk(s string) string {
	for _, re := range truncationRegexes {
		if loc := re.FindStringIndex(s); loc != nil {
			if loc[0] == 0 {
				continue
			}
			s = s[:loc[0]]
		}
	}
	return strings.Trim(s, " .-_[]()/\\")
}

func isMetadataBlock(content string) bool {
	normalized := strings.ToLower(content)
	metadataTokens := []string{
		"1080p", "720p", "2160p", "4k", "uhd", "bluray", "bdrip", "brrip",
		"web-dl", "webdl", "webrip", "hdrip", "dvdrip", "hdtv", "hevc", "x264", "x265",
		"aac", "ddp", "dd5", "ac3", "dts", "atmos", "esub", "msub", "dubbed", "dub",
		"audios", "kbps", "untouched", "multi", "original",
	}
	for _, tok := range metadataTokens {
		if strings.Contains(normalized, tok) {
			return true
		}
	}
	if fileSizeRegex.MatchString(normalized) || sizeCaptureRegex.MatchString(normalized) {
		return true
	}
	return false
}

func cleanBalancedBrackets(input string) string {
	runes := []rune(input)
	var stack []int
	var pairs []bracketPair

	for i, r := range runes {
		if r == '[' || r == '(' {
			stack = append(stack, i)
		} else if r == ']' || r == ')' {
			if len(stack) > 0 {
				startIdx := stack[len(stack)-1]
				stack = stack[:len(stack)-1]
				if len(pairs) < 50 {
					pairs = append(pairs, bracketPair{start: startIdx, end: i})
				}
			}
		}
	}

	keep := make([]bool, len(runes))
	for i := range keep {
		keep[i] = true
	}

	for _, p := range pairs {
		if p.start >= 0 && p.end < len(runes) && p.start < p.end {
			content := string(runes[p.start+1 : p.end])
			if isMetadataBlock(content) {
				for idx := p.start; idx <= p.end; idx++ {
					keep[idx] = false
				}
			}
		}
	}

	var b strings.Builder
	b.Grow(len(runes))
	for i, r := range runes {
		if keep[i] {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func replacePunctuationSmart(input string) string {
	runes := []rune(input)
	var b strings.Builder
	b.Grow(len(runes))

	for i, r := range runes {
		if r == '(' || r == ')' || r == '[' || r == ']' || r == '-' || r == '+' || r == '/' || r == '*' || r == '!' || r == '?' || r == ',' || r == '&' {
			b.WriteRune(' ')
		} else if r == '.' || r == ':' {
			isDecimalOrTime := false
			if i > 0 && i < len(runes)-1 {
				prev := runes[i-1]
				next := runes[i+1]
				if unicode.IsDigit(prev) && unicode.IsDigit(next) {
					isDecimalOrTime = true
				}
			}
			if isDecimalOrTime {
				b.WriteRune(r)
			} else {
				b.WriteRune(' ')
			}
		} else {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func detectRegionalLanguage(title string) string {
	for _, rp := range regionalLanguagePatterns {
		if rp.Pat.MatchString(title) {
			return rp.Lang
		}
	}
	return ""
}

func CompareNatural(a, b string) bool {
	runesA := []rune(strings.ToLower(a))
	runesB := []rune(strings.ToLower(b))

	i, j := 0, 0
	for i < len(runesA) && j < len(runesB) {
		rA := runesA[i]
		rB := runesB[j]

		if unicode.IsDigit(rA) && unicode.IsDigit(rB) {
			numStartA := i
			for i < len(runesA) && unicode.IsDigit(runesA[i]) {
				i++
			}
			valA, _ := strconv.Atoi(string(runesA[numStartA:i]))

			numStartB := j
			for j < len(runesB) && unicode.IsDigit(runesB[j]) {
				j++
			}
			valB, _ := strconv.Atoi(string(runesB[numStartB:j]))

			if valA != valB {
				return valA < valB
			}
		} else {
			if rA != rB {
				return rA < rB
			}
			i++
			j++
		}
	}
	return len(runesA) < len(runesB)
}

func capitalizeTitle(s string) string {
	words := strings.Fields(s)
	for i, w := range words {
		if len(w) > 0 {
			runes := []rune(w)
			runes[0] = unicode.ToUpper(runes[0])
			words[i] = string(runes)
		}
	}
	return strings.Join(words, " ")
}

func filterTorrentNoise(title string, originalTitle string) string {
	title = collapseSpaces(strings.ToLower(title))
	title = fileSizeRegex.ReplaceAllString(title, " ")
	title = channelRegex.ReplaceAllString(title, " ")
	
	title = replacePunctuationSmart(title)

	words := strings.Fields(title)
	filteredWords := make([]string, 0, len(words))

	for _, w := range words {
		if parserJunkWords[w] || parserStopWords[w] {
			continue
		}
		if strings.HasSuffix(w, "kbps") {
			continue
		}
		if strings.Contains(w, "ddp") || strings.Contains(w, "aac") || strings.Contains(w, "dts") || strings.Contains(w, "dolby") || strings.Contains(w, "atmos") {
			continue
		}
		filteredWords = append(filteredWords, w)
	}

	finalTitle := collapseSpaces(strings.Join(filteredWords, " "))
	if finalTitle == "" {
		cleanOriginal := strings.TrimSpace(originalTitle)
		cleanOriginal = rePrefixRegex.ReplaceAllString(cleanOriginal, "")
		cleanOriginal = urlRegex.ReplaceAllString(cleanOriginal, " ")
		cleanOriginal = bracketRegex.ReplaceAllString(cleanOriginal, " ")
		cleanOriginal = collapseSpaces(cleanOriginal)
		
		yearRegex := regexp.MustCompile(`\s*[\(\[]?\d{4}[\)\]]?`)
		cleanOriginal = yearRegex.ReplaceAllString(cleanOriginal, "")
		cleanOriginal = strings.Trim(cleanOriginal, " .-_[]()/\\")
		
		if cleanOriginal != "" {
			return capitalizeTitle(cleanOriginal)
		}
		return capitalizeTitle(originalTitle)
	}

	return capitalizeTitle(finalTitle)
}

func parseForumTitle(title string, contentType string) *ParseResult {
	var year int
	var titlePart string
	var afterPart string

	// Extract Year Anchor first on raw text prior to bracket stripping (Problem 2)
	yearRegex := regexp.MustCompile(`[\(\[]((?:19|20)\d{2})[\)\]]`)
	yearLocs := yearRegex.FindAllStringSubmatchIndex(title, -1)
	if len(yearLocs) > 0 {
		loc := yearLocs[0]
		yearStr := title[loc[2]:loc[3]]
		if yr, err := strconv.Atoi(yearStr); err == nil && yr >= 1900 && yr <= 2030 {
			year = yr
			titlePart = strings.TrimSpace(title[:loc[0]])
			afterPart = strings.TrimSpace(title[loc[1]:])
		}
	} else {
		plainYearRegex := regexp.MustCompile(`\b((?:19|20)\d{2})\b`)
		plainLocs := plainYearRegex.FindAllStringSubmatchIndex(title, -1)
		for i := len(plainLocs) - 1; i >= 0; i-- {
			loc := plainLocs[i]
			yearStr := title[loc[2]:loc[3]]
			if yr, err := strconv.Atoi(yearStr); err == nil && yr >= 1900 && yr <= 2030 {
				year = yr
				titlePart = strings.TrimSpace(title[:loc[0]])
				afterPart = strings.TrimSpace(title[loc[1]:])
				break
			}
		}
	}

	if titlePart == "" {
		titlePart = title
	}

	titlePart = cleanBalancedBrackets(titlePart)
	titlePart = SanitizeName(titlePart)
	titlePart = strings.Trim(titlePart, " .-_[]()/\\")

	searchStr := afterPart
	if searchStr == "" {
		searchStr = title
	}

	season := 1
	var episode, episodeStart, episodeEnd int
	var isPack bool

	seasonRegex := regexp.MustCompile(`(?i)\b(?:s|season|series)[\s\-_]*(\d+)\b`)
	if match := seasonRegex.FindStringSubmatch(searchStr); len(match) > 1 {
		if sVal, err := strconv.Atoi(match[1]); err == nil && sVal > 0 {
			season = sVal
		}
	}

	epRangeRegex := regexp.MustCompile(`(?i)(?:s\d+)?\s*(?:ep?|episode)[\s\-_]*[\(\[]?\s*(\d+)\s*(?:-|to)\s*(\d+)\s*[\)\]]?`)
	if match := epRangeRegex.FindStringSubmatch(searchStr); len(match) > 2 {
		if start, err1 := strconv.Atoi(match[1]); err1 == nil {
			if end, err2 := strconv.Atoi(match[2]); err2 == nil && start <= end {
				episodeStart = start
				episodeEnd = end
				episode = start
				isPack = true
			}
		}
	}

	if episode == 0 {
		singleEpRegex := regexp.MustCompile(`(?i)\b(?:e|ep|episode)[\s\-_]*(\d+)\b`)
		if match := singleEpRegex.FindStringSubmatch(searchStr); len(match) > 1 {
			if epVal, err := strconv.Atoi(match[1]); err == nil {
				episode = epVal
				episodeStart = epVal
				episodeEnd = epVal
			}
		}
	}

	if episode == 0 {
		isPack = true
	}

	lang := "en"
	detectedLang := detectRegionalLanguage(title)
	if detectedLang != "" {
		lang = detectedLang
	}

	quality := "sd"
	qualityRegex := regexp.MustCompile(`(?i)\b(2160p|1080p|720p|480p|360p|4k|uhd)\b`)
	if match := qualityRegex.FindStringSubmatch(title); len(match) > 1 {
		qStr := strings.ToLower(match[1])
		if qStr == "4k" || qStr == "uhd" || qStr == "2160p" {
			quality = "4K"
		} else if qStr == "1080p" {
			quality = "1080p"
		} else if qStr == "720p" {
			quality = "720p"
		} else if qStr == "480p" {
			quality = "480p"
		} else if qStr == "360p" {
			quality = "360p"
		}
	}

	if strings.ToLower(contentType) == "movie" {
		season = 0
		episode = 0
		episodeStart = 0
		episodeEnd = 0
		isPack = false
	}

	if titlePart == "" {
		return nil
	}

	return &ParseResult{
		Title:        titlePart,
		Season:       season,
		Episode:      episode,
		Year:         year,
		Language:     lang,
		Quality:      quality,
		IsPack:       isPack,
		EpisodeStart: episodeStart,
		EpisodeEnd:   episodeEnd,
	}
}

func RobustParseInfo(title string, fallbackSeason int, contentType string) *ParseResult {
	parseCacheMu.RLock()
	if cached, ok := parseCache[title]; ok {
		parseCacheMu.RUnlock()
		return cached
	}
	parseCacheMu.RUnlock()

	if resCustom := parseForumTitle(title, contentType); resCustom != nil {
		resCustom.Title = cleanTitleOnly(resCustom.Title)
		
		parseCacheMu.Lock()
		if len(parseCache) < 10000 {
			parseCache[title] = resCustom
		}
		parseCacheMu.Unlock()
		return resCustom
	}

	balancedClean := cleanBalancedBrackets(title)
	if balancedClean == "" {
		balancedClean = title
	}

	extractedYear, leftTitleCandidate := extractTitleAndYear(balancedClean)
	detectedLang := detectRegionalLanguage(title)
	clean := SanitizeName(leftTitleCandidate)
	searchTitle := cleanTitleOnly(clean)
	fullClean := SanitizeName(balancedClean)

	var res *ParseResult

	if strings.ToLower(contentType) == "movie" {
		movie := rtp.ParseMovieTitle(fullClean)
		if movie != nil {
			lang := "en"
			if len(movie.Languages) > 0 {
				lang = getISO(movie.Languages[0])
			}
			if detectedLang != "" {
				lang = detectedLang
			}
			res = &ParseResult{
				Title:    movie.PrimaryMovieTitle(),
				Season:   0,
				Episode:  0,
				Year:     movie.Year,
				Language: lang,
				Quality:  getQuality(movie.Quality.Quality.Resolution),
			}
			res.Title = cleanTitleOnly(res.Title)
		}
	} else {
		info := rtp.ParseSeriesTitle(fullClean)
		if info != nil {
			lang := "en"
			if len(info.Languages) > 0 {
				lang = getISO(info.Languages[0])
			}
			if detectedLang != "" {
				lang = detectedLang
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
			res.Title = cleanTitleOnly(res.Title)
		} else {
			movie := rtp.ParseMovieTitle(fullClean)
			if movie != nil {
				lang := "en"
				if len(movie.Languages) > 0 {
					lang = getISO(movie.Languages[0])
				}
				if detectedLang != "" {
					lang = detectedLang
				}
				res = &ParseResult{
					Title:    movie.PrimaryMovieTitle(),
					Season:   0,
					Episode:  0,
					Year:     movie.Year,
					Language: lang,
					Quality:  getQuality(movie.Quality.Quality.Resolution),
				}
				res.Title = cleanTitleOnly(res.Title)
			}
		}
	}

	if res == nil {
		res = &ParseResult{
			Title:    searchTitle,
			Season:   fallbackSeason,
			Episode:  0,
			Language: "en",
			Quality:  "sd",
		}
		if detectedLang != "" {
			res.Language = detectedLang
		}
		res.Title = cleanTitleOnly(res.Title)
	}

	if res.Year == 0 && extractedYear != 0 {
		res.Year = extractedYear
	}

	if res.EpisodeStart == 0 && res.EpisodeEnd == 0 {
		if start, end, found := parseEpisodeRange(clean); found {
			res.EpisodeStart = start
			res.EpisodeEnd = end
			res.Episode = start
			res.IsPack = true
		}
	}

	if res.Season == 0 {
		if sMatch := seasonFolderRegex.FindStringSubmatch(clean); len(sMatch) >= 2 {
			if sVal, err := strconv.Atoi(sMatch[1]); err == nil {
				res.Season = sVal
			}
		}
	}
	if res.Season == 0 {
		res.Season = 1
	}

	parseCacheMu.Lock()
	if len(parseCache) < 10000 {
		parseCache[title] = res
	}
	parseCacheMu.Unlock()

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
			return CompareNatural(seasonMatches[i].Path, seasonMatches[j].Path)
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

func GenerateThreadHash(title string, _ []string) string {
	normalized := strings.ToLower(strings.TrimSpace(title))
	words := strings.Fields(normalized)
	normalized = strings.Join(words, " ")
	h := sha256.Sum256([]byte(normalized))
	return hex.EncodeToString(h[:])
}

func ParseTitle(rawTitle string, contentType string) *ParseResult {
	return RobustParseInfo(rawTitle, 0, contentType)
}

func ParseMagnet(magnetURI string, contentType string) *ParsedMagnet {
	infohash := extractInfohash(magnetURI)
	if infohash == "" {
		return nil
	}

	dn := ExtractMagnetDisplayName(magnetURI)
	if dn == "" {
		return &ParsedMagnet{
			Type:     "SINGLE_EPISODE",
			Infohash: infohash,
			Quality:  "sd",
			Language: "ta",
		}
	}

	dn = stripAllPrefixes(dn)

	pr := ParseRelease(dn, contentType)

	pm := &ParsedMagnet{
		Infohash:     infohash,
		Quality:      pr.Resolution,
		Language:     pr.PrimaryLanguage,
		Season:       pr.SeasonNumber,
		Episode:      0,
		EpisodeStart: 0,
		EpisodeEnd:   0,
	}
	if len(pr.EpisodeNumbers) > 0 {
		pm.Episode = pr.EpisodeNumbers[0]
	}
	if len(pr.EpisodeNumbers) > 1 {
		pm.EpisodeStart = pr.EpisodeNumbers[0]
		pm.EpisodeEnd = pr.EpisodeNumbers[len(pr.EpisodeNumbers)-1]
	} else if pr.EpisodeStart > 0 && pr.EpisodeEnd > 0 {
		pm.EpisodeStart = pr.EpisodeStart
		pm.EpisodeEnd = pr.EpisodeEnd
	}

	if strings.ToLower(contentType) == "movie" {
		pm.Type = "MOVIE"
		pm.Season = 0
		pm.Episode = 0
		return pm
	}

	if pr.IsSeasonPack {
		pm.Type = "SEASON_PACK"
	} else if len(pr.EpisodeNumbers) > 1 || (pr.EpisodeStart > 0 && pr.EpisodeEnd > 0) {
		pm.Type = "EPISODE_PACK"
	} else {
		pm.Type = "SINGLE_EPISODE"
	}

	if pm.Season == 0 {
		pm.Season = 1
	}

	return pm
}

func init() {
	_ = time.Now
}

var filtersDef = []struct {
	ID        string
	GroupID   string
	Name      string
	Positive  string
	Negatives []string
}{
	{"q-r", "gq", "Remux", `(?i)\bremux\b`, nil},
	{"q-b", "gq", "BluRay", `(?i)\b(blu[-_. ]?ray|b[rd][-_. ]?rip)\b`, []string{`(?i)\bremux\b`}},
	{"q-w", "gq", "WEB-DL", `(?i)\bweb[-_. ]?dl\b`, nil},
	{"src-webrip", "gq", "WEBRip", `(?i)\bweb[-_. ]?rip\b`, nil},
	{"src-hdtv", "gq", "HDTV", `(?i)\bhdtv\b`, nil},
	{"src-hdrip", "gq", "HDRip", `(?i)\bhd[-_. ]?rip\b`, nil},
	{"src-dvdrip", "gq", "DVDRip", `(?i)\bdvd[-_. ]?rip\b`, nil},
	{"r-4k", "gr", "4K", `(?i)\b2160[pi]?\b|\b4k\b|\buhd\b`, []string{`(?i)\b1080[pi]?\b|\b720[pi]?\b`}},
	{"r-1080", "gr", "1080p", `(?i)\b1080[pi]?\b`, nil},
	{"r-720", "gr", "720p", `(?i)\b720[pi]?\b`, nil},
	{"v-seadex", "gv", "SeaDex", `(?i)\b(seadex|best[\s\-_]?)?filter\b`, nil},
	{"v-seadex-fallback", "gv", "SeaDex", `(?i)\b(seadex|best)\b`, nil},
	{"v-seadex-gr", "gr", "SeaDex", `(?i)\b(seadex|best)\b`, nil},
}
