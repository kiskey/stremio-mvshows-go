// Version: 1.2.0
// Change log: Fixed RobustParseInfo to parse full sanitized string for rtp, and ParseMagnet to parse magnet display names robustly, preventing missing stream indices.

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

// Patterns that identify the boundary of series/episode identifiers to truncate trailing metadata noise
var truncationRegexes = []*regexp.Regexp{
	// SONARR-GRADE UPGRADE: Multi-layered pattern capturing standard, unclosed, and non-standard trailing tags (e.g. s01ep(01 or s01e(01)
	regexp.MustCompile(`(?i)\b(?:s|season|series)?[\s\-_]*\d+[\s\-_]*(?:e|ep|episode)?\s*[\(\[]?\s*\d+.*`),
	regexp.MustCompile(`(?i)\b(?:s|season|series)[\s\-_]*\d+.*`),
	regexp.MustCompile(`(?i)\b(?:e|ep|episode)[\s\-_]*[\(\[]?\s*\d+.*`),
	regexp.MustCompile(`(?i)\b(?:complete|season\s*pack|full\s*season|all\s*episodes)\b.*`),
	regexp.MustCompile(`[\s\-_]{2,}.*`), // Truncates trailing separators like " - - "
}

// parserJunkWords defines common torrent-specific words and tags to aggressively strip from display titles.
var parserJunkWords = map[string]bool{
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
	"regular": true, "korean": true, "japanese": true, "chinese": true,
	"esub": true, "sub": true, "subs": true, "sott": true,
	"51": true, "71": true, "20": true, "10bit": true, "8bit": true,
	"remux": true, "3d": true, "sdr": true,
	"web": true, "dl": true, "hd": true, "web-dl": true, "brip": true, "rip": true, "true": true,
	"s": true, "e": true, "ep": true, "season": true, "episode": true, "pack": true, "complete": true, "full": true, "series": true, "episodes": true,
	"proper": true, "repack": true, "extended": true, "cut": true,
}

var parserStopWords = map[string]bool{
	"the": true, "a": true, "an": true, "and": true, "or": true,
	"of": true, "in": true, "on": true, "at": true, "to": true,
	"for": true, "with": true, "by": true, "from": true, "aka": true,
	"la": true, "le": true, "les": true, "el": true, "un": true, "une": true,
}

var CompiledFilters []BadgeFilter
var compileOnce sync.Once

var (
	parseCache   = make(map[string]*ParseResult)
	parseCacheMu sync.RWMutex
)

// CompileFilters processes flat regex strings into CompiledFilters once in a thread-safe context
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
	case 'À', 'Á', 'Â', 'Ã', 'Ä', 'Å', 'Ā', 'Ă', 'Ą', 'Ǎ', '?', 'Α':
		return 'A'
	case 'Æ':
		return 'E'
	case 'Ç', 'Ć', 'Ĉ', 'Ċ', 'Č':
		return 'C'
	case 'È', 'É', 'Ê', 'Ë', 'Ē', 'Ĕ', 'Ė', 'Ę', 'Ě', 'Ε':
		return 'E'
	case 'Ĝ', 'Ğ', 'Ġ', 'Ģ':
		return 'G'
	case 'Ĥ', 'Ħ':
		return 'H'
	case 'Ì', 'Í', 'Î', 'Ï', 'Ĩ', 'Ī', 'Ĭ', 'Į', 'Ǐ', 'Ι':
		return 'I'
	case 'Ĵ':
		return 'J'
	case 'Ķ':
		return 'K'
	case 'Ĺ', 'Ļ', 'Ľ', 'Ŀ', 'Ł': 
		return 'L'
	case 'Ñ', 'Ń', 'Ņ', 'Ň', 'Ŋ':
		return 'N'
	case 'Ò', 'Ó', 'Ô', 'Õ', 'Ö', 'Ō', 'Ŏ', 'Ő', 'Ǒ', 'Ǿ', 'Ο', 'Ω':
		return 'O'
	case 'Ŕ', 'Ŗ', 'Ř':
		return 'R'
	case 'Ś', 'Ŝ', 'Ş', 'Š', 'Ș':
		return 'S'
	case 'Ţ', 'Ť', 'Ț':
		return 'T'
	case 'Ù', 'Ú', 'Û', 'Ü', 'Ũ', 'Ū', 'Ŭ', 'Ů', 'Ű', 'Ų', 'Ǔ', 'Ǖ', 'Ǘ', 'Ǜ':
		return 'U'
	case 'Ŵ': 
		return 'W'
	case 'Ý', 'Ÿ', 'Ŷ':
		return 'Y'
	case 'Ź', 'Ż', 'Ž':
		return 'Z'
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
			// RADARR-GRADE GUARDRAIL:
			// If the match starts at the very beginning of the string (index 0),
			// it means the numbers are the actual title (e.g., "45 (2025)", "180 (2026)"),
			// not trailing metadata. We skip truncation in this case to prevent stripping titles.
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
			// Boundary Check Lookaround: Preserve decimals (e.g., "3.6.9") or timestamps
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

func extractYearAnchor(s string) (int, string) {
	matches := wrappedYearRegex.FindAllStringSubmatchIndex(s, -1)
	if len(matches) > 0 {
		lastMatch := matches[len(matches)-1]
		yearStr := s[lastMatch[2]:lastMatch[3]]
		if yr, err := strconv.Atoi(yearStr); err == nil && yr >= 1900 && yr <= 2030 {
			leftPart := strings.TrimSpace(s[:lastMatch[0]])
			if leftPart != "" {
				return yr, leftPart
			}
		}
	}

	plainMatches := plainYearRegex.FindAllStringSubmatchIndex(s, -1)
	if len(plainMatches) > 0 {
		for i := len(plainMatches) - 1; i >= 0; i-- {
			m := plainMatches[i]
			yearStr := s[m[2]:m[3]]
			if yr, err := strconv.Atoi(yearStr); err == nil && yr >= 1900 && yr <= 2030 {
				leftPart := strings.TrimSpace(s[:m[0]])
				if leftPart != "" {
					return yr, leftPart
				}
			}
		}
	}

	return 0, s
}

func detectRegionalLanguage(title string) string {
	for _, rp := range regionalLanguagePatterns {
		if rp.Pat.MatchString(title) {
			return rp.Lang
		}
	}
	return ""
}

// CompareNatural implements high-performance unicode-safe natural alphanumeric sorting
// to ensure sequence values (such as "2.mp4") correctly precede multi-digit equivalents (like "10.mp4") natively.
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
			return cleanOriginal
		}
		return originalTitle
	}

	return finalTitle
}

func RobustParseInfo(title string, fallbackSeason int, contentType string) *ParseResult {
	parseCacheMu.RLock()
	if cached, ok := parseCache[title]; ok {
		parseCacheMu.RUnlock()
		return cached
	}
	parseCacheMu.RUnlock()

	// Step 1: Pre-process balanced brackets to strip complex metadata blocks recursively
	balancedClean := cleanBalancedBrackets(title)
	if balancedClean == "" {
		balancedClean = title
	}

	// Step 2: Extract true Release Year and isolate Title Candidate
	extractedYear, leftTitleCandidate := extractYearAnchor(balancedClean)

	// Step 3: Check regional Indian audio track languages before they are cleaned
	detectedLang := detectRegionalLanguage(title)

	// Step 4: Run standard sanitization on isolated Title Candidate
	clean := SanitizeName(leftTitleCandidate)
	searchTitle := truncateSeriesJunk(clean)

	// Clean the full string recursively for parser matching (prevents dropping post-year metadata like Season packs)
	fullClean := SanitizeName(balancedClean)

	var res *ParseResult

	// SONARR/RADARR CONTEXT-AWARE SEPARATION:
	// If the scraped catalog explicitly defines a Movie, bypass series parsing entirely.
	// This prevents releasetitleparser from treating leading numbers as season indicators.
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
			res.Title = filterTorrentNoise(res.Title, searchTitle)
		}
	} else {
		// Series path
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
			res.Title = filterTorrentNoise(res.Title, searchTitle)
		} else {
			// Movie Fallback
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
				res.Title = filterTorrentNoise(res.Title, searchTitle)
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
		res.Title = filterTorrentNoise(res.Title, searchTitle)
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

	// Fallback: Season sorting natural alphanumeric match if file naming doesn't have explicit episode indexes
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
		// ⚡ NATURAL ALPHANUMERIC SORT MATCHING (Radarr/Sonarr Parity):
		// Sort matches using human-sort rules, guaranteeing that "2.mp4" correctly precedes "10.mp4"
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

// GenerateThreadHash produces a 100% deterministic, immutable SHA256 string
// bound strictly to the raw title. It normalizes spacing and casing to ensure
// that subsequent magnet additions overwrite existing records atomically.
func GenerateThreadHash(title string, _ []string) string {
	// 1. Normalize spaces, strip non-alphanumeric separators, and convert to lower case
	normalized := strings.ToLower(strings.TrimSpace(title))
	
	// Collapse multiple spaces to prevent variations from producing different hashes
	words := strings.Fields(normalized)
	normalized = strings.Join(words, " ")

	// 2. Generate SHA256 bytes
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

	// Robustly extract dn parameter without falling back to nil on url.Parse strict failures
	dn := ExtractMagnetDisplayName(magnetURI)

	if dn == "" {
		return &ParsedMagnet{
			Type:     "SINGLE_EPISODE",
			Infohash: infohash,
			Quality:  "sd",
			Language: "ta",
		}
	}

	dn = rePrefixRegex.ReplaceAllString(dn, "") // Use pre-compiled regex
	dn = strings.TrimSpace(dn)

	parsed := RobustParseInfo(dn, 0, contentType)

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

// FormatBadges scans the source filename exactly once and extracts matched tags.
// Results are grouped in priority layout: Resolution -> Quality -> Visual -> Audio -> Channels -> Encoder -> Streaming
func FormatBadges(title string) string {
	CompileFilters()
	var res, qual, vis, aud, ch, enc, str string

	for i := range CompiledFilters {
		f := &CompiledFilters[i]
		if f.Positive.MatchString(title) {
			// Perform lookahead-simulating logical negation assertions
			excluded := false
			for _, neg := range f.Negatives {
				if neg.MatchString(title) {
					excluded = true
					break
				}
			}
			if excluded {
				continue
			}

			switch f.GroupID {
			case "gr":
				if res == "" {
					res = f.Name
				}
			case "gq":
				if qual == "" {
					qual = f.Name
				}
			case "gv":
				if vis == "" {
					vis = f.Name
				}
			case "ga":
				if aud == "" {
					aud = f.Name
				}
			case "gc":
				if ch == "" {
					ch = f.Name
				}
			case "ge":
				if enc == "" {
					enc = f.Name
				}
			case "gs":
				if str == "" {
					str = f.Name
				}
			}
		}
	}

	// Dynamic slice building with pre-allocated hints to prevent heap allocation resizing
	parts := make([]string, 0, 7)
	if res != "" {
		parts = append(parts, "["+res+"]")
	}
	if qual != "" {
		parts = append(parts, "["+qual+"]")
	}
	if vis != "" {
		parts = append(parts, "["+vis+"]")
	}
	if aud != "" {
		parts = append(parts, "["+aud+"]")
	}
	if ch != "" {
		parts = append(parts, "["+ch+"]")
	}
	if enc != "" {
		parts = append(parts, "["+enc+"]")
	}
	if str != "" {
		parts = append(parts, "["+str+"]")
	}

	if len(parts) == 0 {
		return ""
	}
	return strings.Join(parts, " ")
}

// ExtractMagnetDisplayName extracts or resolves a safe display name (dn=) string even on unescaped raw brackets
func ExtractMagnetDisplayName(magnet string) string {
	if u, err := url.Parse(magnet); err == nil {
		if dn := u.Query().Get("dn"); dn != "" {
			if decoded, err := url.QueryUnescape(dn); err == nil {
				return decoded
			}
			return dn
		}
	}
	// Fallback Manual Parser: Splits on &dn= or ?dn= if url.Parse fails under unescaped characters
	for _, key := range []string{"?dn=", "&dn="} {
		if idx := strings.Index(magnet, key); idx != -1 {
			sub := magnet[idx+len(key):]
			if endIdx := strings.Index(sub, "&"); endIdx != -1 {
				sub = sub[:endIdx]
			}
			if decoded, err := url.QueryUnescape(sub); err == nil {
				return decoded
			}
			return sub
		}
	}
	return ""
}

func ExtractFileSize(title string) string {
	match := sizeCaptureRegex.FindString(title)
	if match != "" {
		return strings.ToUpper(strings.ReplaceAll(match, " ", ""))
	}
	return ""
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
