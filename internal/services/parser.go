// Version: 1.8.1
// Change log: Restructured ParseFilePath to prioritize range matching and legacy formats; integrated HTML &nbsp; and Unicode non-breaking space cleanups; enforced strict Year guardrails (1900-2030) on absolute numbering matches; appended untouched/true/hq to parserJunkWords; cleanly removed unused truncateSeriesJunk function. Added a safe filter (isVideoFile and min-size threshold) inside FindBestSeriesFile to prevent non-video clutter assets from generating alphabetical index-shifts.

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
	Type         string
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
	SeriesTitle     string
	MovieTitle      string
	Year            int
	SeasonNumber    int
	EpisodeNumbers  []int
	AbsoluteNumbers []int
	IsSeasonPack    bool
	IsMultiSeason   bool
	IsPartialSeason bool
	Quality         QualityInfo
	Languages       []string
	PrimaryLanguage string
	ReleaseGroup    string
	Edition         EditionInfo
	SpecialTags     []string
	Source          string
	Resolution      string
	VideoCodec      string
	AudioCodec      string
	AudioChannels   string
	SizeMB          int64
	IsValid         bool
	ValidationError string
	EpisodeStart    int
	EpisodeEnd      int
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

// ── Complete Package-Level Pre-compiled Regular Expressions (Zero Alloc Hot Loop) ──

var (
	// Foundational caching mechanism
	parseCache   = make(map[string]*ParseResult)
	parseCacheMu sync.RWMutex

	// Anchor elements and foundational parsing regexes
	epPatternRegex    = regexp.MustCompile(`(?i)(S\d+)?[\s\-_]*\bEP[\s\-_]*[\(\[]?\s*(\d+)\s*[\)\]]?\b`)
	urlRegex          = regexp.MustCompile(`\b(https?://\S+|www\.\S+\.\w+|[\w.-]+@[\w.-]+)\b`)
	bracketRegex      = regexp.MustCompile(`\[.*?[^\w\s-].*?\]`)
	rangeRegex        = regexp.MustCompile(`(?i)\b(?:e|ep|episode)?\s*(\d+)\s*(?:-|to)\s*(?:e|ep|episode)?\s*(\d+)\b`)
	seasonFolderRegex = regexp.MustCompile(`(?i)\b(?:s|season|series)\s*0*(\d+)\b`)
	rePrefixRegex     = regexp.MustCompile(`(?i)^www\.[a-z0-9-]+\.[a-z]{2,4}\s*-\s*`)
	infohashRegex     = regexp.MustCompile(`(?i)btih:([a-f0-9]{40})`)
	fileSizeRegex     = regexp.MustCompile(`\b\d+(\.\d+)?[gmk]b\b`)
	channelRegex      = regexp.MustCompile(`\b(?:ddp)?\d\.\d(?:\.\d)?\b`)
	sizeCaptureRegex  = regexp.MustCompile(`(?i)\b\d+(?:\.\d+)?\s*(?:GB|MB|KB)\b`)

	// Custom robust regexes for file path parsing supporting multi-digit episode numbers
	sXeXRegex         = regexp.MustCompile(`(?i)s(\d+)\s*e(\d+)`)
	sXepXRegex        = regexp.MustCompile(`(?i)s(\d+)[\s\-_]*ep(?:isode)?[\s\-_]*(\d+)`)
	epXRegex          = regexp.MustCompile(`(?i)\bep(?:isode)?[\s\-_]*[\(\[]?\s*(\d+)\s*[\)\]]?\b`)
	filePathRangeRegex = regexp.MustCompile(`(?i)\b(?:e|ep|episode)?[\s\-_]*[\(\[]?\s*(\d+)\s*(?:-|to)\s*(?:e|ep|episode)?\s*(\d+)\s*[\)\]]?\b`)

	// Standard year limits & checks
	wrappedYearRegex = regexp.MustCompile(`[\(\[]((?:19|20)\d{2})[\)\]]`)
	plainYearRegex   = regexp.MustCompile(`\b((?:19|20)\d{2})\b`)
	yearRe           = regexp.MustCompile(`[\(\[]((?:19|20)\d{2})[\)\]]`)

	// Sonarr/Radarr Naming Patterns (GAP-001, GAP-002, GAP-004)
	adjacentRe        = regexp.MustCompile(`(?i)\bs(\d{1,2})e(\d{1,3})\b`)
	dotSeparatorRe    = regexp.MustCompile(`(?i)\bs(\d{1,2})\.e(\d{1,3})(?:-e(\d{1,3}))?\b`)
	legacyXRe         = regexp.MustCompile(`(?i)\b(\d{1,2})x(\d{1,3})(?:-(\d{1,3}))?\b`)
	multiSeasonSRe    = regexp.MustCompile(`(?i)\bs(\d{1,2})-s(\d{1,2})\b`)
	multiSeasonTextRe = regexp.MustCompile(`(?i)\bseason\s*(\d{1,2})\s*-\s*(\d{1,2})\b`)
	absoluteEpRe      = regexp.MustCompile(`\b(\d{1,4})\b`)
	adjacentRangeRe   = regexp.MustCompile(`(?i)\bs(\d{1,2})e(\d{1,3})-e(\d{1,3})\b`)

	// Language models
	regionalLanguagePatterns = []struct {
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

	// Boundary truncations
	truncationRegexes = []*regexp.Regexp{
		regexp.MustCompile(`(?i)\b(?:s|season|series)?[\s\-_]*\d+[\s\-_]*(?:e|ep|episode)?\s*[\(\[]?\s*\d+.*`),
		regexp.MustCompile(`(?i)\b(?:s|season|series)[\s\-_]*\d+.*`),
		regexp.MustCompile(`(?i)\b(?:e|ep|episode)[\s\-_]*[\(\[]?\s*\d+.*`),
		regexp.MustCompile(`(?i)\b(?:complete|season\s*pack|full\s*season|all\s*episodes)\b.*`),
		regexp.MustCompile(`[\s\-_]{2,}.*`),
	}

	// Prefix stripping patterns
	prefixRe1 = regexp.MustCompile(`(?i)^\s*\[[\w.-]+\]\s*[-:]?\s*`)
	prefixRe2 = regexp.MustCompile(`(?i)^\s*(?:www\.)?[a-zA-Z0-9-]+(?:\.[a-zA-Z]{2,})+\s*[-:]\s*`)
	prefixRe3 = regexp.MustCompile(`(?i)^\s*(?:TamilMV|TamilBlasters|1TamilMV|TamilRockers|Isaimini|TamilGun|TamilYogi)\s*(?:\.\w+)?\s*[-:]\s*`)

	// Additional tuning & metadata components
	forumQualityRe  = regexp.MustCompile(`(?i)\b(2160p|1080p|720p|480p|360p|4k|uhd)\b`)
	trailingGroupRe = regexp.MustCompile(`(?i)-([a-zA-Z0-9]+)(?:\])?$`)
	leadingGroupRe  = regexp.MustCompile(`(?i)^\[([a-zA-Z0-9]+)\]`)

	// Quality checks
	qualityRemuxBDRe = regexp.MustCompile(`(?i)(?:bd|blu[-_]?ray)\s*remux`)
	qualityRemuxRe   = regexp.MustCompile(`(?i)\bremux\b`)
	qualityWebDLRe   = regexp.MustCompile(`(?i)\bweb[-_]?dl\b`)
	qualityWebRipRe  = regexp.MustCompile(`(?i)\bweb[-_]?rip\b`)
	qualityBluRayRe  = regexp.MustCompile(`(?i)\b(?:bd|blu[-_]?ray)\b`)
	qualityHdtvRe    = regexp.MustCompile(`(?i)\bhdtv\b`)
	qualityDvdRipRe  = regexp.MustCompile(`(?i)\bdvd[-_]?rip\b`)
	qualityDvdRe     = regexp.MustCompile(`(?i)\bdvd\b`)
	qualitySdtvRe    = regexp.MustCompile(`(?i)\bsdtv\b`)
	qualityCamRe     = regexp.MustCompile(`(?i)\bcam\b`)
	qualityTsRe      = regexp.MustCompile(`(?i)\bts\b`)
	qualityTcRe      = regexp.MustCompile(`(?i)\b60p\b`)

	// Resolutions
	res2160pRe = regexp.MustCompile(`(?i)\b(?:2160p|4k|uhd)\b`)
	res1080pRe = regexp.MustCompile(`(?i)\b1080p\b`)
	res720pRe  = regexp.MustCompile(`(?i)\b720p\b`)
	res480pRe  = regexp.MustCompile(`(?i)\b480p\b`)
	res576pRe  = regexp.MustCompile(`(?i)\b576p\b`)
	res360pRe  = regexp.MustCompile(`(?i)\b360p\b`)

	// Modifiers
	modDvHdr10Re = regexp.MustCompile(`(?i)\bdv\s*hdr10\b`)
	modDolbyViRe = regexp.MustCompile(`(?i)\bdolby\s*vision\b`)
	modHdr10PRe  = regexp.MustCompile(`(?i)\bhdr10plus\b`)
	modHdr10Re   = regexp.MustCompile(`(?i)\bhdr10\b`)
	modHdrRe     = regexp.MustCompile(`(?i)\bhdr\b`)

	// Multilanguage indicators
	langMultiRe = regexp.MustCompile(`(?i)\b(?:multi|dual[-_]?audio|multi[-_]?audio)\b`)

	// Edition keywords
	editionImaxRe      = regexp.MustCompile(`(?i)\bimax\b`)
	editionExtendedRe  = regexp.MustCompile(`(?i)\bextended\b`)
	editionDirectorsRe = regexp.MustCompile(`(?i)\bdirector['’]?s\s*cut\b`)
	editionUnratedRe   = regexp.MustCompile(`(?i)\bunrated\b`)
	editionRemasterRe  = regexp.MustCompile(`(?i)\bremastered\b`)

	// Special tag patterns
	specialTagPatterns = map[string]*regexp.Regexp{
		"PROPER":   regexp.MustCompile(`(?i)\bproper\b`),
		"REPACK":   regexp.MustCompile(`(?i)\brepack\b`),
		"REAL":     regexp.MustCompile(`(?i)\breal\s+proper\b`),
		"RERIP":    regexp.MustCompile(`(?i)\brerip\b`),
		"INTERNAL": regexp.MustCompile(`(?i)\binternal\b`),
		"LIMITED":  regexp.MustCompile(`(?i)\blimited\b`),
	}

	// Series parsing
	metaSeasonRe   = regexp.MustCompile(`(?i)\b(?:s|season|series)[\s\-_]*(\d+)\b`)
	metaEpRangeRe  = regexp.MustCompile(`(?i)(?:s\d+)?\s*(?:ep?|episode)[\s\-_]*[\(\[]?\s*(\d+)\s*(?:-|to)\s*(\d+)\s*[\)\]]?`)
	metaDailyRe    = regexp.MustCompile(`\b(19|20)\d{2}\s*[\.-]?\s*(0[1-9]|1[0-2])\s*[\.-]?\s*(0[1-9]|[12]\d|3[01])\b`)
	metaAnimeRe    = regexp.MustCompile(`\s+([0-9]{3,4})\s+`)
	metaSingleEpRe = regexp.MustCompile(`(?i)\b(?:e|ep|episode)[\s\-_]*(\d+)\b`)

	// Codecs
	videoCodecHevcRe = regexp.MustCompile(`(?i)\b(?:x265|hevc|h\.265)\b`)
	videoCodecAvcRe  = regexp.MustCompile(`(?i)\b(?:x264|avc|h\.264)\b`)
	videoCodecAv1Re  = regexp.MustCompile(`(?i)\bav1\b`)

	// Audio elements
	audioCodecDdpRe    = regexp.MustCompile(`(?i)\b(?:ddp|dd\+|eac3)\b`)
	audioCodecAc3Re    = regexp.MustCompile(`(?i)\b(?:ac3|dd)\b`)
	audioCodecDtsRe    = regexp.MustCompile(`(?i)\b(?:dts)\b`)
	audioCodecTrueHdRe = regexp.MustCompile(`(?i)\b(?:truehd)\b`)
	audioCodecAacRe    = regexp.MustCompile(`(?i)\b(?:aac)\b`)

	// Channels
	audioChannels71Re = regexp.MustCompile(`\b7\.1\b`)
	audioChannels51Re = regexp.MustCompile(`\b5\.1\b`)
	audioChannels20Re = regexp.MustCompile(`\b2\.0\b`)

	// Fallbacks
	fallbackYearRegex = regexp.MustCompile(`\s*[\(\[]?\d{4}[\)\]]?`)

	// Pure RE2-compliant punctuation patterns using double-escaped format
	cleanBracketsRe    = regexp.MustCompile(`[()\[\]{}]`)
	cleanSpacesPunctRe = regexp.MustCompile("\\s+[,<>\\/\\\\;:'\"|`~!?@$%^*\\_\\-=]\\s+")
	cleanSuffixPunctRe = regexp.MustCompile(`[':\?,]([sm]\s|\s|$)`)

	// Bound patterns indicators list
	compiledBoundaryPatterns []*regexp.Regexp
)

func init() {
	boundaryPatterns := []string{
		`\s+(?:2160p|1080p|720p|480p|360p|4K|UHD|HDR)\b`,
		`\s+(?:WEB[-_]?DL|WEB[-_]?Rip|Blu[-_]?Ray|BDRip|BRRip|HDTV|DVDRip|CAM|TS|TC)\b`,
		`\s+(?:AAC|AC3|DTS|DDP|DD5\.1|TrueHD|Atmos)\b`,
		`\s+(?:x264|x265|HEVC|AVC|H\.264|H\.265)\b`,
		`\s+(?:Tamil|Telugu|Hindi|Malayalam|Kannada|English|Dual|Multi)\b`,
		`\s+(?:Proper|Repack|Extended|Unrated|Remastered)\b`,
		`\s+\d+(?:\.\d+)?\s*(?:GB|MB|KB)\b`,
	}
	for _, p := range boundaryPatterns {
		compiledBoundaryPatterns = append(compiledBoundaryPatterns, regexp.MustCompile(`(?i)`+p))
	}
}

// ── Complete Language Parser Mapping ──

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

// ── Secondary Structs & Utilities ──

var CompiledFilters []BadgeFilter
var compileOnce sync.Once

var filtersDef = []struct {
	ID        string
	GroupID   string
	Name      string
	Positive  string
	Negatives []string
}{
	{ID: "hdr", GroupID: "quality", Name: "HDR", Positive: `(?i)\bhdr\b`, Negatives: []string{}},
	{ID: "dv", GroupID: "quality", Name: "DV", Positive: `(?i)\b(?:dv|dolby[-_]?vision)\b`, Negatives: []string{}},
	{ID: "1080p", GroupID: "quality", Name: "1080p", Positive: `(?i)\b1080p\b`, Negatives: []string{}},
	{ID: "720p", GroupID: "quality", Name: "720p", Positive: `(?i)\b720p\b`, Negatives: []string{}},
	{ID: "2160p", GroupID: "quality", Name: "2160p", Positive: `(?i)\b(?:2160p|4k)\b`, Negatives: []string{}},
}

// Added forum terminology elements to junkwords map (GAP-007)
var parserJunkWords = map[string]bool{
	"proper": true, "repack": true, "extended": true, "unrated": true, "remastered": true,
	"x264": true, "x265": true, "hevc": true, "avc": true, "aac": true, "ac3": true, "dts": true,
	"720p": true, "1080p": true, "2160p": true, "480p": true, "4k": true, "uhd": true,
	"webdl": true, "webrip": true, "bluray": true, "hdtv": true, "dvdrip": true,
	"gb": true, "mb": true, "kb": true, "esub": true, "sub": true, "subs": true,
	"untouched": true, "true": true, "hq": true,
}

var parserStopWords = map[string]bool{
	"the": true, "a": true, "an": true, "and": true, "or": true, "but": true, "of": true, "for": true, "with": true, "by": true, "at": true, "to": true, "in": true, "on": true,
}

type PatternLibrary struct {
	PrefixPatterns     []*regexp.Regexp
	YearInParens       *regexp.Regexp
	YearStandalone     *regexp.Regexp
	MetadataIndicators []*regexp.Regexp
	ResolutionPattern  *regexp.Regexp
	SourcePatterns     map[string]*regexp.Regexp
	ModifierPatterns   map[string]*regexp.Regexp
	SeasonPattern      *regexp.Regexp
	EpisodePattern     *regexp.Regexp
	AbsolutePattern    *regexp.Regexp
	DatePattern        *regexp.Regexp
	TrailingGroup      *regexp.Regexp
	LeadingGroup       *regexp.Regexp
	ParenGroup         *regexp.Regexp
	EditionPatterns    map[string]*regexp.Regexp
	SpecialTagPatterns map[string]*regexp.Regexp
}

func NewPatternLibrary() *PatternLibrary {
	pl := &PatternLibrary{
		SourcePatterns:     make(map[string]*regexp.Regexp),
		ModifierPatterns:   make(map[string]*regexp.Regexp),
		EditionPatterns:    make(map[string]*regexp.Regexp),
		SpecialTagPatterns: make(map[string]*regexp.Regexp),
	}

	pl.PrefixPatterns = []*regexp.Regexp{prefixRe1, prefixRe2, prefixRe3}
	pl.YearInParens = wrappedYearRegex
	pl.YearStandalone = plainYearRegex
	pl.MetadataIndicators = compiledBoundaryPatterns

	return pl
}

// ── Structural Parsing Core Methods ──

func extractInfohash(magnet string) string {
	m := infohashRegex.FindStringSubmatch(magnet)
	if len(m) > 1 {
		return strings.ToLower(m[1])
	}
	if u, err := url.Parse(magnet); err == nil {
		xt := u.Query().Get("xt")
		if strings.HasPrefix(xt, "urn:btih:") {
			return strings.ToLower(strings.TrimPrefix(xt, "urn:btih:"))
		}
	}
	return ""
}

// ExtractMagnetDisplayName parses raw query parameters directly to prevent strict url.Parse from failing on unescaped spaces or square brackets in TamilMV magnet links.
// Incorporates direct cleanup rules for both Unicode non-breaking spaces (\u00a0) and literal HTML &nbsp; entities (BUG-003 & GAP-010).
func ExtractMagnetDisplayName(magnet string) string {
	magnetClean := strings.ReplaceAll(magnet, "&nbsp;", " ")
	magnetClean = strings.ReplaceAll(magnetClean, "\u00a0", " ")

	idx := strings.Index(magnetClean, "dn=")
	if idx == -1 {
		return ""
	}
	if idx > 0 && magnetClean[idx-1] != '?' && magnetClean[idx-1] != '&' {
		return ""
	}

	val := magnetClean[idx+3:]
	if endIdx := strings.Index(val, "&"); endIdx != -1 {
		val = val[:endIdx]
	}

	if decoded, err := url.QueryUnescape(val); err == nil {
		return decoded
	}
	return val
}

func FormatBadges(title string) string {
	pr := ParseRelease(title, "movie")
	var badges []string
	if pr.Quality.FullString != "Unknown" {
		badges = append(badges, pr.Quality.FullString)
	} else if pr.Resolution != "" {
		badges = append(badges, pr.Resolution)
	} else {
		badges = append(badges, "SD")
	}
	if len(pr.Languages) > 0 {
		badges = append(badges, formatLanguage(pr.Languages[0]))
	}
	if pr.ReleaseGroup != "" {
		badges = append(badges, pr.ReleaseGroup)
	}
	return strings.Join(badges, " | ")
}

func ExtractFileSize(title string) string {
	m := sizeCaptureRegex.FindString(title)
	if m != "" {
		return strings.ToUpper(m)
	}
	return ""
}

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
	case 576:
		return "576p"
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
	s = strings.ReplaceAll(s, "&nbsp;", " ") // literal HTML &nbsp; entity (BUG-003 & GAP-010)
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

// Removed dead uncalled truncateSeriesJunk function (GAP-014)

// Check if block contains metadata
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
		
		cleanOriginal = fallbackYearRegex.ReplaceAllString(cleanOriginal, "")
		cleanOriginal = strings.Trim(cleanOriginal, " .-_[]()/\\")
		
		if cleanOriginal != "" {
			return capitalizeTitle(cleanOriginal)
		}
		return capitalizeTitle(originalTitle)
	}

	return capitalizeTitle(finalTitle)
}

// ── Overhauled Parsing Framework (Architect-Grade Parity Objects) ──

type TitleExtractor struct{}

func NewTitleExtractor() *TitleExtractor { return &TitleExtractor{} }

type TitleExtraction struct {
	RawTitle   string
	CleanTitle string
	Year       int
	Remainder  string
}

func (te *TitleExtractor) Extract(rawTitle string, contentType string) (*TitleExtraction, error) {
	working := stripAllPrefixes(rawTitle)
	
	titlePart, year := extractTitleAndYear(working)
	_ = titlePart

	yearPos := -1
	if year > 0 {
		yearRe := regexp.MustCompile(`[\(\[]?` + strconv.Itoa(year) + `[\)\]]?`)
		if loc := yearRe.FindStringIndex(working); loc != nil {
			yearPos = loc[0]
		}
	}
	
	boundary := findTitleBoundary(working, yearPos)

	cleanTitleText := working
	if boundary > 0 && boundary < len(working) {
		cleanTitleText = working[:boundary]
	}

	cleanTitleText = strings.TrimSuffix(cleanTitleText, "(")
	cleanTitleText = strings.TrimSuffix(cleanTitleText, "[")
	cleanTitleText = strings.TrimSpace(cleanTitleText)
	cleanTitleText = cleanBalancedBrackets(cleanTitleText)

	cleanTitle := cleanTitleOnly(cleanTitleText)

	remainder := ""
	if boundary > 0 && boundary < len(working) {
		remainder = working[boundary:]
	}

	return &TitleExtraction{
		RawTitle:   rawTitle,
		CleanTitle: cleanTitle,
		Year:       year,
		Remainder:  remainder,
	}, nil
}

type MetadataParser struct{}

func NewMetadataParser() *MetadataParser { return &MetadataParser{} }

type MetadataResult struct {
	Quality        QualityInfo
	Source         string
	Resolution     string
	Languages      []string
	ReleaseGroup   string
	Edition        EditionInfo
	SpecialTags    []string
	VideoCodec     string
	AudioCodec     string
	AudioChannels  string
	SeasonNumber   int
	EpisodeNumbers []int
	IsSeasonPack   bool
	EpisodeStart   int
	EpisodeEnd     int
}

func (mp *MetadataParser) Parse(remainder string, contentType string) *MetadataResult {
	qp := NewQualityParser()
	lp := NewLanguageParser()
	rgp := NewReleaseGroupParser()
	ep := NewEditionParser()

	quality := qp.ParseQuality(remainder)
	langs := lp.ParseLanguages(remainder)
	group := rgp.ParseReleaseGroup(remainder)
	edition := ep.ParseEdition(remainder)
	specialTags := ep.ParseSpecialTags(remainder)

	season := 1
	var episodes []int
	isSeasonPack := false
	var episodeStart, episodeEnd int

	if match := metaSeasonRe.FindStringSubmatch(remainder); len(match) > 1 {
		if sVal, err := strconv.Atoi(match[1]); err == nil && sVal > 0 {
			season = sVal
		}
	}

	if match := metaEpRangeRe.FindStringSubmatch(remainder); len(match) > 2 {
		if start, err1 := strconv.Atoi(match[1]); err1 == nil {
			if end, err2 := strconv.Atoi(match[2]); err2 == nil && start <= end {
				episodeStart = start
				episodeEnd = end
				isSeasonPack = false
				for ep := start; ep <= end; ep++ {
					episodes = append(episodes, ep)
				}
			}
		}
	}

	if len(episodes) == 0 {
		if start, end, found := parseEpisodeRange(remainder); found {
			episodeStart = start
			episodeEnd = end
			isSeasonPack = false
			for ep := start; ep <= end; ep++ {
				episodes = append(episodes, ep)
			}
		}
	}

	if len(episodes) == 0 {
		if match := metaSingleEpRe.FindStringSubmatch(remainder); len(match) > 1 {
			if epVal, err := strconv.Atoi(match[1]); err == nil {
				episodes = append(episodes, epVal)
				episodeStart = epVal
				episodeEnd = epVal
			}
		}
	}

	if match := metaDailyRe.FindStringSubmatch(remainder); len(match) > 0 {
		isSeasonPack = false
		if yearVal, err := strconv.Atoi(match[1]); err == nil {
			season = yearVal
		}
		if monthVal, err := strconv.Atoi(match[2]); err == nil {
			episodes = append(episodes, monthVal)
			episodeStart = monthVal
			episodeEnd = monthVal
		}
	}

	if len(episodes) == 0 {
		if match := metaAnimeRe.FindStringSubmatch(remainder); len(match) > 1 {
			if epVal, err := strconv.Atoi(match[1]); err == nil {
				episodes = append(episodes, epVal)
				episodeStart = epVal
				episodeEnd = epVal
			}
		}
	}

	if len(episodes) == 0 {
		isSeasonPack = true
		lowerRemainder := strings.ToLower(remainder)
		if strings.Contains(lowerRemainder, "complete") || strings.Contains(lowerRemainder, "season pack") || strings.Contains(lowerRemainder, "full season") || strings.Contains(lowerRemainder, "all episodes") {
			// confirmed
		}
	}

	if strings.ToLower(contentType) == "movie" {
		season = 0
		episodes = nil
		episodeStart = 0
		episodeEnd = 0
		isSeasonPack = false
	}

	return &MetadataResult{
		Quality:        quality,
		Source:         quality.Source,
		Resolution:     quality.Resolution,
		Languages:      langs,
		ReleaseGroup:   group,
		Edition:        edition,
		SpecialTags:    specialTags,
		VideoCodec:     parseVideoCodec(remainder),
		AudioCodec:     parseAudioCodec(remainder),
		AudioChannels:  parseAudioChannels(remainder),
		SeasonNumber:   season,
		EpisodeNumbers: episodes,
		IsSeasonPack:   isSeasonPack,
		EpisodeStart:   episodeStart,
		EpisodeEnd:     episodeEnd,
	}
}

func parseForumTitle(title string, contentType string) *ParseResult {
	var year int
	var titlePart string
	var afterPart string

	yearLocs := wrappedYearRegex.FindAllStringSubmatchIndex(title, -1)
	if len(yearLocs) > 0 {
		loc := yearLocs[0]
		yearStr := title[loc[2]:loc[3]]
		if yr, err := strconv.Atoi(yearStr); err == nil && yr >= 1900 && yr <= 2030 {
			year = yr
			titlePart = strings.TrimSpace(title[:loc[0]])
			afterPart = strings.TrimSpace(title[loc[1]:])
		}
	} else {
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

	if match := metaSeasonRe.FindStringSubmatch(searchStr); len(match) > 1 {
		if sVal, err := strconv.Atoi(match[1]); err == nil && sVal > 0 {
			season = sVal
		}
	}

	if match := metaEpRangeRe.FindStringSubmatch(searchStr); len(match) > 2 {
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
		if match := metaSingleEpRe.FindStringSubmatch(searchStr); len(match) > 1 {
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
	if match := forumQualityRe.FindStringSubmatch(title); len(match) > 1 {
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

	leftTitleCandidate, extractedYear := extractTitleAndYear(balancedClean)
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

func isVideoFile(path string) bool {
	p := strings.ToLower(path)
	return strings.HasSuffix(p, ".mkv") || strings.HasSuffix(p, ".mp4") || strings.HasSuffix(p, ".avi") || strings.HasSuffix(p, ".flv") || strings.HasSuffix(p, ".webm") || strings.HasSuffix(p, ".mov")
}

// ParseFilePath evaluates complex multi-episode and legacy range-based patterns first, falling back to standard single-episode formats.
// Implements absolute numbering fallbacks with year guardrails to eliminate false positives on release years (BUG-001 & GAP-005).
func ParseFilePath(path string, fallbackSeason int) *ParseResult {
	fileName := path
	if idx := strings.LastIndexAny(path, "/\\"); idx != -1 {
		fileName = path[idx+1:]
	}

	cleanPath := strings.ReplaceAll(fileName, "\u00a0", " ")
	cleanPath = strings.ReplaceAll(cleanPath, "&nbsp;", " ")
	cleanPath = strings.ReplaceAll(cleanPath, "\u200b", " ")
	cleanPath = normalizeEpisodePatterns(cleanPath)

	// 1. Evaluate range-based patterns FIRST (BUG-001)
	if m := adjacentRangeRe.FindStringSubmatch(cleanPath); len(m) > 3 {
		sVal, _ := strconv.Atoi(m[1])
		startVal, _ := strconv.Atoi(m[2])
		endVal, _ := strconv.Atoi(m[3])
		return &ParseResult{
			Season:       sVal,
			Episode:      startVal,
			EpisodeStart: startVal,
			EpisodeEnd:   endVal,
			IsPack:       true,
		}
	}

	if m := dotSeparatorRe.FindStringSubmatch(cleanPath); len(m) > 3 && m[3] != "" {
		sVal, _ := strconv.Atoi(m[1])
		startVal, _ := strconv.Atoi(m[2])
		endVal, _ := strconv.Atoi(m[3])
		return &ParseResult{
			Season:       sVal,
			Episode:      startVal,
			EpisodeStart: startVal,
			EpisodeEnd:   endVal,
			IsPack:       true,
		}
	}

	if m := legacyXRe.FindStringSubmatch(cleanPath); len(m) > 3 && m[3] != "" {
		sVal, _ := strconv.Atoi(m[1])
		startVal, _ := strconv.Atoi(m[2])
		endVal, _ := strconv.Atoi(m[3])
		return &ParseResult{
			Season:       sVal,
			Episode:      startVal,
			EpisodeStart: startVal,
			EpisodeEnd:   endVal,
			IsPack:       true,
		}
	}

	if matches := filePathRangeRegex.FindAllStringSubmatch(cleanPath, -1); len(matches) > 0 {
		match := matches[0]
		if len(match) >= 3 {
			start, err1 := strconv.Atoi(match[1])
			end, err2 := strconv.Atoi(match[2])
			if err1 == nil && err2 == nil && start <= end {
				// Safety Guardrail: Skip if numeric episode matches represent valid release years (GAP-005)
				if (start < 1900 || start > 2030) && (end < 1900 || end > 2030) {
					season := fallbackSeason
					if sMatch := regexp.MustCompile(`(?i)\bs(\d+)\b`).FindStringSubmatch(cleanPath); len(sMatch) > 1 {
						season, _ = strconv.Atoi(sMatch[1])
					}
					return &ParseResult{
						Season:       season,
						Episode:      start,
						EpisodeStart: start,
						EpisodeEnd:   end,
						IsPack:       true,
					}
				}
			}
		}
	}

	// Multi-season pack checks (GAP-004)
	if m := multiSeasonSRe.FindStringSubmatch(cleanPath); len(m) > 2 {
		startS, _ := strconv.Atoi(m[1])
		return &ParseResult{
			Season: startS,
			IsPack: true,
		}
	}
	if m := multiSeasonTextRe.FindStringSubmatch(cleanPath); len(m) > 2 {
		startS, _ := strconv.Atoi(m[1])
		return &ParseResult{
			Season: startS,
			IsPack: true,
		}
	}

	// 2. Fallback to standard single-episode matches
	if m := adjacentRe.FindStringSubmatch(cleanPath); len(m) > 2 {
		sVal, _ := strconv.Atoi(m[1])
		eVal, _ := strconv.Atoi(m[2])
		return &ParseResult{
			Season:  sVal,
			Episode: eVal,
			IsPack:  false,
		}
	}

	if m := dotSeparatorRe.FindStringSubmatch(cleanPath); len(m) > 2 {
		sVal, _ := strconv.Atoi(m[1])
		eVal, _ := strconv.Atoi(m[2])
		return &ParseResult{
			Season:  sVal,
			Episode: eVal,
			IsPack:  false,
		}
	}

	if m := legacyXRe.FindStringSubmatch(cleanPath); len(m) > 2 {
		sVal, _ := strconv.Atoi(m[1])
		eVal, _ := strconv.Atoi(m[2])
		return &ParseResult{
			Season:  sVal,
			Episode: eVal,
			IsPack:  false,
		}
	}

	if m := sXeXRegex.FindStringSubmatch(cleanPath); len(m) > 2 {
		sVal, _ := strconv.Atoi(m[1])
		eVal, _ := strconv.Atoi(m[2])
		return &ParseResult{
			Season:  sVal,
			Episode: eVal,
			IsPack:  false,
		}
	}
	if m := sXepXRegex.FindStringSubmatch(cleanPath); len(m) > 2 {
		sVal, _ := strconv.Atoi(m[1])
		eVal, _ := strconv.Atoi(m[2])
		return &ParseResult{
			Season:  sVal,
			Episode: eVal,
			IsPack:  false,
		}
	}
	if m := epXRegex.FindStringSubmatch(cleanPath); len(m) > 1 {
		eVal, _ := strconv.Atoi(m[1])
		if eVal < 1900 || eVal > 2030 {
			return &ParseResult{
				Season:  fallbackSeason,
				Episode: eVal,
				IsPack:  false,
			}
		}
	}

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
		if episode < 1900 || episode > 2030 {
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
	}

	// Fallback to absolute numbering (GAP-005 Anime Guardrail)
	if matches := absoluteEpRe.FindAllStringSubmatch(cleanPath, -1); len(matches) > 0 {
		for _, m := range matches {
			val, err := strconv.Atoi(m[1])
			if err == nil && (val < 1900 || val > 2030) {
				return &ParseResult{
					Season:  fallbackSeason,
					Episode: val,
					IsPack:  false,
				}
			}
		}
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
		if checkExtra(c.Path) || !isVideoFile(c.Path) || c.Size < 15*1024*1024 { // Guardrail: Ignore non-video and tiny assets
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
		if checkExtra(c.Path) || !isVideoFile(c.Path) || c.Size < 15*1024*1024 { // Guardrail: Apply same filters on fallback indexing
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

func stripAllPrefixes(s string) string {
	s = prefixRe1.ReplaceAllString(s, "")
	s = prefixRe2.ReplaceAllString(s, "")
	s = prefixRe3.ReplaceAllString(s, "")
	return strings.TrimSpace(s)
}

func extractTitleAndYear(s string) (title string, year int) {
	yearEnd := -1
	if m := yearRe.FindStringSubmatchIndex(s); m != nil {
		year, _ = strconv.Atoi(s[m[2]:m[3]])
		yearEnd = m[1]
	}

	if yearEnd > 0 {
		title = strings.TrimSpace(s[:yearEnd])
		title = strings.TrimSuffix(title, "(")
		title = strings.TrimSuffix(title, "[")
		title = strings.TrimSpace(title)
	} else {
		title = s
	}

	title = cleanBalancedBrackets(title)
	return title, year
}

func findTitleBoundary(s string, yearPos int) int {
	if yearPos > 0 {
		return yearPos
	}

	earliest := len(s)
	for _, re := range compiledBoundaryPatterns {
		if loc := re.FindStringIndex(s); loc != nil {
			if loc[0] < earliest && loc[0] > 0 {
				earliest = loc[0]
			}
		}
	}
	return earliest
}

func cleanTitleOnly(title string) string {
	s := strings.ReplaceAll(title, "&", "and")

	s = cleanBracketsRe.ReplaceAllString(s, " ")
	s = cleanSpacesPunctRe.ReplaceAllString(s, " ")
	s = cleanSuffixPunctRe.ReplaceAllString(s, "$1")

	s = collapseSpaces(s)
	s = strings.TrimSpace(s)
	s = moveArticleToFront(s)
	return s
}

type ReleaseGroupParser struct{}

func NewReleaseGroupParser() *ReleaseGroupParser {
	return &ReleaseGroupParser{}
}

func (rgp *ReleaseGroupParser) ParseReleaseGroup(title string) string {
	return parseReleaseGroup(title)
}

func parseReleaseGroup(title string) string {
	if m := trailingGroupRe.FindStringSubmatch(title); m != nil {
		group := m[1]
		if !isMetadataWord(group) {
			return group
		}
	}
	if m := leadingGroupRe.FindStringSubmatch(title); m != nil {
		return m[1]
	}
	return ""
}

type QualityParser struct{}

func NewQualityParser() *QualityParser { return &QualityParser{} }

func (qp *QualityParser) ParseQuality(title string) QualityInfo {
	result := QualityInfo{}
	lower := strings.ToLower(title)

	switch {
	case qualityRemuxBDRe.MatchString(lower):
		result.Source = "BluRay"
		result.Modifier = "Remux"
	case qualityRemuxRe.MatchString(lower):
		result.Modifier = "Remux"
	case qualityWebDLRe.MatchString(lower):
		result.Source = "WEB-DL"
	case qualityWebRipRe.MatchString(lower):
		result.Source = "WEBRip"
	case qualityBluRayRe.MatchString(lower):
		result.Source = "BluRay"
	case qualityHdtvRe.MatchString(lower):
		result.Source = "HDTV"
	case qualityDvdRipRe.MatchString(lower):
		result.Source = "DVD"
	case qualityDvdRe.MatchString(lower):
		result.Source = "DVD"
	case qualitySdtvRe.MatchString(lower):
		result.Source = "SDTV"
	case qualityCamRe.MatchString(lower):
		result.Source = "CAM"
	case qualityTsRe.MatchString(lower):
		result.Source = "Telesync"
	case qualityTcRe.MatchString(lower):
		result.Source = "Telecine"
	}

	switch {
	case res2160pRe.MatchString(lower):
		result.Resolution = "2160p"
	case res1080pRe.MatchString(lower):
		result.Resolution = "1080p"
	case res720pRe.MatchString(lower):
		result.Resolution = "720p"
	case res480pRe.MatchString(lower):
		result.Resolution = "480p"
	case res576pRe.MatchString(lower):
		result.Resolution = "576p"
	case res360pRe.MatchString(lower):
		result.Resolution = "360p"
	}

	switch {
	case modDvHdr10Re.MatchString(lower):
		result.Modifier = "DV HDR10"
	case modDolbyViRe.MatchString(lower):
		if result.Modifier != "" {
			result.Modifier += " DV"
		} else {
			result.Modifier = "DV"
		}
	case modHdr10PRe.MatchString(lower):
		result.Modifier = "HDR10Plus"
	case modHdr10Re.MatchString(lower):
		result.Modifier = "HDR10"
	case modHdrRe.MatchString(lower):
		if result.Modifier == "" {
			result.Modifier = "HDR"
		}
	}

	if result.Source != "" && result.Resolution != "" {
		result.FullString = result.Source + "-" + result.Resolution
		if result.Modifier != "" {
			result.FullString += " " + result.Modifier
		}
	} else if result.Resolution != "" {
		result.FullString = result.Resolution
	} else if result.Source != "" {
		result.FullString = result.Source
	} else {
		result.FullString = "Unknown"
	}

	return result
}

type LanguageParser struct {
	patterns map[string]*regexp.Regexp
}

func NewLanguageParser() *LanguageParser {
	return &LanguageParser{patterns: globalLanguagePatterns}
}

var globalLanguagePatterns = map[string]*regexp.Regexp{
	"ta": regexp.MustCompile(`(?i)\b(?:tamil|tam)\b`),
	"te": regexp.MustCompile(`(?i)\b(?:telugu|tel)\b`),
	"hi": regexp.MustCompile(`(?i)\b(?:hindi|hin)\b`),
	"ml": regexp.MustCompile(`(?i)\b(?:malayalam|mal)\b`),
	"kn": regexp.MustCompile(`(?i)\b(?:kannada|kan)\b`),
	"bn": regexp.MustCompile(`(?i)\b(?:bengali|ben)\b`),
	"mr": regexp.MustCompile(`(?i)\b(?:marathi|mar)\b`),
	"en": regexp.MustCompile(`(?i)\b(?:english|eng)\b`),
}

func (lp *LanguageParser) ParseLanguages(title string) []string {
	detected := make(map[string]bool)
	lower := " " + strings.ToLower(title) + " "
	if langMultiRe.MatchString(lower) {
		detected["multi"] = true
	}
	for code, pattern := range lp.patterns {
		if pattern.MatchString(lower) {
			detected[code] = true
		}
	}
	result := make([]string, 0, len(detected))
	for code := range detected {
		result = append(result, code)
	}
	if len(result) == 0 {
		result = append(result, "en")
	}
	return result
}

func (lp *LanguageParser) GetPrimaryLanguage(title string) string {
	langs := lp.ParseLanguages(title)
	priority := []string{"ta", "te", "hi", "ml", "kn", "bn", "mr"}
	for _, p := range priority {
		for _, l := range langs {
			if l == p {
				return p
			}
		}
	}
	return "en"
}

type EditionParser struct{}

func NewEditionParser() *EditionParser { return &EditionParser{} }

func (ep *EditionParser) ParseEdition(title string) EditionInfo {
	result := EditionInfo{}
	lower := strings.ToLower(title)

	if editionImaxRe.MatchString(lower) {
		result.IsIMAX = true
		result.EditionString += "IMAX "
	}
	if editionExtendedRe.MatchString(lower) {
		result.IsExtended = true
		result.EditionString += "Extended "
	}
	if editionDirectorsRe.MatchString(lower) {
		result.IsDirectorsCut = true
		result.EditionString += "Director's Cut "
	}
	if editionUnratedRe.MatchString(lower) {
		result.IsUnrated = true
		result.EditionString += "Unrated "
	}
	if editionRemasterRe.MatchString(lower) {
		result.IsRemastered = true
		result.EditionString += "Remastered "
	}
	result.EditionString = strings.TrimSpace(result.EditionString)
	return result
}

func (ep *EditionParser) ParseSpecialTags(title string) []string {
	tags := []string{}
	lower := " " + strings.ToLower(title) + " "
	for tag, pattern := range specialTagPatterns {
		if pattern.MatchString(lower) {
			tags = append(tags, tag)
		}
	}
	return tags
}

type Validator struct{}

func NewValidator() *Validator { return &Validator{} }

func (v *Validator) ValidateParsedRelease(pr *ParsedRelease) bool {
	if strings.TrimSpace(pr.CleanTitle) == "" {
		pr.IsValid = false
		pr.ValidationError = "Empty title after parsing"
		return false
	}
	if len(pr.CleanTitle) <= 1 {
		pr.IsValid = false
		pr.ValidationError = "Title too short"
		return false
	}
	if isAllNumbers(pr.CleanTitle) {
		pr.IsValid = false
		pr.ValidationError = "Title is all numbers"
		return false
	}
	if isAllUppercase(pr.CleanTitle) && len(pr.CleanTitle) > 3 {
		pr.IsValid = false
		pr.ValidationError = "Title appears to be metadata (all caps)"
		return false
	}
	if pr.Year != 0 && (pr.Year < 1900 || pr.Year > 2030) {
		pr.IsValid = false
		pr.ValidationError = "Year out of valid range"
		return false
	}
	pr.IsValid = true
	return true
}

func ParseRelease(rawTitle string, contentType string) *ParsedRelease {
	extractor := NewTitleExtractor()
	metaParser := NewMetadataParser()
	validator := NewValidator()

	extraction, _ := extractor.Extract(rawTitle, contentType)
	metadata := metaParser.Parse(extraction.Remainder, contentType)

	if strings.ToLower(contentType) == "series" && len(metadata.EpisodeNumbers) == 0 {
		fullMetadata := metaParser.Parse(rawTitle, contentType)
		metadata.SeasonNumber = fullMetadata.SeasonNumber
		metadata.EpisodeNumbers = fullMetadata.EpisodeNumbers
		metadata.IsSeasonPack = fullMetadata.IsSeasonPack
		metadata.EpisodeStart = fullMetadata.EpisodeStart
		metadata.EpisodeEnd = fullMetadata.EpisodeEnd
	}

	lp := NewLanguageParser()

	result := &ParsedRelease{
		ReleaseTitle:    rawTitle,
		CleanTitle:      extraction.CleanTitle,
		Year:            extraction.Year,
		SeasonNumber:    metadata.SeasonNumber,
		EpisodeNumbers:  metadata.EpisodeNumbers,
		IsSeasonPack:    metadata.IsSeasonPack,
		EpisodeStart:    metadata.EpisodeStart,
		EpisodeEnd:      metadata.EpisodeEnd,
		Quality:         metadata.Quality,
		Source:          metadata.Source,
		Resolution:      metadata.Resolution,
		Languages:       metadata.Languages,
		PrimaryLanguage: lp.GetPrimaryLanguage(rawTitle),
		ReleaseGroup:    metadata.ReleaseGroup,
		Edition:         metadata.Edition,
		SpecialTags:     metadata.SpecialTags,
		VideoCodec:      metadata.VideoCodec,
		AudioCodec:      metadata.AudioCodec,
		AudioChannels:   metadata.AudioChannels,
	}

	validator.ValidateParsedRelease(result)
	return result
}

func parseVideoCodec(s string) string {
	if videoCodecHevcRe.MatchString(s) {
		return "HEVC"
	}
	if videoCodecAvcRe.MatchString(s) {
		return "AVC"
	}
	if videoCodecAv1Re.MatchString(s) {
		return "AV1"
	}
	return ""
}

func parseAudioCodec(s string) string {
	if audioCodecDdpRe.MatchString(s) {
		return "DDP"
	}
	if audioCodecAc3Re.MatchString(s) {
		return "AC3"
	}
	if audioCodecDtsRe.MatchString(s) {
		return "DTS"
	}
	if audioCodecTrueHdRe.MatchString(s) {
		return "TrueHD"
	}
	if audioCodecAacRe.MatchString(s) {
		return "AAC"
	}
	return ""
}

func parseAudioChannels(s string) string {
	if audioChannels71Re.MatchString(s) {
		return "7.1"
	}
	if audioChannels51Re.MatchString(s) {
		return "5.1"
	}
	if audioChannels20Re.MatchString(s) {
		return "2.0"
	}
	return ""
}

// ── Secondary Parsers and Helper Functions ──

func moveArticleToFront(s string) string {
	articles := []string{", The", ", A", ", An", ", Le", ", La", ", Les", ", L'"}
	for _, article := range articles {
		if strings.HasSuffix(strings.ToLower(s), strings.ToLower(article)) {
			base := strings.TrimSpace(s[:len(s)-len(article)])
			art := strings.TrimPrefix(article, ", ")
			return art + " " + base
		}
	}
	return s
}

func isMetadataWord(s string) bool {
	metadataWords := []string{
		"proper", "repack", "extended", "unrated", "remastered",
		"x264", "x265", "hevc", "avc", "aac", "ac3", "dts",
		"720p", "1080p", "2160p", "480p", "4k", "uhd",
		"webdl", "webrip", "bluray", "hdtv", "dvdrip",
		"gb", "mb", "kb", "esub", "sub", "subs",
	}
	s = strings.ToLower(s)
	for _, w := range metadataWords {
		if s == w {
			return true
		}
	}
	return false
}

func isAllNumbers(s string) bool {
	for _, r := range s {
		if !unicode.IsDigit(r) && !unicode.IsSpace(r) {
			return false
		}
	}
	return true
}

func isAllUppercase(s string) bool {
	hasLetter := false
	for _, r := range s {
		if unicode.IsLetter(r) {
			hasLetter = true
			if unicode.IsLower(r) {
				return false
			}
		}
	}
	return hasLetter
}

var parserLanguageFlags = map[string]string{
	"ta": "Tamil 🇮🇳",
	"te": "Telugu 🇮🇳",
	"ml": "Malayalam 🇮🇳",
	"hi": "Hindi 🇮🇳",
	"kn": "Kannada 🇮🇳",
	"en": "English 🇬🇧",
	"fr": "French 🇫🇷",
	"es": "Spanish 🇪🇸",
	"de": "German 🇩🇪",
	"it": "Italian 🇮🇹",
	"ja": "Japanese 🇯🇵",
	"ko": "Korean 🇰🇷",
	"zh": "Chinese 🇨🇳",
}

func formatLanguage(lang string) string {
	lang = strings.ToLower(strings.TrimSpace(lang))
	if lang == "" {
		return "Unknown 🌐"
	}
	if val, ok := parserLanguageFlags[lang]; ok {
		return val
	}
	return strings.ToUpper(lang) + " 🌐"
}

func init() {
	_ = url.QueryEscape
}
