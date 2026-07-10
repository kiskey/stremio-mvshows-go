// Version: 2.1.5
// Change log: Removed url.Parse in P2P magnet processing inside streamHandler to prevent unescaped characters from stripping titles and falling back to Season Pack.

package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/kiskey/stremio-mvshows-go/internal/config"
	"github.com/kiskey/stremio-mvshows-go/internal/database"
	"github.com/kiskey/stremio-mvshows-go/internal/services/debrid"
	"github.com/kiskey/stremio-mvshows-go/internal/services/parser"
	"github.com/kiskey/stremio-mvshows-go/internal/services/tracker"
	"github.com/kiskey/stremio-mvshows-go/internal/utils"
	bolt "go.etcd.io/bbolt"
)

// Global concurrency semaphore: restricts background debrid pollers to maximum 3 concurrent goroutines.
// This completely protects the server IP from ever hammering or throttling the debrid API providers.
var backgroundPollSemaphore = make(chan struct{}, 3)

// Hot Path regex pre-compiled to prevent CPU and heap allocations during file selections
var epRegex = regexp.MustCompile(`[Ss]\d{1,2}\s*[Ee]\s*(\d{1,3})`)

// writeJSON pre-marshals objects to []byte to guarantee Content-Length is always known before writing.
// This eliminates the non-deterministic fallback to Transfer-Encoding: chunked on payloads >= 4 KB.
func writeJSON(c *gin.Context, code int, obj interface{}) {
	data, err := json.Marshal(obj)
	if err != nil {
		utils.Logger.Error().Err(err).Msg("Failed to pre-marshal Stremio JSON payload")
		c.AbortWithStatus(http.StatusInternalServerError)
		return
	}
	c.Writer.Header().Set("Content-Length", strconv.Itoa(len(data)))
	c.Data(code, "application/json; charset=utf-8", data)
}

// ── Stremio Protocol Statically-Typed Struct Responses ──

type StremioCatalogResponse struct {
	Metas []StremioMetaEntry `json:"metas"`
}

type StremioMetaEntry struct {
	ID          string   `json:"id"`
	Type        string   `json:"type"`
	Name        string   `json:"name"`
	ReleaseInfo string   `json:"releaseInfo"`
	Poster      string   `json:"poster,omitempty"`
	Description string   `json:"description,omitempty"`
	ImdbRating  string   `json:"imdbRating,omitempty"`
	Genres      []string `json:"genres,omitempty"`
}

type StremioMetaResponse struct {
	Meta StremioMetaDetail `json:"meta"`
}

type StremioMetaDetail struct {
	ID               string               `json:"id"`
	Type             string               `json:"type"`
	Name             string               `json:"name"`
	ReleaseInfo      string               `json:"releaseInfo"`
	Poster           string               `json:"poster,omitempty"`
	Description      string               `json:"description,omitempty"`
	ImdbRating       string               `json:"imdbRating,omitempty"`
	Genres           []string             `json:"genres,omitempty"`
	Videos           []StremioVideoDetail `json:"videos,omitempty"`
}

type StremioVideoDetail struct {
	ID      string `json:"id"`
	Season  int    `json:"season"`
	Episode int    `json:"episode"`
	Title   string `json:"title"`
}

type StremioStreamResponse struct {
	Streams []StremioStreamDetail `json:"streams"`
}

type StremioStreamDetail struct {
	Name     string   `json:"name"`
	Title    string   `json:"title"`
	URL      string   `json:"url,omitempty"`
	InfoHash string   `json:"infoHash,omitempty"`
	Sources  []string `json:"sources,omitempty"`
	// Internal tracking fields (excluded from JSON serialization)
	Quality  string   `json:"-"`
	Language string   `json:"-"`
}

// ── TMDB Data Structures ──

type tmdbLightData struct {
	Title        string  `json:"title"`
	Name         string  `json:"name"`
	Overview     string  `json:"overview"`
	ReleaseDate  string  `json:"release_date"`
	FirstAirDate string  `json:"first_air_date"`
	VoteAverage  float64 `json:"vote_average"`
	Genres       []genre `json:"genres"`
}

type genre struct {
	Name string `json:"name"`
}

// ── Stream Deduplication Key ──

type streamDupKey struct {
	IsRD     string
	Quality  string
	Language string
	Infohash string
}

// ── Quality Sorting ──

var qualityOrder = map[string]int{
	"4K":    1,
	"2160p": 1,
	"1080p": 2,
	"720p":  3,
	"480p":  4,
	"SD":    5,
}

type StreamSlice []StremioStreamDetail

func (s StreamSlice) Len() int      { return len(s) }
func (s StreamSlice) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s StreamSlice) Less(i, j int) bool {
	a, b := s[i], s[j]

	aIsP2P := a.InfoHash != ""
	bIsP2P := b.InfoHash != ""

	if !aIsP2P && bIsP2P {
		return true
	}
	if aIsP2P && !bIsP2P {
		return false
	}

	aQuality := a.Quality
	bQuality := b.Quality
	if aQuality == "" {
		aQuality = "SD"
	}
	if bQuality == "" {
		bQuality = "SD"
	}
	aQRank := qualityOrder[aQuality]
	bQRank := qualityOrder[bQuality]
	if aQRank == 0 {
		aQRank = 99
	}
	if bQRank == 0 {
		bQRank = 99
	}
	if aQRank != bQRank {
		return aQRank < bQRank
	}

	aLang := a.Language
	bLang := b.Language
	if aLang == "" {
		aLang = "zz"
	}
	if bLang == "" {
		bLang = "zz"
	}
	return strings.ToLower(aLang) < strings.ToLower(bLang)
}

func sortStreams(streams []StremioStreamDetail) {
	sort.Stable(StreamSlice(streams))
}

func dedupeStreams(streams []StremioStreamDetail) []StremioStreamDetail {
	seen := make(map[string]bool)
	out := make([]StremioStreamDetail, 0, len(streams))
	for _, s := range streams {
		streamType := "p2p"
		if s.URL != "" {
			streamType = "rd"
		}

		quality := s.Quality
		if quality == "" {
			quality = "SD"
		}
		lang := s.Language
		if lang == "" {
			lang = "NA"
		}

		hashOrURL := s.InfoHash
		if s.URL != "" {
			hashOrURL = s.URL
		}

		key := fmt.Sprintf("%s|%s|%s|%s", streamType, quality, strings.ToLower(lang), hashOrURL)
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, s)
	}
	return out
}

// ── Route Registration ──

func RegisterStremioRoutes(r *gin.RouterGroup) {
	r.GET("/manifest.json", manifestHandler)
	r.GET("/catalog/:type/:id/:extra", catalogHandler)
	r.GET("/catalog/:type/:id", catalogHandler)
	r.GET("/meta/:type/:id", metaHandler)
	r.GET("/stream/:type/:id", streamHandler)
	r.GET("/rd-add/:infohash/:episode", rdAddHandler)
}

// ── Manifest ──

func manifestHandler(c *gin.Context) {
	cfg := config.Load()
	writeJSON(c, http.StatusOK, gin.H{
		"id":          cfg.AddonID,
		"version":     cfg.AddonVersion,
		"name":        cfg.AddonName,
		"description": cfg.AddonDescription,
		"resources": []interface{}{
			"catalog",
			"stream",
		},
		"types": []string{"series", "movie"},
		"catalogs": []gin.H{
			{
				"type": "series",
				"id":   "tamilmv_series",
				"name": "Tamil WebSeries",
				"extra": []gin.H{
					{"name": "skip", "isRequired": false},
				},
			},
			{
				"type": "movie",
				"id":   "tamilmv_hd_movies",
				"name": "Tamil HD Movies",
				"extra": []gin.H{
					{"name": "skip", "isRequired": false},
				},
			},
			{
				"type": "movie",
				"id":   "tamilmv_dubbed_movies",
				"name": "Tamil HD Dubbed Movies",
				"extra": []gin.H{
					{"name": "skip", "isRequired": false},
				},
			},
		},
		"idPrefixes": []string{"tt"},
	})
}

// ── Catalog ──

func catalogHandler(c *gin.Context) {
	mediaType := c.Param("type")
	catalogID := c.Param("id")
	extra := c.Param("extra")

	catalogID = strings.TrimSuffix(catalogID, ".json")
	extra = strings.TrimSuffix(extra, ".json")

	skip := 0
	if qSkip := c.Query("skip"); qSkip != "" {
		if val, err := strconv.Atoi(qSkip); err == nil {
			skip = val
		}
	} else if extra != "" {
		if vals, err := url.ParseQuery(extra); err == nil {
			if parsedSkip := vals.Get("skip"); parsedSkip != "" {
				if val, err := strconv.Atoi(parsedSkip); err == nil {
					skip = val
				}
			}
		} else if strings.Contains(extra, "skip=") {
			parts := strings.Split(extra, "skip=")
			if len(parts) > 1 {
				numStr := parts[1]
				if idx := strings.IndexAny(numStr, "&?"); idx != -1 {
					numStr = numStr[:idx]
				}
				if val, err := strconv.Atoi(numStr); err == nil {
					skip = val
				}
			}
		}
	}

	catalogFilter := "top-series-from-forum"
	if catalogID == "tamilmv_hd_movies" {
		catalogFilter = "tamil-hd-movies"
	} else if catalogID == "tamilmv_dubbed_movies" {
		catalogFilter = "tamil-dubbed-movies"
	}

	var threads []database.Thread

	_ = database.DB.View(func(tx *bolt.Tx) error {
		idxB := tx.Bucket([]byte("catalog_index"))
		thrB := tx.Bucket([]byte("threads"))
		metaB := tx.Bucket([]byte("tmdb_metadata"))

		prefix := []byte("cat:" + catalogFilter + ":" + mediaType + ":")
		cursor := idxB.Cursor()

		skipped := 0
		collected := 0

		for k, v := cursor.Seek(prefix); k != nil && strings.HasPrefix(string(k), string(prefix)); k, v = cursor.Next() {
			if skipped < skip {
				skipped++
				continue
			}

			threadHash := string(v)
			
			tBytes := thrB.Get([]byte(threadHash))
			if tBytes == nil {
				continue
			}
			var t database.Thread
			if err := database.DecodeGob(tBytes, &t); err != nil {
				continue
			}

			if t.TmdbID != nil {
				metaBytes := metaB.Get([]byte(*t.TmdbID))
				if metaBytes == nil {
					continue
				}
				var meta database.TmdbMetadata
				if err := database.DecodeGob(metaBytes, &meta); err != nil {
					continue
				}
				if meta.ImdbID == nil || *meta.ImdbID == "" {
					continue
				}
				t.TmdbMetadata = &meta
			} else {
				continue
			}

			threads = append(threads, t)
			collected++
			if collected >= 40 {
				break
			}
		}
		return nil
	})

	metas := make([]StremioMetaEntry, 0, len(threads))
	seenIDs := make(map[string]bool)

	for _, t := range threads {
		metaID := ""
		if t.TmdbMetadata != nil && t.TmdbMetadata.ImdbID != nil {
			metaID = *t.TmdbMetadata.ImdbID
		}
		if metaID == "" {
			continue
		}

		if seenIDs[metaID] {
			continue
		}
		seenIDs[metaID] = true

		title := t.CleanTitle
		if title == "" || strings.Contains(title, "[") || strings.Contains(title, "]") || strings.Contains(strings.ToLower(title), "1080p") || strings.Contains(strings.ToLower(title), "720p") || strings.Contains(strings.ToLower(title), "s0") {
			parsed := parser.ParseTitle(t.RawTitle, t.Type)
			if parsed != nil && parsed.Title != "" {
				title = parsed.Title
			} else if t.CleanTitle != "" {
				title = t.CleanTitle
			} else {
				title = t.RawTitle
			}
		}

		releaseInfo := ""
		if t.Year != nil {
			releaseInfo = strconv.Itoa(*t.Year)
		}

		poster := "https://images.metahub.space/poster/medium/" + metaID + "/img"

		if t.CustomPoster != nil && *t.CustomPoster != "" {
			poster = *t.CustomPoster
		}

		desc := ""
		if t.CustomDescription != nil && *t.CustomDescription != "" {
			desc = *t.CustomDescription
		}

		metaEntry := StremioMetaEntry{
			ID:          metaID,
			Type:        t.Type,
			Name:        title,
			ReleaseInfo: releaseInfo,
			Poster:      poster,
		}

		if desc != "" {
			metaEntry.Description = desc
		}

		metas = append(metas, metaEntry)
	}

	writeJSON(c, http.StatusOK, StremioCatalogResponse{Metas: metas})
}

// ── Meta ──

func metaHandler(c *gin.Context) {
	id := c.Param("id")
	cfg := config.Load()

	if decoded, err := url.QueryUnescape(id); err == nil {
		id = decoded
	}

	id = strings.TrimSpace(strings.TrimSuffix(id, ".json"))

	cleanID := id
	if idx := strings.Index(id, ":pending:"); idx != -1 {
		cleanID = id[idx+len(":pending:"):]
	}

	var meta database.TmdbMetadata
	var foundMeta bool
	var mappedThread database.Thread
	var hasMappedThread bool

	_ = database.DB.View(func(tx *bolt.Tx) error {
		metaB := tx.Bucket([]byte("tmdb_metadata"))
		thrB := tx.Bucket([]byte("threads"))
		threadIdxB := tx.Bucket([]byte("tmdb_thread_index"))

		data := metaB.Get([]byte(cleanID))
		if data != nil {
			if errDec := database.DecodeGob(data, &meta); errDec == nil {
				foundMeta = true
			}
		}

		// Decouple thread point-lookup dynamically from TmdbMetadata, eliminating cyclic meta.Threads [report.md]
		if foundMeta {
			tHash := threadIdxB.Get([]byte(meta.TmdbID))
			if tHash != nil {
				tBytes := thrB.Get(tHash)
				if tBytes != nil {
					if errDec := database.DecodeGob(tBytes, &mappedThread); errDec == nil {
						hasMappedThread = true
					}
				}
			}
		}
		return nil
	})

	if foundMeta {
		displayName := "Unknown"
		if hasMappedThread {
			displayName = mappedThread.CleanTitle
			if displayName == "" || strings.Contains(displayName, "[") || strings.Contains(displayName, "]") || strings.Contains(strings.ToLower(displayName), "1080p") || strings.Contains(strings.ToLower(displayName), "720p") || strings.Contains(strings.ToLower(displayName), "s0") {
				parsed := parser.ParseTitle(mappedThread.RawTitle, mappedThread.Type)
				if parsed != nil && parsed.Title != "" {
					displayName = parsed.Title
				}
			}
		}

		mediaType := "movie"
		if hasMappedThread {
			mediaType = mappedThread.Type
		}

		poster := "https://images.metahub.space/poster/medium/" + cleanID + "/img"

		overview := "Up-to-date metadata resolving on Cinemeta."
		if hasMappedThread {
			t := mappedThread
			if t.CustomPoster != nil && *t.CustomPoster != "" {
				poster = *t.CustomPoster
			}
			if t.CustomDescription != nil && *t.CustomDescription != "" {
				overview = *t.CustomDescription
			}
		}

		releaseInfo := ""
		if meta.Year != nil {
			releaseInfo = strconv.Itoa(*meta.Year)
		}

		metaObj := StremioMetaDetail{
			ID:          id,
			Type:        mediaType,
			Name:        displayName,
			ReleaseInfo: releaseInfo,
			Poster:      poster,
			Description: overview,
		}

		if mediaType == "series" {
			streams, _ := database.FindMovieStreams(nil, meta.TmdbID)

			videos := make([]StremioVideoDetail, 0)
			seen := make(map[string]bool)

			for _, s := range streams {
				if s.Season != nil && s.Episode != nil {
					sVal := *s.Season
					eVal := *s.Episode
					endVal := eVal
					if s.EpisodeEnd != nil {
						endVal = *s.EpisodeEnd
					}

					for ep := eVal; ep <= endVal; ep++ {
						vKey := fmt.Sprintf("%d:%d", sVal, ep)
						if seen[vKey] {
							continue
						}
						seen[vKey] = true

						videos = append(videos, StremioVideoDetail{
							ID:      fmt.Sprintf("%s:%d:%d", id, sVal, ep),
							Season:  sVal,
							Episode: ep,
							Title:   fmt.Sprintf("Season %d - Episode %d", sVal, ep),
						})
					}
				}
			}
			metaObj.Videos = videos
		}

		writeJSON(c, http.StatusOK, StremioMetaResponse{Meta: metaObj})
		return
	}

	var foundThread *database.Thread
	_ = database.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("threads"))
		data := b.Get([]byte(cleanID))
		if data != nil {
			var t database.Thread
			if errDec := database.DecodeGob(data, &t); errDec == nil {
				foundThread = &t
			}
		}
		return nil
	})

	if foundThread != nil {
		t := *foundThread
		metaObj := StremioMetaDetail{
			ID:          id,
			Type:        t.Type,
			Name:        t.RawTitle,
			Poster:      cfg.PlaceholderPoster,
			Description: "Pending metadata match. You can link this manually in the administration rescue panel.",
		}
		if t.Year != nil {
			metaObj.ReleaseInfo = strconv.Itoa(*t.Year)
		}
		writeJSON(c, http.StatusOK, StremioMetaResponse{Meta: metaObj})
		return
	}

	writeJSON(c, http.StatusNotFound, gin.H{"error": "Metadata not found"})
}

// ── Stream ──

func streamHandler(c *gin.Context) {
	id := c.Param("id")
	cfg := config.Load()

	if decoded, err := url.QueryUnescape(id); err == nil {
		id = decoded
	}

	id = strings.TrimSpace(strings.TrimSuffix(id, ".json"))

	cleanID := id
	if idx := strings.Index(id, ":pending:"); idx != -1 {
		cleanID = id[idx+len(":pending:"):]
	}

	var baseID string
	season := -1
	episode := -1

	lastColon := strings.LastIndex(cleanID, ":")
	if lastColon != -1 {
		secondLastColon := strings.LastIndex(cleanID[:lastColon], ":")
		if secondLastColon != -1 {
			sVal, errS := strconv.Atoi(cleanID[secondLastColon+1 : lastColon])
			eVal, errE := strconv.Atoi(cleanID[lastColon+1:])
			if errS == nil && errE == nil {
				season = sVal
				episode = eVal
				baseID = cleanID[:secondLastColon]
			} else {
				baseID = cleanID
			}
		} else {
			baseID = cleanID
		}
	} else {
		baseID = cleanID
	}

	var meta database.TmdbMetadata
	var foundMeta bool
	var mappedThread database.Thread
	var hasMappedThread bool

	_ = database.DB.View(func(tx *bolt.Tx) error {
		metaB := tx.Bucket([]byte("tmdb_metadata"))
		thrB := tx.Bucket([]byte("threads"))
		threadIdxB := tx.Bucket([]byte("tmdb_thread_index"))

		data := metaB.Get([]byte(baseID))
		if data != nil {
			if errDec := database.DecodeGob(data, &meta); errDec == nil {
				foundMeta = true
			}
		}

		if foundMeta {
			tHash := threadIdxB.Get([]byte(meta.TmdbID))
			if tHash != nil {
				tBytes := thrB.Get(tHash)
				if tBytes != nil {
					if errDec := database.DecodeGob(tBytes, &mappedThread); errDec == nil {
						hasMappedThread = true
					}
				}
			}
		}
		return nil
	})

	if !foundMeta {
		var threads []database.Thread
		errThread := database.DB.View(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte("threads"))
			data := b.Get([]byte(baseID))
			if data != nil {
				var t database.Thread
				if errDec := database.DecodeGob(data, &t); errDec == nil {
					threads = append(threads, t)
				}
			}
			return nil
		})

		if errThread == nil && len(threads) > 0 {
			t := threads[0]
			streamList := make([]StremioStreamDetail, 0)
			seenP2P := make(map[string]bool)

			for _, magnet := range t.MagnetURIs {
				parsedMagnet := parser.ParseMagnet(magnet, t.Type)
				if parsedMagnet == nil {
					continue
				}

				if seenP2P[parsedMagnet.Infohash] {
					continue
				}
				seenP2P[parsedMagnet.Infohash] = true

				var p2pBadgeLine string
				var p2pResTag string
				
				dnQuery := parser.ExtractMagnetDisplayName(magnet)
				if dnQuery != "" {
					p2pBadgeLine = parser.FormatBadges(dnQuery)
					if p2pSize := parser.ExtractFileSize(dnQuery); p2pSize != "" {
						p2pBadgeLine += "  |  💾 " + p2pSize
					}

					parser.CompileFilters()
					for _, f := range parser.CompiledFilters {
						if f.GroupID == "gr" && f.Positive.MatchString(dnQuery) {
							p2pResTag = " [" + f.Name + "]"
							break
						}
					}
				}

				if p2pBadgeLine == "" {
					p2pBadgeLine = formatQualityBadge(parsedMagnet.Quality)
				}
				if p2pResTag == "" && parsedMagnet.Quality != "" && strings.ToLower(parsedMagnet.Quality) != "sd" {
					p2pResTag = " [" + strings.ToUpper(parsedMagnet.Quality) + "]"
				}

				trackerSources := buildTrackerSources()
				sources := withDhtSource(trackerSources, parsedMagnet.Infohash)

				streamList = append(streamList, StremioStreamDetail{
					Name:     "🔌 P2P" + p2pResTag,
					Title:    fmt.Sprintf("🎬 %s\n✨ %s   |   🔊 Peer-to-Peer Stream", t.RawTitle, p2pBadgeLine),
					InfoHash: parsedMagnet.Infohash,
					Sources:  sources,
				})
			}
			writeJSON(c, http.StatusOK, StremioStreamResponse{Streams: streamList})
			return
		}

		writeJSON(c, http.StatusOK, StremioStreamResponse{Streams: []StremioStreamDetail{}})
		return
	}

	var streams []database.Stream
	var errStreams error
	if season != -1 && episode != -1 {
		streams, errStreams = database.FindSeriesStreams(nil, meta.TmdbID, season, episode)
	} else {
		streams, errStreams = database.FindMovieStreams(nil, meta.TmdbID)
	}

	if errStreams != nil {
		writeJSON(c, http.StatusInternalServerError, gin.H{"error": "Streams lookup failed"})
		return
	}

	p := debrid.GetProvider(cfg)

	var allHashes []string
	for _, s := range streams {
		allHashes = append(allHashes, s.Infohash)
	}
	cacheMap := debrid.CheckCached(allHashes, database.DB)

	if len(allHashes) == 0 {
		writeJSON(c, http.StatusOK, StremioStreamResponse{Streams: []StremioStreamDetail{}})
		return
	}

	magnetMap := make(map[string]string)
	_ = database.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("magnet_cache"))
		for _, h := range allHashes {
			data := b.Get([]byte(h))
			if data != nil {
				var mc database.MagnetCache
				if errDec := database.DecodeGob(data, &mc); errDec == nil {
					if dn := parser.ExtractMagnetDisplayName(mc.Magnet); dn != "" {
						magnetMap[h] = dn
					}
				}
			}
		}
		return nil
	})

	var cachedStreams []StremioStreamDetail
	var uncachedStreams []StremioStreamDetail

	seenStreams := make(map[streamDupKey]bool)

	mediaType := "movie"
	if hasMappedThread {
		mediaType = mappedThread.Type
	}

	tmdbTitle := ""
	if hasMappedThread {
		tmdbTitle = mappedThread.CleanTitle
		if tmdbTitle == "" || strings.Contains(tmdbTitle, "[") || strings.Contains(tmdbTitle, "]") || strings.Contains(strings.ToLower(tmdbTitle), "1080p") || strings.Contains(strings.ToLower(tmdbTitle), "720p") || strings.Contains(strings.ToLower(tmdbTitle), "s0") {
			parsed := parser.ParseTitle(mappedThread.RawTitle, mappedThread.Type)
			if parsed != nil && parsed.Title != "" {
				tmdbTitle = parsed.Title
			}
		}
	}
	if tmdbTitle == "" {
		var tmdbData tmdbLightData
		dec := json.NewDecoder(strings.NewReader(meta.Data))
		if dec.Decode(&tmdbData) == nil {
			tmdbTitle = tmdbData.Title
			if tmdbTitle == "" {
				tmdbTitle = tmdbData.Name
			}
		}
	}
	if tmdbTitle == "" {
		tmdbTitle = "Unknown Release"
	}

	for _, s := range streams {
		isCached := cacheMap[s.Infohash]

		var resTag string
		var dn string
		if val, ok := magnetMap[s.Infohash]; ok {
			dn = val
		}

		pr := parser.ParseRelease(dn, mediaType)

		if pr.ReleaseGroup != "" {
			resTag = " [" + pr.ReleaseGroup + "]"
		} else if s.Quality != "" && strings.ToLower(s.Quality) != "sd" {
			resTag = " [" + strings.ToUpper(s.Quality) + "]"
		}

		formattedTitle := buildStreamTitle(pr, tmdbTitle, mediaType, dn)

		dupKey := streamDupKey{
			IsRD:     "",
			Quality:  s.Quality,
			Language: s.Language,
			Infohash: s.Infohash,
		}
		if seenStreams[dupKey] {
			continue
		}
		seenStreams[dupKey] = true

		if p.IsEnabled() {
			targetEpStr := "movie"
			if season != -1 && episode != -1 {
				targetEpStr = fmt.Sprintf("%d-%d", season, episode)
			}
			rdUrl := fmt.Sprintf("%s/rd-add/%s/%s", cfg.AppHost, s.Infohash, targetEpStr)

			badgeName := "⏳ RD" + resTag
			if isCached {
				badgeName = "⚡ RD+" + resTag
			}
			if cfg.DebridService == "torbox" {
				badgeName = "⏳ TB" + resTag
				if isCached {
					badgeName = "⚡ TB+" + resTag
				}
			}

			item := StremioStreamDetail{
				Name:     badgeName,
				Title:    formattedTitle,
				URL:      rdUrl,
				Quality:  s.Quality,
				Language: s.Language,
			}
			if isCached {
				cachedStreams = append(cachedStreams, item)
			} else {
				uncachedStreams = append(uncachedStreams, item)
			}
		} else {
			trackerSources := buildTrackerSources()
			sources := withDhtSource(trackerSources, s.Infohash)
			badgeName := "🔌 P2P" + resTag

			item := StremioStreamDetail{
				Name:     badgeName,
				Title:    formattedTitle,
				InfoHash: s.Infohash,
				Sources:  sources,
				Quality:  s.Quality,
				Language: s.Language,
			}
			uncachedStreams = append(uncachedStreams, item)
		}
	}

	streamList := append(cachedStreams, uncachedStreams...)
	streamList = dedupeStreams(streamList)
	sortStreams(streamList)

	writeJSON(c, http.StatusOK, StremioStreamResponse{Streams: streamList})
}

func rdAddHandler(c *gin.Context) {
	infohash := strings.ToLower(c.Param("infohash"))
	episodeParam := c.Param("episode")

	season := -1
	episode := -1
	isMovie := true

	if episodeParam != "movie" {
		parts := strings.Split(episodeParam, "-")
		if len(parts) == 2 {
			season, _ = strconv.Atoi(parts[0])
			episode, _ = strconv.Atoi(parts[1])
			isMovie = false
		}
	}

	var cache database.MagnetCache
	errCache := database.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("magnet_cache"))
		data := b.Get([]byte(infohash))
		if data == nil {
			return bolt.ErrBucketNotFound
		}
		return database.DecodeGob(data, &cache)
	})
	if errCache != nil {
		writeJSON(c, http.StatusNotFound, gin.H{"error": "Magnet not found in local cache"})
		return
	}

	cfg := config.Load()
	p := debrid.GetProvider(cfg)
	if !p.IsEnabled() {
		writeJSON(c, http.StatusBadRequest, gin.H{"error": "No active debrid service configured"})
		return
	}

	reqCtx := c.Request.Context()

	var torrentRecord database.DebridTorrent
	var foundRecord bool
	_ = database.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("debrid_torrents"))
		data := b.Get([]byte(infohash))
		if data != nil {
			if errDec := database.DecodeGob(data, &torrentRecord); errDec == nil {
				if torrentRecord.Status == "downloaded" {
					foundRecord = true
				}
			}
		}
		return nil
	})

	if foundRecord {
		torrentRecord.LastChecked = time.Now()
		_ = database.DB.Update(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte("debrid_torrents"))
			bytesData, _ := database.EncodeGob(torrentRecord)
			return b.Put([]byte(infohash), bytesData)
		})

		dlLink, errCachedLink := getDebridCachedLink(reqCtx, &torrentRecord, season, episode, isMovie)
		if errCachedLink == nil && dlLink != "" {
			c.Redirect(http.StatusFound, dlLink)
			return
		}
	}

	info, errAdd := p.AddAndSelect(reqCtx, cache.Magnet)
	if errAdd != nil {
		utils.Logger.Error().Err(errAdd).Str("infohash", infohash).Msg("Debrid AddAndSelect failed.")
		writeJSON(c, http.StatusInternalServerError, gin.H{"error": "Failed to add magnet to debrid provider: " + errAdd.Error()})
		return
	}

	maxPolls := 60
	pollInterval := 3 * time.Second
	torrentID := info.ID 

	for i := 0; i < maxPolls; i++ {
		select {
		case <-reqCtx.Done():
			utils.Logger.Info().
				Str("infohash", infohash).
				Str("id", torrentID).
				Msg("Client disconnected during active stream loading. Detaching and delegating debrid caching to background.")
			
			go func(tID string, infohash string) {
				defer func() {
					if r := recover(); r != nil {
						utils.Logger.Error().Interface("panic", r).Msg("Recovered from background caching poll panic.")
					}
				}()

				select {
				case backgroundPollSemaphore <- struct{}{}:
					defer func() { <-backgroundPollSemaphore }()
				default:
					utils.Logger.Warn().Str("infohash", infohash).Msg("Background caching queue full. Skipping background polling to prevent API throttling.")
					return
				}

				bgCtx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
				defer cancel()

				bgProvider := debrid.GetProvider(config.Load())
				bgDownloaded := false
				var bgInfo *debrid.TorrentInfo
				var bgErr error

				bgPollInterval := 15 * time.Second

				for j := 0; j < (180 / 15); j++ {
					select {
					case <-bgCtx.Done():
						return
					default:
					}

					bgInfo, bgErr = bgProvider.GetTorrentInfo(bgCtx, tID)
					if bgErr == nil && bgInfo.Status == "downloaded" {
						bgDownloaded = true
						break
					}
					time.Sleep(bgPollInterval)
				}

				if bgDownloaded && bgInfo != nil {
					var record database.DebridTorrent
					record.Infohash = infohash
					record.TorrentID = tID
					record.Provider = cfg.DebridService
					record.Status = "downloaded"
					record.Files = make([]database.TorrentFile, len(bgInfo.Files))
					for idx, f := range bgInfo.Files {
						record.Files[idx] = database.TorrentFile{
							ID:       f.ID,
							Path:     f.Path,
							Bytes:    f.Bytes,
							Selected: f.Selected,
						}
					}
					record.Links = bgInfo.Links
					record.LastChecked = time.Now()
					record.CreatedAt = time.Now()
					record.UpdatedAt = time.Now()

					_ = database.DB.Update(func(tx *bolt.Tx) error {
						b := tx.Bucket([]byte("debrid_torrents"))
						bytesData, _ := database.EncodeGob(record)
						return b.Put([]byte(infohash), bytesData)
					})
					utils.Logger.Info().Str("infohash", infohash).Msg("Debrid torrent cached successfully in background.")
				}
			}(torrentID, infohash)

			writeJSON(c, 499, gin.H{"error": "Request cancelled by client. Cache polling detached to background."})
			return
		default:
		}

		var errPoll error
		info, errPoll = p.GetTorrentInfo(reqCtx, torrentID)
		if errPoll != nil {
			utils.Logger.Warn().Err(errPoll).Str("id", torrentID).Msg("Error polling debrid torrent status. Retrying.")
		} else if info != nil && info.Status == "downloaded" {
			break
		}
		time.Sleep(pollInterval)
	}

	if info == nil || info.Status != "downloaded" {
		writeJSON(c, http.StatusRequestTimeout, gin.H{"error": "Debrid download timed out. Please try streaming this item again shortly."})
		return
	}

	finalLink := ""
	if isMovie {
		var selectedFiles []debrid.FileInfo
		for _, f := range info.Files {
			if f.Selected == 1 {
				selectedFiles = append(selectedFiles, f)
			}
		}
		if len(selectedFiles) > 0 {
			var fileToPlay *debrid.FileInfo
			var videoFiles []debrid.FileInfo
			for _, f := range selectedFiles {
				if strings.HasSuffix(strings.ToLower(f.Path), ".mkv") ||
					strings.HasSuffix(strings.ToLower(f.Path), ".mp4") ||
					strings.HasSuffix(strings.ToLower(f.Path), ".avi") ||
					strings.HasSuffix(strings.ToLower(f.Path), ".mov") {
					videoFiles = append(videoFiles, f)
				}
			}
			if len(videoFiles) > 0 {
				largest := &videoFiles[0]
				for i := 1; i < len(videoFiles); i++ {
					if videoFiles[i].Bytes > largest.Bytes {
						largest = &videoFiles[i]
					}
				}
				fileToPlay = largest
			} else {
				largest := &selectedFiles[0]
				for i := 1; i < len(selectedFiles); i++ {
					if selectedFiles[i].Bytes > largest.Bytes {
						largest = &selectedFiles[i]
					}
				}
				fileToPlay = largest
			}

			dl, errDl := getDownloadLinkForFile(reqCtx, p, info, fileToPlay.ID)
			if errDl == nil {
				finalLink = dl
			}
		}
	} else {
		candidates := make([]parser.CandidateFile, len(info.Files))
		for idx, f := range info.Files {
			candidates[idx] = parser.CandidateFile{
				ID:   f.ID,
				Path: f.Path,
				Size: f.Bytes,
			}
		}

		best, found := parser.FindBestSeriesFile(candidates, season, episode, season)
		if found {
			dl, errDl := getDownloadLinkForFile(reqCtx, p, info, best.ID)
			if errDl == nil {
				finalLink = dl
			}
		}
	}

	if finalLink == "" {
		writeJSON(c, http.StatusNotFound, gin.H{"error": "Failed to locate target video file inside debrid payload."})
		return
	}

	torrentRecord.Infohash = infohash
	torrentRecord.TorrentID = info.ID
	torrentRecord.Provider = cfg.DebridService
	torrentRecord.Status = "downloaded"
	torrentRecord.Files = make([]database.TorrentFile, len(info.Files))
	for idx, f := range info.Files {
		torrentRecord.Files[idx] = database.TorrentFile{
			ID:       f.ID,
			Path:     f.Path,
			Bytes:    f.Bytes,
			Selected: f.Selected,
		}
	}
	torrentRecord.Links = info.Links
	torrentRecord.LastChecked = time.Now()
	torrentRecord.CreatedAt = time.Now()
	torrentRecord.UpdatedAt = time.Now()

	_ = database.DB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("debrid_torrents"))
		bytesData, _ := database.EncodeGob(torrentRecord)
		return b.Put([]byte(infohash), bytesData)
	})

	c.Redirect(http.StatusFound, finalLink)
}

func getDebridCachedLink(ctx context.Context, dt *database.DebridTorrent, season, episode int, isMovie bool) (string, error) {
	cfg := config.Load()
	p := debrid.GetProvider(cfg)
	if !p.IsEnabled() {
		return "", fmt.Errorf("debrid not enabled")
	}

	if isMovie {
		var selectedFiles []database.TorrentFile
		for _, f := range dt.Files {
			if f.Selected == 1 {
				selectedFiles = append(selectedFiles, f)
			}
		}
		if len(selectedFiles) > 0 {
			var fileToPlay *database.TorrentFile
			var videoFiles []database.TorrentFile
			for _, f := range selectedFiles {
				pathLower := strings.ToLower(f.Path)
				if strings.HasSuffix(pathLower, ".mkv") ||
					strings.HasSuffix(pathLower, ".mp4") ||
					strings.HasSuffix(pathLower, ".avi") ||
					strings.HasSuffix(pathLower, ".mov") {
					videoFiles = append(videoFiles, f)
				}
			}
			if len(videoFiles) > 0 {
				largest := &videoFiles[0]
				for i := 1; i < len(videoFiles); i++ {
					if videoFiles[i].Bytes > largest.Bytes {
						largest = &videoFiles[i]
					}
				}
				fileToPlay = largest
			} else {
				largest := &selectedFiles[0]
				for i := 1; i < len(selectedFiles); i++ {
					if selectedFiles[i].Bytes > largest.Bytes {
						largest = &selectedFiles[i]
					}
				}
				fileToPlay = largest
			}

			linkIdx := -1
			for idx, f := range dt.Files {
				if f.ID == fileToPlay.ID {
					linkIdx = idx
					break
				}
			}
			if linkIdx >= 0 && linkIdx < len(dt.Links) {
				unres, err := p.UnrestrictLink(ctx, dt.Links[linkIdx])
				if err == nil {
					return unres.Download, nil
				}
			}
		}
	} else {
		candidates := make([]parser.CandidateFile, len(dt.Files))
		for idx, f := range dt.Files {
			candidates[idx] = parser.CandidateFile{
				ID:   f.ID,
				Path: f.Path,
				Size: f.Bytes,
			}
		}

		best, found := parser.FindBestSeriesFile(candidates, season, episode, season)
		if found {
			linkIdx := -1
			for idx, f := range dt.Files {
				if f.ID == best.ID {
					linkIdx = idx
					break
				}
			}
			if linkIdx >= 0 && linkIdx < len(dt.Links) {
				unres, err := p.UnrestrictLink(ctx, dt.Links[linkIdx])
				if err == nil {
					return unres.Download, nil
				}
			}
		}
	}
	return "", fmt.Errorf("cached link not found or unable to unrestrict")
}

func getDownloadLinkForFile(ctx context.Context, p debrid.Provider, info *debrid.TorrentInfo, fileID int) (string, error) {
	cfg := config.Load()
	if cfg.DebridService == "torbox" {
		link := fmt.Sprintf("tb:%s:%d", info.ID, fileID)
		unres, err := p.UnrestrictLink(ctx, link)
		if err == nil {
			return unres.Download, nil
		}
		return "", err
	}

	linkIdx := -1
	for idx, f := range info.Files {
		if f.ID == fileID {
			linkIdx = idx
			break
		}
	}
	if linkIdx >= 0 && linkIdx < len(info.Links) {
		unres, err := p.UnrestrictLink(ctx, info.Links[linkIdx])
		if err == nil {
			return unres.Download, nil
		}
		return "", err
	}

	return "", fmt.Errorf("link index not found or unrestricting failed")
}

func buildStreamTitle(pr *parser.ParsedRelease, tmdbTitle string, mediaType string, fallbackDn string) string {
	var b strings.Builder

	if mediaType == "series" {
		seasonStr := fmt.Sprintf("S%02d", pr.SeasonNumber)
		var epPart string
		if pr.IsSeasonPack {
			epPart = "Season Pack"
		} else if len(pr.EpisodeNumbers) == 1 {
			epPart = fmt.Sprintf("E%02d", pr.EpisodeNumbers[0])
		} else if len(pr.EpisodeNumbers) > 1 {
			epPart = fmt.Sprintf("E%02d-E%02d", pr.EpisodeNumbers[0], pr.EpisodeNumbers[len(pr.EpisodeNumbers)-1])
		} else {
			epPart = "Pack"
		}
		b.WriteString(fmt.Sprintf("🎬 %s (%s | %s)", tmdbTitle, seasonStr, epPart))
	} else {
		b.WriteString(fmt.Sprintf("🎬 %s", tmdbTitle))
	}

	if pr.Edition.EditionString != "" {
		b.WriteString(fmt.Sprintf(" [%s]", pr.Edition.EditionString))
	}

	b.WriteString("\n✨ ")
	if pr.Quality.FullString != "Unknown" {
		b.WriteString(pr.Quality.FullString)
	} else if pr.Resolution != "" {
		b.WriteString(pr.Resolution)
	} else {
		b.WriteString("SD")
	}

	if pr.Source != "" {
		b.WriteString(" | " + pr.Source)
	}

	if pr.VideoCodec != "" {
		b.WriteString(" | " + pr.VideoCodec)
	}
	if pr.AudioCodec != "" {
		b.WriteString(" | " + pr.AudioCodec)
	}
	if pr.AudioChannels != "" {
		b.WriteString(" " + pr.AudioChannels)
	}

	b.WriteString("\n🔊 ")
	if len(pr.Languages) > 0 {
		b.WriteString(formatLanguage(pr.Languages[0]))
		if len(pr.Languages) > 1 {
			b.WriteString(fmt.Sprintf(" +%d", len(pr.Languages)-1))
		}
	} else {
		b.WriteString("Tamil 🇮🇳")
	}

	if pr.ReleaseGroup != "" {
		b.WriteString(fmt.Sprintf("\n🏷️ %s", pr.ReleaseGroup))
	}

	if len(pr.SpecialTags) > 0 {
		b.WriteString(fmt.Sprintf("\n⚡ %s", strings.Join(pr.SpecialTags, ", ")))
	}

	b.WriteString("\n📥 Click to Stream")
	return b.String()
}

var languageFlags = map[string]string{
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
	if val, ok := languageFlags[lang]; ok {
		return val
	}
	return strings.ToUpper(lang) + " 🌐"
}

func formatQualityBadge(q string) string {
	q = strings.ToUpper(strings.TrimSpace(q))
	switch q {
	case "4K", "2160P":
		return "🚀 4K UHD"
	case "1080P":
		return "🔥 1080p HD"
	case "720P":
		return "⚡ 720p HD"
	case "480P", "SD", "360P":
		return "📼 SD"
	default:
		return "🎥 " + q
	}
}

func buildTrackerSources() []string {
	trackers := tracker.GetTrackers()
	allowed := make([]string, 0, len(trackers))
	for _, t := range trackers {
		if strings.HasPrefix(t, "udp://") || strings.HasPrefix(t, "http://") || strings.HasPrefix(t, "https://") {
			proto := "http"
			rest := t
			if strings.HasPrefix(t, "udp://") {
				proto = "udp"
				rest = strings.TrimPrefix(t, "udp://")
			} else if strings.HasPrefix(t, "http://") {
				rest = strings.TrimPrefix(t, "http://")
			} else if strings.HasPrefix(t, "https://") {
				rest = strings.TrimPrefix(rest, "https://")
			}
			allowed = append(allowed, "tracker:"+proto+"://"+rest)
		}
	}
	return allowed
}

func withDhtSource(sources []string, infohash string) []string {
	if infohash == "" {
		return sources
	}
	list := make([]string, len(sources))
	copy(list, sources)
	list = append(list, "dht:"+infohash)
	return list
}
