
// Version: 2.0.4
// Change log: Removed the downloaded state tracker variable to resolve unused variable compilation errors. Polling success is evaluated directly against the resulting debrid torrent info structures.

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
	// CRITICAL FIX: Set Content-Length explicitly BEFORE any write call.
	// net/http's automatic Content-Length detection fails when
	// headers + body together exceed the 4096-byte bufio.Writer buffer,
	// causing Transfer-Encoding: chunked to be selected instead.
	// Explicit pre-setting bypasses this threshold entirely.
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
	ID          string               `json:"id"`
	Type        string               `json:"type"`
	Name        string               `json:"name"`
	ReleaseInfo string               `json:"releaseInfo"`
	Poster      string               `json:"poster,omitempty"`
	Description string               `json:"description,omitempty"`
	ImdbRating  string               `json:"imdbRating,omitempty"`
	Genres      []string             `json:"genres,omitempty"`
	Videos      []StremioVideoDetail `json:"videos,omitempty"`
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

// qualityOrder defines stream quality ranking — lower number = higher priority
var qualityOrder = map[string]int{
	"4K":    1,
	"2160p": 1,
	"1080p": 2,
	"720p":  3,
	"480p":  4,
	"SD":    5,
}

// StreamSlice implements standard sort.Interface to achieve reflection-free sorting
type StreamSlice []StremioStreamDetail

func (s StreamSlice) Len() int      { return len(s) }
func (s StreamSlice) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s StreamSlice) Less(i, j int) bool {
	a, b := s[i], s[j]

	// RD streams (have non-empty URL) come before P2P (have non-empty InfoHash)
	aIsP2P := a.InfoHash != ""
	bIsP2P := b.InfoHash != ""

	if !aIsP2P && bIsP2P {
		return true // a is RD, b is P2P
	}
	if aIsP2P && !bIsP2P {
		return false // a is P2P, b is RD
	}

	// Compare quality rank
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

	// Compare language alphabetically
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

// sortStreams stable-sorts stream array cleanly without reflective performance penalties.
func sortStreams(streams []StremioStreamDetail) {
	sort.Stable(StreamSlice(streams))
}

// dedupeStreams removes duplicate entries keyed by (isRD | quality | language | infohash).
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
	r.GET("/meta/:type/:id", metaHandler)   // Bypasses Gin suffix wildcard match limitation
	r.GET("/stream/:type/:id", streamHandler) // Bypasses Gin suffix wildcard match limitation
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
			"stream", // Removed "meta" to let Cinemeta completely render descriptions and episode selectors
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
		"idPrefixes": []string{"tt"}, // Restored "tt" as the only idPrefix, completely matching the Node.js standard
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

	// Highly optimized lexicographical descending index scan using pre-sorted B+ tree pages
	_ = database.DB.View(func(tx *bolt.Tx) error {
		idxB := tx.Bucket([]byte("catalog_index"))
		thrB := tx.Bucket([]byte("threads"))
		metaB := tx.Bucket([]byte("tmdb_metadata"))

		prefix := []byte("cat:" + catalogFilter + ":")
		cursor := idxB.Cursor()

		skipped := 0
		collected := 0

		for k, v := cursor.Seek(prefix); k != nil && strings.HasPrefix(string(k), string(prefix)); k, v = cursor.Next() {
			threadHash := string(v)
			
			tBytes := thrB.Get([]byte(threadHash))
			if tBytes == nil {
				continue
			}
			var t database.Thread
			if err := database.DecodeGob(tBytes, &t); err != nil {
				continue
			}

			// Validate type bounds
			if t.Type != mediaType || t.Status != "linked" {
				continue
			}

			// Preload TMDB relationship metadata locally
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
					continue // Discard records with missing IMDb pointers
				}
				t.TmdbMetadata = &meta
			} else {
				continue
			}

			// Offset limits
			if skipped < skip {
				skipped++
				continue
			}

			threads = append(threads, t)
			collected++
			if collected >= 40 { // Limit window matching Stremio standard
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
		if title == "" {
			parsed := parser.ParseTitle(t.RawTitle, t.Type)
			if parsed != nil && parsed.Title != "" {
				title = parsed.Title
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

	_ = database.DB.View(func(tx *bolt.Tx) error {
		metaB := tx.Bucket([]byte("tmdb_metadata"))
		thrB := tx.Bucket([]byte("threads"))

		// Read by keys
		data := metaB.Get([]byte(cleanID))
		if data == nil {
			// Check by IMDb ID scan
			c := metaB.Cursor()
			for k, v := c.First(); k != nil; k, v = c.Next() {
				var temp database.TmdbMetadata
				if errDec := database.DecodeGob(v, &temp); errDec == nil {
					if temp.ImdbID != nil && *temp.ImdbID == cleanID {
						meta = temp
						foundMeta = true
						break
					}
				}
			}
		} else {
			if errDec := database.DecodeGob(data, &meta); errDec == nil {
				foundMeta = true
			}
		}

		if foundMeta {
			// Populate related threads
			c := thrB.Cursor()
			for k, v := c.First(); k != nil; k, v = c.Next() {
				var t database.Thread
				if errDec := database.DecodeGob(v, &t); errDec == nil {
					if t.TmdbID != nil && *t.TmdbID == meta.TmdbID {
						meta.Threads = append(meta.Threads, t)
					}
				}
			}
		}
		return nil
	})

	if foundMeta {
		displayName := "Unknown"
		if len(meta.Threads) > 0 {
			displayName = meta.Threads[0].CleanTitle
		}

		mediaType := "movie"
		if len(meta.Threads) > 0 {
			mediaType = meta.Threads[0].Type
		}

		poster := "https://images.metahub.space/poster/medium/" + cleanID + "/img"

		overview := "Up-to-date metadata resolving on Cinemeta."
		if len(meta.Threads) > 0 {
			t := meta.Threads[0]
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

	// URL-decode the ID parameter to handle percent-encoded colons (%3A) safely
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

	_ = database.DB.View(func(tx *bolt.Tx) error {
		metaB := tx.Bucket([]byte("tmdb_metadata"))
		thrB := tx.Bucket([]byte("threads"))

		data := metaB.Get([]byte(baseID))
		if data == nil {
			c := metaB.Cursor()
			for k, v := c.First(); k != nil; k, v = c.Next() {
				var temp database.TmdbMetadata
				if errDec := database.DecodeGob(v, &temp); errDec == nil {
					if temp.ImdbID != nil && *temp.ImdbID == baseID {
						meta = temp
						foundMeta = true
						break
					}
				}
			}
		} else {
			if errDec := database.DecodeGob(data, &meta); errDec == nil {
				foundMeta = true
			}
		}

		if foundMeta {
			c := thrB.Cursor()
			for k, v := c.First(); k != nil; k, v = c.Next() {
				var t database.Thread
				if errDec := database.DecodeGob(v, &t); errDec == nil {
					if t.TmdbID != nil && *t.TmdbID == meta.TmdbID {
						meta.Threads = append(meta.Threads, t)
					}
				}
			}
		}
		return nil
	})

	if !foundMeta {
		var foundThread *database.Thread
		_ = database.DB.View(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte("threads"))
			data := b.Get([]byte(baseID))
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
				if u, err := url.Parse(magnet); err == nil {
					if dnQuery := u.Query().Get("dn"); dnQuery != "" {
						if decoded, err := url.QueryUnescape(dnQuery); err == nil {
							p2pBadgeLine = parser.FormatBadges(decoded)
							if p2pSize := parser.ExtractFileSize(decoded); p2pSize != "" {
								p2pBadgeLine += "  |  💾 " + p2pSize
							}

							parser.CompileFilters()
							for _, f := range parser.CompiledFilters {
								if f.GroupID == "gr" && f.Positive.MatchString(decoded) {
									p2pResTag = " [" + f.Name + "]"
									break
								}
							}
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
	if len(meta.Threads) > 0 {
		mediaType = meta.Threads[0].Type
	}

	tmdbTitle := ""
	if len(meta.Threads) > 0 {
		tmdbTitle = meta.Threads[0].CleanTitle
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

		if dn != "" {
			parser.CompileFilters()
			for _, f := range parser.CompiledFilters {
				if f.GroupID == "gr" && f.Positive.MatchString(dn) {
					resTag = " [" + f.Name + "]"
					break
				}
			}
		}
		if resTag == "" && s.Quality != "" && strings.ToLower(s.Quality) != "sd" {
			resTag = " [" + strings.ToUpper(s.Quality) + "]"
		}

		var parsedBadges string
		if dn != "" {
			parsedBadges = parser.FormatBadges(dn)
		}

		badgeLine := ""
		if parsedBadges != "" {
			badgeLine = parsedBadges
		} else if s.Quality != "" {
			badgeLine = "[" + strings.ToUpper(s.Quality) + "]"
		}

		if dn != "" {
			if parsedSize := parser.ExtractFileSize(dn); parsedSize != "" {
				badgeLine += "  |  💾 " + parsedSize
			}
		}

		var detailsHeader string
		if mediaType == "series" {
			seasonVal := 0
			if s.Season != nil {
				seasonVal = *s.Season
			}
			epVal := 0
			if s.Episode != nil {
				epVal = *s.Episode
			}
			epEndVal := epVal
			if s.EpisodeEnd != nil {
				epEndVal = *s.EpisodeEnd
			}

			seasonStr := fmt.Sprintf("S%02d", seasonVal)
			var epPart string
			if epEndVal == 0 || epEndVal == epVal {
				epPart = fmt.Sprintf("Episode %02d", epVal)
			} else if epVal == 1 && epEndVal == 999 {
				epPart = "Season Pack"
			} else {
				epPart = fmt.Sprintf("Episodes %02d-%02d", epVal, epEndVal)
			}
			detailsHeader = fmt.Sprintf("🎬 %s (%s | %s)", tmdbTitle, seasonStr, epPart)
		} else {
			detailsHeader = fmt.Sprintf("🎬 %s", tmdbTitle)
		}

		langBadge := formatLanguage(s.Language)

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

			formattedTitle := fmt.Sprintf("%s\n✨ %s\n🔊 %s\n📥 Click to Stream", 
				detailsHeader, 
				badgeLine, 
				langBadge,
			)

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

			formattedTitle := fmt.Sprintf("%s\n✨ %s\n🔊 %s\n🔗 Peer-to-Peer Stream", 
				detailsHeader, 
				badgeLine, 
				langBadge,
			)

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

// ── Stream Title Builders ──

func buildSeriesTitle(tmdbTitle string, season, episode, episodeEnd int, quality, language string) string {
	seasonStr := fmt.Sprintf("S%02d", season)

	var epPart string
	if episodeEnd == 0 || episodeEnd == episode {
		epPart = fmt.Sprintf("Episode %02d", episode)
	} else if episode == 1 && episodeEnd == 999 {
		epPart = "Season Pack"
	} else {
		epPart = fmt.Sprintf("Episodes %02d-%02d", episode, episodeEnd)
	}

	qBadge := formatQualityBadge(quality)
	langBadge := formatLanguage(language)

	return fmt.Sprintf("🎬 %s (%s | %s)\n✨ %s   |   🔊 %s", tmdbTitle, seasonStr, epPart, qBadge, langBadge)
}

func buildMovieTitle(tmdbTitle, quality, language string) string {
	qBadge := formatQualityBadge(quality)
	langBadge := formatLanguage(language)

	return fmt.Sprintf("🎬 %s\n✨ %s   |   🔊 %s", tmdbTitle, qBadge, langBadge)
}

// ── Presentation Formatters ──

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

// ── Tracker Sources ──

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

// ── rdAddHandler (On-Demand Player Activation) ──

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

	// Resolve original magnet from cache database
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

	// Check if already completely downloaded and saved locally in DebridTorrent table
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
		// Update LastChecked timestamp to extend cache TTL
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

	// Add and Select magnet on Debrid Provider
	info, errAdd := p.AddAndSelect(reqCtx, cache.Magnet)
	if errAdd != nil {
		utils.Logger.Error().Err(errAdd).Str("infohash", infohash).Msg("Debrid AddAndSelect failed.")
		writeJSON(c, http.StatusInternalServerError, gin.H{"error": "Failed to add magnet to debrid provider: " + errAdd.Error()})
		return
	}

	// Poll status (max 60 iterations * 3s = 3 minutes) until "downloaded"
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
					// Successfully finished downloading in the background. Write to local Bolt cache
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

	// Find the matching video link
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
		// For series, map files to CandidateFile structures and run FindBestSeriesFile selection
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

	// Cache the success inside Bbolt database
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

// ── Cached Debrid Link Resolver ──

func getDebridCachedLink(ctx context.Context, r *database.DebridTorrent, season, episode int, isMovie bool) (string, error) {
	cfg := config.Load()
	p := debrid.GetProvider(cfg)

	// Map DB record back to TorrentInfo shape to utilize our universal link resolver
	info := &debrid.TorrentInfo{
		ID:    r.TorrentID,
		Links: r.Links,
	}
	info.Files = make([]debrid.FileInfo, len(r.Files))
	for i, f := range r.Files {
		info.Files[i] = debrid.FileInfo{
			ID:       f.ID,
			Path:     f.Path,
			Bytes:    f.Bytes,
			Selected: f.Selected,
		}
	}

	if isMovie {
		var selectedFiles []debrid.FileInfo
		for _, f := range info.Files {
			if f.Selected == 1 {
				selectedFiles = append(selectedFiles, f)
			}
		}
		if len(selectedFiles) == 0 {
			return "", fmt.Errorf("no selected files")
		}
		var fileToPlay *debrid.FileInfo
		var videoFiles []debrid.FileInfo
		for _, f := range selectedFiles {
			if strings.HasSuffix(strings.ToLower(f.Path), ".mkv") ||
				strings.HasSuffix(strings.ToLower(f.Path), ".mp4") ||
				strings.HasSuffix(strings.ToLower(f.Path), ".avi") {
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

		return getDownloadLinkForFile(ctx, p, info, fileToPlay.ID)
	}

	// Build Series Candidates list
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
		return getDownloadLinkForFile(ctx, p, info, best.ID)
	}

	return "", fmt.Errorf("best file match not found")
}

// getDownloadLinkForFile dynamically asserts if the provider implements direct link resolution (TorBox) or index-based unrestrict (Real-Debrid)
func getDownloadLinkForFile(ctx context.Context, p debrid.Provider, info *debrid.TorrentInfo, fileID int) (string, error) {
	if dlProvider, ok := p.(interface {
		GetDownloadLinkForFile(context.Context, string, string) (string, error)
	}); ok {
		return dlProvider.GetDownloadLinkForFile(ctx, info.ID, strconv.Itoa(fileID))
	}

	// Real-Debrid / Fallback index-based link unrestriction
	linkIndex := -1
	selectedCount := 0
	for _, f := range info.Files {
		if f.Selected == 1 {
			if f.ID == fileID {
				linkIndex = selectedCount
				break
			}
			selectedCount++
		}
	}

	if linkIndex < 0 || linkIndex >= len(info.Links) {
		return "", fmt.Errorf("file ID %d not selected or links index out of range", fileID)
	}

	unrestricted, err := p.UnrestrictLink(ctx, info.Links[linkIndex])
	if err != nil {
		return "", err
	}
	return unrestricted.Download, nil
}
