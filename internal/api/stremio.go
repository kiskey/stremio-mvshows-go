package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url" // Critical fix: added net/url to resolve compile error in url.QueryEscape
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
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// tmdbLightData is a highly optimized, allocation-free struct replacing map[string]interface{} unmarshaling
type tmdbLightData struct {
	Title      string `json:"title"`
	Name       string `json:"name"`
	PosterPath string `json:"poster_path"`
	Overview   string `json:"overview"`
}

func RegisterStremioRoutes(r *gin.RouterGroup) {
	r.GET("/manifest.json", manifestHandler)
	r.GET("/catalog/:type/:id/:extra", catalogHandler)
	r.GET("/catalog/:type/:id", catalogHandler)
	r.GET("/meta/:type/:id.json", metaHandler)
	r.GET("/stream/:type/:id.json", streamHandler)
	r.GET("/rd-add/:infohash/:episode", rdAddHandler)
}

func manifestHandler(c *gin.Context) {
	cfg := config.Load()
	c.JSON(http.StatusOK, gin.H{
		"id":          cfg.AddonID,
		"version":     cfg.AddonVersion,
		"name":        cfg.AddonName,
		"description": cfg.AddonDescription,
		"resources":   []string{"catalog", "meta", "stream"},
		"types":       []string{"series", "movie"},
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
		"idPrefixes": []string{"tt", "tv", "movie"},
	})
}

func catalogHandler(c *gin.Context) {
	mediaType := c.Param("type")
	catalogID := c.Param("id")
	extra := c.Param("extra")

	skip := 0
	// Parse skip parameter from query or path suffix
	if qSkip := c.Query("skip"); qSkip != "" {
		if val, err := strconv.Atoi(qSkip); err == nil {
			skip = val
		}
	} else if strings.Contains(extra, "skip=") {
		parts := strings.Split(extra, "skip=")
		if len(parts) > 1 {
			if val, err := strconv.Atoi(parts[1]); err == nil {
				skip = val
			}
		}
	}

	var threads []database.Thread
	query := database.DB.Where("status = ? AND type = ?", "linked", mediaType)

	// Filter by specific catalogs matching manifest IDs
	if catalogID == "tamilmv_hd_movies" {
		query = query.Where("catalog = ?", "tamil-hd-movies")
	} else if catalogID == "tamilmv_dubbed_movies" {
		query = query.Where("catalog = ?", "tamil-dubbed-movies")
	} else {
		query = query.Where("catalog = ?", "top-series-from-forum")
	}

	err := query.
		Order("posted_at DESC").
		Offset(skip).
		Limit(100).
		Preload("TmdbMetadata").
		Find(&threads).Error

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database lookup failed"})
		return
	}

	cfg := config.Load()
	metas := make([]gin.H, 0, len(threads))

	for _, t := range threads {
		metaID := t.ThreadHash
		poster := cfg.PlaceholderPoster
		desc := ""
		title := t.CleanTitle
		if title == "" {
			title = t.RawTitle
		}

		if t.TmdbMetadata != nil {
			if t.TmdbMetadata.ImdbID != "" {
				metaID = t.TmdbMetadata.ImdbID
			} else {
				metaID = t.TmdbMetadata.TmdbID
			}

			// Optimized unmarshaling to prevent thousands of reflection heap allocations per page
			var tmdbData tmdbLightData
			if json.Unmarshal([]byte(t.TmdbMetadata.Data), &tmdbData) == nil {
				if tmdbData.PosterPath != "" {
					poster = "https://image.tmdb.org/t/p/w500" + tmdbData.PosterPath
				}
				if tmdbData.Overview != "" {
					desc = tmdbData.Overview
				}
			}
		}

		if t.CustomPoster != nil && *t.CustomPoster != "" {
			poster = *t.CustomPoster
		}
		if t.CustomDescription != nil && *t.CustomDescription != "" {
			desc = *t.CustomDescription
		}

		yearStr := ""
		if t.Year != nil {
			yearStr = strconv.Itoa(*t.Year)
		}

		metas = append(metas, gin.H{
			"id":          metaID,
			"type":        t.Type,
			"name":        title,
			"poster":      poster,
			"description": desc,
			"releaseInfo": yearStr,
		})
	}

	c.JSON(http.StatusOK, gin.H{"metas": metas})
}

func metaHandler(c *gin.Context) {
	id := strings.TrimSuffix(c.Param("id"), ".json")

	var meta database.TmdbMetadata
	err := database.DB.Where("imdb_id = ? OR tmdb_id = ?", id, id).Preload("Threads").First(&meta).Error
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Metadata not found"})
		return
	}

	// Optimized unmarshaling replacing map[string]interface{}
	var details tmdbLightData
	if errJson := json.Unmarshal([]byte(meta.Data), &details); errJson != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to parse TMDB data payload"})
		return
	}

	cfg := config.Load()
	poster := cfg.PlaceholderPoster
	if details.PosterPath != "" {
		poster = "https://image.tmdb.org/t/p/w500" + details.PosterPath
	}

	overview := details.Overview

	// Determine matching media type
	mediaType := "movie"
	if len(meta.Threads) > 0 {
		mediaType = meta.Threads[0].Type
	}

	displayName := details.Title
	if displayName == "" {
		displayName = details.Name
	}

	metaObj := gin.H{
		"id":          id,
		"type":        mediaType,
		"name":        displayName,
		"poster":      poster,
		"description": overview,
		"year":        meta.Year,
	}

	if mediaType == "series" {
		// Fetch linked streams to build the Stremio series videos episodic navigation
		var streams []database.Stream
		_ = database.DB.Where("tmdb_id = ?", meta.TmdbID).Order("season ASC, episode ASC").Find(&streams)

		videos := make([]gin.H, 0)
		seen := make(map[string]bool)

		for _, s := range streams {
			if s.Season != nil && s.Episode != nil {
				sVal := *s.Season
				eVal := *s.Episode
				endVal := eVal
				if s.EpisodeEnd != nil {
					endVal = *s.EpisodeEnd
				}

				// Generate chronological sequence mapping for episode ranges (packs)
				for ep := eVal; ep <= endVal; ep++ {
					vKey := fmt.Sprintf("%d:%d", sVal, ep)
					if seen[vKey] {
						continue
					}
					seen[vKey] = true

					videos = append(videos, gin.H{
						"id":      fmt.Sprintf("%s:%d:%d", id, sVal, ep),
						"season":  sVal,
						"episode": ep,
						"title":   fmt.Sprintf("Season %d - Episode %d", sVal, ep),
					})
				}
			}
		}
		metaObj["videos"] = videos
	}

	c.JSON(http.StatusOK, gin.H{"meta": metaObj})
}

func streamHandler(c *gin.Context) {
	id := strings.TrimSuffix(c.Param("id"), ".json")

	var imdbID string
	season := -1
	episode := -1

	parts := strings.Split(id, ":")
	imdbID = parts[0]
	if len(parts) > 2 {
		season, _ = strconv.Atoi(parts[1])
		episode, _ = strconv.Atoi(parts[2])
	}

	var meta database.TmdbMetadata
	err := database.DB.Where("imdb_id = ? OR tmdb_id = ?", imdbID, imdbID).First(&meta).Error
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Metadata mapping not found"})
		return
	}

	var streams []database.Stream
	if season != -1 && episode != -1 {
		// Series path: search direct episode match OR full season packs where episode is NULL
		err = database.DB.Where("tmdb_id = ? AND ((season = ? AND episode <= ? AND episode_end >= ?) OR (season = ? AND episode IS NULL))",
			meta.TmdbID, season, episode, episode, season).
			Order("quality DESC").
			Find(&streams).Error
	} else {
		// Movie path
		err = database.DB.Where("tmdb_id = ?", meta.TmdbID).Order("quality DESC").Find(&streams).Error
	}

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Streams lookup failed"})
		return
	}

	cfg := config.Load()
	p := debrid.GetProvider(cfg)

	// Pre-fetch cache checks for matching infohashes to mark stream availability
	var allHashes []string
	for _, s := range streams {
		allHashes = append(allHashes, s.Infohash)
	}
	cacheMap := debrid.CheckCached(allHashes, database.DB)

	streamList := make([]gin.H, 0, len(streams))
	trackersStr := strings.Join(buildTrackerSources(), "")

	for _, s := range streams {
		isCached := cacheMap[s.Infohash]
		emoji := "⏳"
		if isCached {
			emoji = "⚡ Instant"
		}

		label := fmt.Sprintf("[%s] TamilMV\n%s / %s", emoji, s.Quality, s.Language)

		targetEpStr := "movie"
		if season != -1 && episode != -1 {
			targetEpStr = fmt.Sprintf("%d-%d", season, episode)
		}

		rdUrl := fmt.Sprintf("%s/rd-add/%s/%s", cfg.AppHost, s.Infohash, targetEpStr)

		if p.IsEnabled() {
			streamList = append(streamList, gin.H{
				"name":  "TamilMV Addon",
				"title": label,
				"url":   rdUrl,
			})
		} else {
			// P2P fallback streams directly
			streamList = append(streamList, gin.H{
				"name":     label,
				"title":    "Direct Torrent P2P Stream",
				"infoHash": s.Infohash,
				"sources":  tracker.GetTrackers(),
			})
		}
	}

	// Add trackers to redirect URLs if required
	_ = trackersStr

	c.JSON(http.StatusOK, gin.H{"streams": streamList})
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

	// Resolve the original magnet from cache to submit to debrid
	var cache database.MagnetCache
	err := database.DB.Where("infohash = ?", infohash).First(&cache).Error
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Magnet not found in local cache"})
		return
	}

	cfg := config.Load()
	p := debrid.GetProvider(cfg)
	if !p.IsEnabled() {
		c.JSON(http.StatusBadRequest, gin.H{"error": "No active debrid service configured"})
		return
	}

	// Check if already completely downloaded and saved locally in DebridTorrent table
	var torrentRecord database.DebridTorrent
	errRecord := database.DB.Where("infohash = ?", infohash).First(&torrentRecord).Error
	if errRecord == nil && torrentRecord.Status == "downloaded" {
		dlLink := getDebridCachedLink(&torrentRecord, season, episode, isMovie)
		if dlLink != "" {
			c.Redirect(http.StatusFound, dlLink)
			return
		}
	}

	// Add and Select magnet on Debrid Provider
	info, errAdd := p.AddAndSelect(cache.Magnet)
	if errAdd != nil {
		utils.Logger.Error().Err(errAdd).Str("infohash", infohash).Msg("Debrid AddAndSelect failed.")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to add magnet to debrid provider: " + errAdd.Error()})
		return
	}

	// Poll status (max 60 iterations * 3s = 3 minutes) until "downloaded"
	maxPolls := 60
	pollInterval := 3 * time.Second
	downloaded := false

	for i := 0; i < maxPolls; i++ {
		info, err = p.GetTorrentInfo(info.ID)
		if err != nil {
			utils.Logger.Warn().Err(err).Str("id", info.ID).Msg("Error polling debrid torrent status. Retrying.")
		} else if info.Status == "downloaded" {
			downloaded = true
			break
		}
		time.Sleep(pollInterval)
	}

	if !downloaded {
		c.JSON(http.StatusRequestTimeout, gin.H{"error": "Debrid download timed out. Please try streaming this item again shortly."})
		return
	}

	// Find the matching video link
	finalLink := ""
	if isMovie {
		// For movies, choose first download link or larger video link
		if len(info.Links) > 0 {
			finalLink = info.Links[0]
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
			dl, errDl := p.GetDownloadLinkForFile(info.ID, best.ID)
			if errDl == nil {
				finalLink = dl
			}
		}
	}

	if finalLink == "" {
		c.JSON(http.StatusNotFound, gin.H{"error": "Failed to locate target video file inside debrid payload."})
		return
	}

	// Cache the success inside GORM database
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

	_ = database.DB.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "infohash"}},
		UpdateAll: true,
	}).Create(&torrentRecord).Error

	c.Redirect(http.StatusFound, finalLink)
}

// ── Stremio Route Helper Routines ──

func getDebridCachedLink(r *database.DebridTorrent, season, episode int, isMovie bool) string {
	cfg := config.Load()
	p := debrid.GetProvider(cfg)

	if isMovie {
		if len(r.Links) > 0 {
			return r.Links[0]
		}
		return ""
	}

	// Build Series Candidates list
	candidates := make([]parser.CandidateFile, len(r.Files))
	for idx, f := range r.Files {
		candidates[idx] = parser.CandidateFile{
			ID:   f.ID,
			Path: f.Path,
			Size: f.Bytes,
		}
	}

	best, found := parser.FindBestSeriesFile(candidates, season, episode, season)
	if found {
		dl, err := p.GetDownloadLinkForFile(r.TorrentID, best.ID)
		if err == nil {
			return dl
		}
	}

	return ""
}

func buildTrackerSources() []string {
	trackers := tracker.GetTrackers()
	out := make([]string, 0, len(trackers))
	for _, t := range trackers {
		t = strings.TrimSpace(t)
		if t == "" {
			continue
		}
		proto := "http"
		rest := t
		if strings.HasPrefix(t, "udp://") {
			proto = "udp"
			rest = strings.TrimPrefix(t, "udp://")
		} else if strings.HasPrefix(t, "http://") {
			rest = strings.TrimPrefix(t, "http://")
		} else if strings.HasPrefix(t, "https://") {
			rest = strings.TrimPrefix(t, "https://")
		}
		out = append(out, "&tr="+url.QueryEscape(proto+"://"+rest))
	}
	return out
}
