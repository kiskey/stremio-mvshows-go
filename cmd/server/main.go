
// Version: 2.0.5
// Change log: Removed all SQLite GORM connection logging pools and initialization pathways, booting the primary process directly into the bbolt transactional B+ Tree memory-mapped index layer.

package main

import (
	"log"
	"os"
	"strconv"
	"time"

	"github.com/kiskey/stremio-mvshows-go/internal/api"
	"github.com/kiskey/stremio-mvshows-go/internal/config"
	"github.com/kiskey/stremio-mvshows-go/internal/database"
	"github.com/kiskey/stremio-mvshows-go/internal/services/maintenance"
	"github.com/kiskey/stremio-mvshows-go/internal/services/orchestrator"
	"github.com/kiskey/stremio-mvshows-go/internal/services/tracker"
	"github.com/kiskey/stremio-mvshows-go/internal/utils"
	"github.com/robfig/cron/v3"
)

func main() {
	// Defensive top-level panic recovery to protect the main server thread
	defer func() {
		if r := recover(); r != nil {
			log.Printf("CRITICAL: Main server thread crashed with panic: %v", r)
			os.Exit(1)
		}
	}()

	// 1. Load centralized configuration
	cfg := config.Load()

	// 2. Initialize zerolog structured logging
	utils.Init(cfg.LogLevel)
	utils.Logger.Info().Msg("Bootstrap sequence initiated...")

	// 3. Initialize Bbolt Memory-Mapped Key-Value Engine
	dbPath := "/data/stremio_addon.db.bolt"
	_, err := database.Init(dbPath)
	if err != nil {
		utils.Logger.Fatal().Err(err).Str("path", dbPath).Msg("Critical database initialization failure.")
	}
	utils.Logger.Info().Str("path", dbPath).Msg("Bbolt transactional key-value store initialized successfully.")

	// 4. Build initial trackers list cache
	tracker.FetchAndCacheTrackers(cfg)

	// 5. Generate starting dashboard state cache
	orchestrator.UpdateDashboardCache()

	// 6. Configure background cron scheduler using UTC timezone and panic recovery chain
	c := cron.New(
		cron.WithLocation(time.UTC),
		cron.WithChain(cron.Recover(cron.PrintfLogger(log.New(os.Stdout, "[Cron] ", log.LstdFlags)))),
	)

	// Main workflow background scraper cron task
	_, err = c.AddFunc(cfg.MainWorkflowCron, func() {
		utils.Logger.Info().Msg("Cron Triggered: Executing full scraping and parsing workflow.")
		orchestrator.RunFullWorkflow(cfg)
	})
	if err != nil {
		utils.Logger.Error().Err(err).Str("schedule", cfg.MainWorkflowCron).Msg("Failed to schedule main workflow cron task.")
	}

	// Hourly tracker list refresh task
	_, err = c.AddFunc("0 * * * *", func() {
		utils.Logger.Info().Msg("Cron Triggered: Refreshing tracking lists.")
		tracker.FetchAndCacheTrackers(cfg)
	})
	if err != nil {
		utils.Logger.Error().Err(err).Msg("Failed to schedule hourly tracker refresh task.")
	}

	// Database vacuum / WAL truncation maintenance cron task
	if cfg.DBAutoVacuumEnabled && cfg.DBAutoVacuumCron != "" {
		_, err = c.AddFunc(cfg.DBAutoVacuumCron, func() {
			utils.Logger.Info().Msg("Cron Triggered: Starting database maintenance.")
			maintenance.PerformMaintenance()
		})
		if err != nil {
			utils.Logger.Error().Err(err).Str("schedule", cfg.DBAutoVacuumCron).Msg("Failed to schedule database maintenance task.")
		}
	}

	c.Start()
	utils.Logger.Info().Msg("Background cron scheduler started successfully.")

	// 7. Fire initial background crawl workflow on cold start to immediately sync fresh releases
	go func() {
		// Small delay to let the HTTP server launch first
		time.Sleep(3 * time.Second)
		orchestrator.RunFullWorkflow(cfg)
	}()

	// 8. Bootstrap and launch primary Gin HTTP Server blocking on the main thread
	router := api.SetupRouter()
	portStr := ":" + strconv.Itoa(cfg.Port)
	utils.Logger.Info().Str("port", portStr).Msg("HTTP Server starting...")

	if errRun := router.Run(portStr); errRun != nil {
		utils.Logger.Fatal().Err(errRun).Msg("Critical server execution crash.")
	}
}
