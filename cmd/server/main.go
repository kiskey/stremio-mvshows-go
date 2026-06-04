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
	gormlogger "gorm.io/gorm/logger"
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

	// 3. Map global log levels to GORM SQLite log levels
	gormLevel := gormlogger.Error
	if cfg.LogLevel == "debug" {
		gormLevel = gormlogger.Info
	}

	// 4. Initialize SQLite GORM Database
	dbPath := "/data/stremio_addon.db"
	_, err := database.Init(dbPath, gormLevel)
	if err != nil {
		utils.Logger.Fatal().Err(err).Str("path", dbPath).Msg("Critical database initialization failure.")
	}
	utils.Logger.Info().Str("path", dbPath).Msg("SQLite database connection verified and schemas synchronized.")

	// 5. Build initial trackers list cache
	tracker.FetchAndCacheTrackers(cfg)

	// 6. Generate starting dashboard state cache
	orchestrator.UpdateDashboardCache()

	// 7. Configure background cron scheduler using UTC timezone and panic recovery chain
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

	// 8. Fire initial background crawl workflow on cold start to immediately sync fresh releases
	go func() {
		// Small delay to let the HTTP server launch first
		time.Sleep(3 * time.Second)
		orchestrator.RunFullWorkflow(cfg)
	}()

	// 9. Bootstrap and launch primary Gin HTTP Server blocking on the main thread
	router := api.SetupRouter()
	portStr := ":" + strconv.Itoa(cfg.Port)
	utils.Logger.Info().Str("port", portStr).Msg("HTTP Server starting...")

	if errRun := router.Run(portStr); errRun != nil {
		utils.Logger.Fatal().Err(errRun).Msg("Critical server execution crash.")
	}
}
