package api

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/kiskey/stremio-mvshows-go/internal/utils"
)

// SetupRouter initializes and configures the production HTTP router, middleware, and paths.
func SetupRouter() *gin.Engine {
	// Set Gin mode based on global logger level
	gin.SetMode(gin.ReleaseMode)

	r := gin.New()
	r.Use(customRecovery())
	r.Use(corsMiddleware())
	r.Use(requestLogger())

	// Serve the admin panel static page at root and explicitly at /admin
	r.StaticFile("/", "./public/admin.html")
	r.StaticFile("/admin", "./public/admin.html")

	// Register Stremio Addon manifest, catalog, metadata, and stream routes
	stremioGroup := r.Group("/")
	RegisterStremioRoutes(stremioGroup)

	// Register admin rescue panel operations
	adminGroup := r.Group("/admin/api")
	RegisterAdminRoutes(adminGroup)

	return r
}

// corsMiddleware implements a lightweight, high-performance native CORS handler.
// This completely removes dependency on "github.com/rs/cors" to reduce binary size (space complexity) and allocations.
func corsMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Writer.Header().Set("Access-Control-Allow-Origin", "*")
		c.Writer.Header().Set("Access-Control-Allow-Credentials", "true")
		c.Writer.Header().Set("Access-Control-Allow-Headers", "Origin, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization, Accept, X-Requested-With")
		c.Writer.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}
		c.Next()
	}
}

// customRecovery replaces default gin.Recovery with highly optimized, zerolog-integrated panic catching.
func customRecovery() gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if err := recover(); err != nil {
				utils.Logger.Error().
					Interface("panic", err).
					Str("method", c.Request.Method).
					Str("path", c.Request.URL.Path).
					Msg("Unhandled panic rescued inside HTTP Router request chain.")
				c.AbortWithStatus(http.StatusInternalServerError)
			}
		}()
		c.Next()
	}
}

func requestLogger() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		path := c.Request.URL.Path
		query := c.Request.URL.RawQuery

		c.Next()

		latency := time.Since(start)
		status := c.Writer.Status()

		if status >= 400 {
			utils.Logger.Warn().
				Int("status", status).
				Str("method", c.Request.Method).
				Str("path", path).
				Str("query", query).
				Str("ip", c.ClientIP()).
				Dur("latency", latency).
				Msg("HTTP Request completed with warning/error")
		} else {
			utils.Logger.Info().
				Int("status", status).
				Str("method", c.Request.Method).
				Str("path", path).
				Str("query", query).
				Str("ip", c.ClientIP()).
				Dur("latency", latency).
				Msg("HTTP Request processed successfully")
		}
	}
}
