package api

import (
	"time"

	"github.com/gin-gonic/gin"
	"github.com/rs/cors"
	"github.com/kiskey/stremio-mvshows-go/internal/utils"
)

// SetupRouter initializes and configures the production HTTP router, middleware, and paths.
func SetupRouter() *gin.Engine {
	// Set Gin mode based on global logger level
	gin.SetMode(gin.ReleaseMode)

	r := gin.New()
	r.Use(gin.Recovery())
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

func corsMiddleware() gin.HandlerFunc {
	c := cors.New(cors.Options{
		AllowedOrigins: []string{"*"},
		AllowedMethods: []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowedHeaders: []string{"Origin", "Content-Type", "Accept", "Authorization"},
	})
	return func(ginCtx *gin.Context) {
		c.HandlerFunc(ginCtx.Writer, ginCtx.Request)
		if ginCtx.Request.Method == "OPTIONS" {
			ginCtx.AbortWithStatus(200)
			return
		}
		ginCtx.Next()
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
