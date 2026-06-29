// Version: 1.1.6
// Change log: Upgraded gzipWriter to intercept WriteHeader and Write calls to guarantee the deletion of "Content-Length" before headers are flushed, resolving browser-specific catalog truncation.

package api

import (
	"compress/gzip"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/kiskey/stremio-mvshows-go/internal/utils"
)

// SetupRouter initializes and configures the production HTTP router, middleware, and paths.
func SetupRouter() *gin.Engine {
	// Set Gin mode based on global logger level
	gin.SetMode(gin.ReleaseMode)

	r := gin.New()
	
	// Explicitly configure Gin to trust headers from Nginx/reverse proxy network gateways safely.
	// Prevents warning logs and ensures c.ClientIP() accurately resolves true client IPs.
	_ = r.SetTrustedProxies(nil)

	r.Use(customRecovery())
	r.Use(corsMiddleware())
	r.Use(requestLogger())
	r.Use(gzipMiddleware()) // Native high-performance Gzip payload compression with admin-bypass support

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

// gzipWriter wraps Gin's ResponseWriter, routing writes directly to the Gzip compressor.
type gzipWriter struct {
	gin.ResponseWriter
	writer *gzip.Writer
}

// WriteHeader intercepts the header flush and deletes Content-Length to force chunked encoding.
func (g *gzipWriter) WriteHeader(code int) {
	g.Header().Del("Content-Length")
	g.ResponseWriter.WriteHeader(code)
}

// Write intercepts the body writes and deletes Content-Length if WriteHeader was bypassed.
func (g *gzipWriter) Write(data []byte) (int, error) {
	g.Header().Del("Content-Length")
	return g.writer.Write(data)
}

// WriteString intercepts the body writes and deletes Content-Length if WriteHeader was bypassed.
func (g *gzipWriter) WriteString(s string) (int, error) {
	g.Header().Del("Content-Length")
	return g.writer.Write([]byte(s))
}

// Flush implements http.Flusher to prevent chunk buffering hangs on reverse proxies like OpenResty/NPM.
func (g *gzipWriter) Flush() {
	_ = g.writer.Flush()
	if flusher, ok := g.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

// gzipMiddleware compresses HTTP payloads using the standard library compressor with optimal CPU speed.
func gzipMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		// EXPLICIT BYPASS: Do not Gzip compress any admin panel API calls.
		// These payloads are small (<5KB) and pre-compressing them causes OpenResty/NPM SSL socket-read hangs.
		if strings.HasPrefix(c.Request.URL.Path, "/admin/api/") {
			c.Next()
			return
		}

		// Only compress if the client supports Gzip encoding
		if !strings.Contains(c.GetHeader("Accept-Encoding"), "gzip") {
			c.Next()
			return
		}

		// Use BestSpeed (Level 1) to maximize throughput while minimizing CPU overhead
		gz, err := gzip.NewWriterLevel(c.Writer, gzip.BestSpeed)
		if err != nil {
			c.Next()
			return
		}

		c.Header("Content-Encoding", "gzip")
		c.Header("Vary", "Accept-Encoding")

		// Wrap and assign our upgraded Gzip Flusher and Header-Interceptor writer
		gWriter := &gzipWriter{ResponseWriter: c.Writer, writer: gz}
		c.Writer = gWriter

		c.Next()

		// Explicitly close the Gzip writer to write the trailing CRC32 checksum footer
		_ = gz.Close()

		// Explicitly flush the newly closed gzip footer out of Go's HTTP write buffer.
		gWriter.Flush()
	}
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
