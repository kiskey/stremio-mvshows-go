# -------- Build Stage --------
FROM golang:1.22-alpine AS builder

# Install build tools required for compiling CGO-based SQLite driver
RUN apk add --no-cache gcc musl-dev

WORKDIR /app

# Copy module manifests first to optimize layer caching
COPY go.mod go.sum ./
RUN go mod download

# Copy the remaining project sources
COPY . .

# Compile static binary with optimizations and CGO enabled
RUN CGO_ENABLED=1 GOOS=linux go build -ldflags="-s -w" -installsuffix cgo -o server ./cmd/server

# -------- Runtime Stage --------
FROM alpine:latest

# Ensure secure HTTPS endpoints are reachable by fetching TLS root certificates and config timezone databases
RUN apk --no-cache add ca-certificates tzdata

WORKDIR /root/

# Copy the compiled executable from the build container
COPY --from=builder /app/server .

# Copy static frontend assets served by the admin router
COPY public/ ./public/

# Default exposed port
EXPOSE 3000

# ── Cache Expiry Environment Defaults ──
ENV CACHE_EXPIRY_ENABLED=true
ENV CACHE_EXPIRY_DAYS=5

# Run server
CMD ["./server"]
