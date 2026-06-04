# -------- Build Stage --------
FROM golang:1.22-alpine AS builder

# Install build tools required for compiling CGO-based SQLite driver
RUN apk add --no-cache gcc musl-dev

WORKDIR /app

# Copy module manifests first to optimize layer caching
COPY go.mod ./
# Generate go.sum if needed, or download immediately
RUN go mod tidy && go mod download

# Copy the remaining project sources
COPY . .

# Compile static binary with optimizations and CGO enabled
RUN CGO_ENABLED=1 GOOS=linux go build -ldflags="-s -w" -o server ./cmd/server

# -------- Runtime Stage --------
FROM alpine:latest

# Ensure secure HTTPS endpoints are reachable by fetching TLS root certificates
RUN apk --no-cache add ca-certificates tzdata

WORKDIR /root/

# Copy the compiled executable from the build container
COPY --from=builder /app/server .

# Copy static frontend assets served by the admin router
COPY public/ ./public/

# Default port exposed by the addon
EXPOSE 3000

# Run server
CMD ["./server"]
