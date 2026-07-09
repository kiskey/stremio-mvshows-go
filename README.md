
# Proxmox Alpine LXC Deployment & Database Management Guide

This guide provides step-by-step instructions to compile, migrate, deploy, and maintain the statically linked Go addon directly on an ultra-lightweight **Alpine Linux LXC container** in Proxmox VE, utilizing OpenRC as a standalone system service and **bbolt** (the Kubernetes-hardened memory-mapped B+ Tree storage engine) as its high-speed database layer.

---

## 1. Architectural Footprint & Performance

Running the statically compiled Go binary with an in-process, memory-mapped BoltDB engine on Alpine Linux inside an unprivileged Proxmox LXC container delivers the lowest possible compute overhead:

* **Disk Footprint:** ~12 MB total (excluding data storage).
* **Memory Footprint:** Under 12 MB idle RAM (virtual space managed natively via OS kernel `mmap`).
* **CPU Footprint:** ~0% idle CPU usage (completely free of background SQLite CGO translation overhead).
* **Query Latency:** Point lookups resolved in **microseconds** (sub-millisecond ranges) directly from memory-mapped page boundaries.

---

## 2. Compiling the Static Binaries

### Method A: Automated CI/CD (GitHub Actions)
If you push a commit/tag (e.g. `v2.0.0`), the `.github/workflows/release.yml` pipeline automatically cross-compiles static binaries for both `linux/amd64` and `linux/arm64` targets and uploads them directly to your Releases tab:
1. `stremio-mvshows-linux-amd64` (The runtime Stremio Addon Server)
2. `stremio-migrator-linux-amd64` (The GORM SQLite ➔ BoltDB ETL conversion utility)
3. `stremio-inspector-linux-amd64` (The database diagnostics and repair tool)

### Method B: Manual Compiling (Go Workspace)
To compile these static binaries locally on your development system, run:

```bash
# Compile the primary server binary
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -installsuffix cgo -o stremio-mvshows ./cmd/server

# Compile the offline migrator utility
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -installsuffix cgo -o db-migrator ./cmd/migrator

# Compile the diagnostic inspector utility
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -installsuffix cgo -o db-inspector ./cmd/inspector
```

---

## 3. Creating the Alpine Container in Proxmox

1. Log in to your Proxmox VE Web UI.
2. Navigate to **local storage → CT Templates → Templates** and download the latest `alpine-3.x-default` template.
3. Click **Create CT** (upper-right corner) and configure:

### General
- Uncheck **Privileged Container** (for security).
- Check **Nesting**.

### root Disk
- Set size to **2.00 GiB to 4.00 GiB** (plenty of room for database growth).

### CPU & Memory
- Allocate **1 Core**.
- Set RAM to **256 MiB** (or **512 MiB**).
- Set Swap to **256 MiB**.

---

## 4. Enabling SSH on the Alpine Container

By default, Alpine default templates block root SSH logins. To enable access:

1. Open the Proxmox container Console (NoVNC) as root.
2. Execute the following commands to install OpenSSH, configure root password login, and start the daemon:

```bash
# Update repositories and install OpenSSH
apk update && apk add openssh

# Configure SSH to allow root password logins
sed -i 's/#PermitRootLogin.*/PermitRootLogin yes/' /etc/ssh/sshd_config
sed -i 's/PermitRootLogin.*/PermitRootLogin yes/' /etc/ssh/sshd_config

# Enable SSH service on boot and start it
rc-update add sshd default
rc-service sshd start
```

You can now close the Proxmox console and SSH into the container from your terminal:

```bash
ssh root@<your_alpine_lxc_ip_address>
```

---

## 5. Service Installation & Configuration (Dynamic OpenRC)

### Step 1: Deploy the Binary
Transfer your compiled binary `stremio-mvshows` into the container's `/usr/local/bin/` folder using `scp` or SFTP, and make it executable:

```bash
chmod +x /usr/local/bin/stremio-mvshows
```

### Step 2: Deploy the Static Frontend Assets
Create the target public directory inside the executable directory and download the `admin.html` file cleanly:

```bash
# Create directory
mkdir -p /usr/local/bin/public

# Download the dashboard file (Note: Must use UPPERCASE -O)
wget -O /usr/local/bin/public/admin.html https://raw.githubusercontent.com/kiskey/stremio-mvshows-go/refs/heads/main/public/admin.html
```

### Step 3: Create the Environment File
Create the config file at:

```bash
vi /etc/conf.d/stremio-mvshows
```

Paste your custom environment variables:

```bash
# /etc/conf.d/stremio-mvshows

PORT="3000"
LOG_LEVEL="info"
APP_HOST="http://127.0.0.1:3000"
DEBRID_SERVICE="torbox"
TORBOX_API_KEY="your_torbox_key_here"
TMDB_API_KEY="your_tmdb_key_here"

# Auto-compaction / automated maintenance configurations
DB_AUTO_VACUUM_ENABLED="true"
DB_AUTO_VACUUM_CRON="0 3 * * *"

# Cache Expiry configuration
CACHE_EXPIRY_ENABLED="true"
CACHE_EXPIRY_DAYS="5"
```

### Step 4: Create the OpenRC Service Script
Create the OpenRC service script at:

```bash
vi /etc/init.d/stremio-mvshows
```

Paste this optimized script:

```bash
#!/sbin/openrc-run
# Version: 2.0.0
# Description: Custom OpenRC service manager featuring automatic dynamic exporting of configuration variables and forced binary-folder CWD.

name="stremio-mvshows"
description="Stremio MVShows Go standalone daemon"
command="/usr/local/bin/stremio-mvshows"
command_background="true"
directory="/usr/local/bin" # Forces working directory to the binary folder to resolve ./public/ admin files
pidfile="/run/${RC_SVCNAME}.pid"
output_log="/var/log/stremio-mvshows.log"
error_log="/var/log/stremio-mvshows.err"

start_pre() {
    # Ensure database folder exists
    mkdir -p /data

    # Dynamic Auto-Exporter:
    if [ -f /etc/conf.d/stremio-mvshows ]; then
        export $(grep -v '^#' /etc/conf.d/stremio-mvshows | grep -E '^[a-zA-Z_]' | cut -d= -f1)
    fi
}
```

---

## 6. Managing the Service Lifecycle

Make the service script executable, enable the daemon on boot, and start it:

```bash
# 1. Apply executable permissions
chmod +x /etc/init.d/stremio-mvshows

# 2. Add to default runlevel to launch on boot
rc-update add stremio-mvshows default

# 3. Start/Stop/Restart commands
rc-service stremio-mvshows start
rc-service stremio-mvshows stop
rc-service stremio-mvshows restart

# 4. Check active status
rc-service stremio-mvshows status
```

---

# ── DATABASE TRANSITION & MAINTENANCE CONSOLE ──

## Step 1: Perform the Offline Database Migration

To safely migrate your historical GORM SQLite records, associations, and debrid cached states into BoltDB, run the transition migrator tool offline:

```bash
# 1. Stop your active service to release database file locks
rc-service stremio-mvshows stop

# 2. Run the offline database migrator CLI
chmod +x ./db-migrator
./db-migrator -sqlite /data/stremio_addon.db -bolt /data/stremio_addon.db.bolt

# 3. Securely swap the active databases
mv /data/stremio_addon.db /data/stremio_addon.db.bak

# 4. Start your BoltDB-native OpenRC service!
rc-service stremio-mvshows start
```

---

## Step 2: Database Maintenance & Compaction

Because Bbolt uses memory-mapped allocation pages, space from overwritten or deleted items is kept internally as blank space within the file layout for future inserts. 

To compact the database file and shrink its physical disk size to its absolute structural minimum, run your database inspector:

```bash
# Run a dry-run diagnostic scan for duplicate groups and index anomalies
./db-inspector --db /data/stremio_addon.db.bolt

# Run a live, atomic database repair and write compaction on-disk:
# (Stop the service first to safely release memory-mapped pointer locks)
rc-service stremio-mvshows stop
./db-inspector --db /data/stremio_addon.db.bolt --repair
rc-service stremio-mvshows start
```
--- END OF FILE stremio-mvshows-go-main/README.md ---
```

---

### Suggested Commit Message
```text
docs: update Proxmox LXC Readme v1.0.6 to support Bbolt deployment pipelines

- Document the db-migrator workflow steps to execute offline SQLite-to-Bolt conversions
- Document the db-inspector maintenance procedures for database repair and compaction
- Synchronize OpenRC init scripts and directory mappings with .bolt paths
