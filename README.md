# Proxmox Alpine LXC Deployment Guide

This guide provides step-by-step instructions to compile and deploy the statically linked Go addon binary directly on an ultra-lightweight **Alpine Linux LXC container** in Proxmox VE, utilizing OpenRC as a standalone system service.

---

## 1. Architectural Architecture & Footprint

Running the standalone compiled binary on Alpine Linux inside a Proxmox LXC offers the absolute lowest possible resource footprint:

* **Disk Footprint:** ~10 MB total (excluding SQLite database storage).
* **Memory Footprint:** Under 10 MB idle RAM.
* **CPU Footprint:** Near 0% idle CPU usage.

To achieve this, the Go toolchain compiles a fully static binary with CGO disabled. This eliminates dependencies on standard dynamic libraries (`glibc`/`musl`), allowing the binary to run standalone on Alpine.

---

## 2. Compiling the Static Binary

### Method A: Manual Compilation

Execute this command on your development machine to build a fully independent static binary:

```bash
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -installsuffix cgo -o stremio-mvshows ./cmd/server
```

### Method B: Automated CI/CD (GitHub Actions)

If you push a tag (e.g. `v1.1.2`), the `.github/workflows/release.yml` pipeline automatically compiles static binaries for both `linux/amd64` and `linux/arm64`, and attaches them directly to your repository's Releases page.

You can simply download the binary from there.

---

## 3. Creating the Alpine Container in Proxmox

1. Log in to your Proxmox VE Web UI.
2. Navigate to **local storage → CT Templates → Templates** and download the latest `alpine-3.x-default` template.
3. Click **Create CT** (upper-right corner) and configure:

### General

- Uncheck **Privileged Container** (for security).
- Check **Nesting**.

### Template

- Select the downloaded Alpine default archive.

### Root Disk

- Set size to **2.00 GiB to 4.00 GiB** (plenty of room for database growth).

### CPU

- Allocate **1 Core**.

### Memory

- Set RAM to **256 MiB** (or **512 MiB**).
- Set Swap to **256 MiB**.

### Network

- Set up either:
  - Static IPv4
  - DHCP

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

To prevent having to edit the system init file every time you change configuration values, we separate configuration from execution using OpenRC's native `/etc/conf.d/` mapping.

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

# Auto-vacuum configuration
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

Paste this optimized script. It uses a single-line shell pipeline to automatically extract and export all variable names declared in your config file, and sets the working directory explicitly to `/usr/local/bin` to allow Gin to resolve your static frontend assets.

```bash
#!/sbin/openrc-run
# Version: 1.1.4
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
    # Ensure SQLite database folder exists
    mkdir -p /data

    # Dynamic Auto-Exporter:
    # Slices and exports all custom variable assignments from /etc/conf.d/stremio-mvshows
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

### Monitoring the Outputs

To monitor standard outputs and server initialization diagnostics in real-time, run:

```bash
tail -f /var/log/stremio-mvshows.log
```

If any connection or boot-level errors occur, they will be logged here:

```bash
tail -f /var/log/stremio-mvshows.err
```
