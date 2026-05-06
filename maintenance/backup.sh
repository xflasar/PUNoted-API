#!/bin/bash
set -e
set -o pipefail

# --- CONFIGURATION ---
TIMESTAMP=$(date +"%Y-%m-%d_%H-%M-%S")
DAY_OF_WEEK=$(date +"%A") # e.g., "Monday"

# Local file path
LOCAL_FILE="/backups/db_backup_$TIMESTAMP.sql.gz"

# Google Drive Destination
# "gdrive" is the name of the config profile we will create in Step 4
# "db_backups" is the folder inside your Google Drive
REMOTE_DEST="gdrive:db_backups/db_backup_$DAY_OF_WEEK.sql.gz"

echo "[$(date)] Starting Backup..."

# 1. Dump Database
pg_dumpall -h "$PGHOST" -U "$PGUSER" --clean --if-exists | gzip > "$LOCAL_FILE"

# 2. Upload to Google Drive
# 'copyto' uploads the file and renames it to the destination name simultaneously
echo "[$(date)] Uploading to Google Drive as $REMOTE_DEST..."

# --config points to where we will mount the file in Docker
rclone copyto "$LOCAL_FILE" "$REMOTE_DEST" --config /config/rclone.conf

# 3. Local Cleanup
find /backups -type f -name "*.sql.gz" -mtime +30 -delete

echo "[$(date)] Backup process completed."