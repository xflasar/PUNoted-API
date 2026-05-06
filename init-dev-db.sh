#!/bin/bash
set -e

echo "🔄 DEV MODE: Looking for latest Production backup..."

# Find the latest .sql.gz file in the mounted /backups/last folder
# (The backup container usually stores daily snapshots in 'last')
LATEST_BACKUP=$(ls -t /backups/last/*.sql.gz 2>/dev/null | head -n1)

if [ -z "$LATEST_BACKUP" ]; then
    echo "⚠️  No backup found in /backups/last/! Starting with empty DB."
else
    echo "📥 Found backup: $LATEST_BACKUP"
    echo "♻️  Restoring to Dev Database..."
    
    # Unzip and pipe directly into the Dev DB
    gunzip -c "$LATEST_BACKUP" | psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"
    
    echo "✅ Restore Complete!."
fi