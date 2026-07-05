#!/bin/bash
set -e

echo "[$(date)] Starting Token Cleanup..."

# ==============================================================================
# 1. EXPIRED TOKENS CLEANUP
# ==============================================================================
echo "Cleaning up expired user tokens..."
psql -h "$PGHOST" -U "$PGUSER" -d "$PGDATABASE" -c "
    DELETE FROM user_tokens 
    WHERE expiresat < NOW();
"

# ==============================================================================
# 2. VACUUM (Optional but Recommended)
# ==============================================================================
echo "Running VACUUM ANALYZE for user_tokens..."
psql -h "$PGHOST" -U "$PGUSER" -d "$PGDATABASE" -c "
    VACUUM ANALYZE user_tokens;
"

echo "[$(date)] Token cleanup completed successfully."