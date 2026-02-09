#!/bin/bash

# Script to test import data dump into a temporary Docker container
# Usage: ./test_import.sh <dump_file>
# Example: ./test_import.sh oregon_app_taxlot.dump

set -euo pipefail

# Check if dump file is provided
if [ $# -eq 0 ]; then
    echo "Error: No dump file provided."
    echo "Usage: $0 <dump_file>"
    echo "Example: $0 oregon_app_taxlot.dump"
    exit 1
fi

DUMP_FILE="$1"

# Validate file exists
if [ ! -f "$DUMP_FILE" ]; then
    echo "Error: File '$DUMP_FILE' not found."
    exit 1
fi

# Extract table name from filename (remove .dump or .sql.gz extension)
TABLE_NAME=$(basename "$DUMP_FILE" | sed -e 's/\.dump$//' -e 's/\.sql\.gz$//')

# Configuration
CONTAINER_NAME="dpl-db-test"
IMAGE_NAME="postgis/postgis"
HOST_PORT="5434"
TEST_DB="test_gis"
TEST_USER="test_gis"
TEST_PASS="test_password"

# Cleanup function
cleanup() {
    EXIT_CODE=$?
    echo ""
    # Always show logs if we failed
    if [ $EXIT_CODE -ne 0 ]; then
        echo "Exited with error (Code: $EXIT_CODE). Last 50 lines of container logs:"
        docker logs --tail 50 "$CONTAINER_NAME"
    fi

    # If the container stopped unexpectedly, show logs
    if [ "$(docker inspect -f '{{.State.Running}}' "$CONTAINER_NAME" 2>/dev/null)" == "false" ]; then
        echo "WARNING: Container stopped unexpectedly!"
    fi
    echo "Cleaning up container..."
    docker stop "$CONTAINER_NAME" >/dev/null 2>&1 || true
    docker rm "$CONTAINER_NAME" >/dev/null 2>&1 || true
    echo "Cleanup complete."
}

# Set trap to cleanup on exit
trap cleanup EXIT

echo "Starting temporary PostgreSQL container..."
echo "  Container: $CONTAINER_NAME"
echo "  Image: $IMAGE_NAME"
echo "  Port: $HOST_PORT"
echo "  Database: $TEST_DB"
echo "  User: $TEST_USER"
echo ""

# Start temp container
docker run -d \
    --name "$CONTAINER_NAME" \
    -e POSTGRES_DB="$TEST_DB" \
    -e POSTGRES_USER="$TEST_USER" \
    -e POSTGRES_PASSWORD="$TEST_PASS" \
    --shm-size=512m \
    -p "${HOST_PORT}:5432" \
    "$IMAGE_NAME" >/dev/null

echo "Waiting for database to be ready..."
# Wait for PostgreSQL to be ready (max 60 seconds)
# We wait for the "PostgreSQL init process complete; ready for start up." message
for i in {1..60}; do
    if docker logs "$CONTAINER_NAME" 2>&1 | grep -q "PostgreSQL init process complete; ready for start up."; then
        if docker exec "$CONTAINER_NAME" pg_isready -U "$TEST_USER" -d "$TEST_DB" >/dev/null 2>&1; then
            echo "Database is ready."
            break
        fi
    fi
    echo "Waiting for database initialization... ($i/60)"
    if [ $i -eq 60 ]; then
        echo "Error: Database failed to start within 60 seconds."
        exit 1
    fi
    sleep 1
done

# Enable PostGIS extension
echo "Enabling PostGIS extension..."
docker exec -i "$CONTAINER_NAME" psql -U "$TEST_USER" -d "$TEST_DB" -c "CREATE EXTENSION IF NOT EXISTS postgis;"

echo ""
echo "Restoring dump file: $DUMP_FILE"
echo "  Table name: $TABLE_NAME"
echo ""

# Restore the dump file
echo "Restoring data (verbose)..."
# Custom format restore
# Note: pg_restore might return 1 for minor warnings, so we allow it to continue
docker exec -i "$CONTAINER_NAME" pg_restore -U "$TEST_USER" -d "$TEST_DB" \
    --verbose \
    --no-owner --no-privileges --no-comments \
    < "$DUMP_FILE" || echo "Note: pg_restore finished with some warnings (check logs above)."

echo "Restore completed successfully."
echo ""

# Generate table summary
echo "========================================"
echo "DATABASE CONTENT OVERVIEW"
echo "========================================"
docker exec -i "$CONTAINER_NAME" psql -U "$TEST_USER" -d "$TEST_DB" -c "\dt public.*"

# Check if the specific table exists
TABLE_EXISTS=$(docker exec -i "$CONTAINER_NAME" psql -U "$TEST_USER" -d "$TEST_DB" -t -A \
    -c "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'public' AND table_name = '$TABLE_NAME');")

if [ "$TABLE_EXISTS" != "t" ]; then
    echo ""
    echo "ERROR: Table 'public.$TABLE_NAME' does not exist in the database!"
    echo "Possible causes:"
    echo "  1. pg_restore/psql failed (check logs above)"
    echo "  2. Table was created with a different name"
    echo "  3. Table was created in a different schema"
    exit 1
fi

echo ""
echo "========================================"
echo "TABLE SUMMARY: public.$TABLE_NAME"
echo "========================================"
echo ""

# Get column information
echo "Column Information:"
echo "-------------------"
# Querying specifically in the public schema
docker exec -i "$CONTAINER_NAME" psql -U "$TEST_USER" -d "$TEST_DB" -t -A -F" | " <<EOF | (column -t -s'|' || cat)
SELECT 
    column_name AS "Column",
    data_type AS "Data Type",
    COALESCE(character_maximum_length::text, 
             numeric_precision::text || ',' || numeric_scale::text) AS "Size/Precision",
    is_nullable AS "Nullable"
FROM information_schema.columns 
WHERE table_name = '$TABLE_NAME'
  AND table_schema = 'public'
ORDER BY ordinal_position;
EOF

echo ""
echo "-------------------"

# Get row count with explicit schema
ROW_COUNT=$(docker exec -i "$CONTAINER_NAME" psql -U "$TEST_USER" -d "$TEST_DB" -t -A \
    -c "SELECT COUNT(*) FROM public.$TABLE_NAME;")
echo "Total Rows: $ROW_COUNT"
echo ""

# Get table size with explicit schema
TABLE_SIZE=$(docker exec -i "$CONTAINER_NAME" psql -U "$TEST_USER" -d "$TEST_DB" -t -A \
    -c "SELECT pg_size_pretty(pg_total_relation_size('public.$TABLE_NAME'));")
echo "Table Size (with indexes): $TABLE_SIZE"
echo ""

echo "========================================"
echo ""
echo "Connection Info:"
echo "  Host: localhost"
echo "  Port: $HOST_PORT"
echo "  Database: $TEST_DB"
echo "  User: $TEST_USER"
echo "  Password: $TEST_PASS"
echo ""

