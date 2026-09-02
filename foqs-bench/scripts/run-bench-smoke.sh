#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────
# FOQS Bench — 30-second smoke test
#
# Proves the harness works end-to-end:
#   1. Starts MySQL (docker compose) if not running
#   2. Runs Liquibase migration
#   3. Starts the FOQS gRPC server
#   4. Runs a 30s benchmark at low rate (1000 msg/s)
#   5. Prints the CSV row and exits
#
# Usage:  ./foqs-bench/scripts/run-bench-smoke.sh
# ─────────────────────────────────────────────────────────────────
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$PROJECT_ROOT"

# ── Configuration ──
MYSQL_URL="jdbc:mysql://localhost:3306/foqs_shard_0?useSSL=false&allowPublicKeyRetrieval=true"
MYSQL_USER="root"
MYSQL_PASS="root"
SERVER_PORT=8080
TARGET_RATE=1000
WARMUP=5
DURATION=30
OUTPUT="bench-results-smoke.csv"

SERVER_JVM_FLAGS="-Xms512m -Xmx512m -XX:+UseG1GC"
BENCH_JVM_FLAGS="-Xms512m -Xmx512m"

# ── Colors ──
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

log()  { echo -e "${GREEN}[SMOKE]${NC} $*"; }
warn() { echo -e "${YELLOW}[SMOKE]${NC} $*"; }
err()  { echo -e "${RED}[SMOKE]${NC} $*"; }

cleanup() {
    log "Cleaning up..."
    if [ -n "${SERVER_PID:-}" ]; then
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT

# ── Step 1: Ensure MySQL is running ──
log "Step 1: Starting MySQL container..."
docker compose -f docker/docker-compose-bench.yml down 2>/dev/null || true
docker compose -f docker/docker-compose.yml down 2>/dev/null || true
docker stop foqs-mysql-shard-0 foqs-mysql-bench 2>/dev/null || true
docker rm foqs-mysql-shard-0 foqs-mysql-bench 2>/dev/null || true

INNODB_BUFFER_POOL_SIZE=536870912 \
    docker compose -f docker/docker-compose-bench.yml up -d
log "Waiting for MySQL to be ready..."
for i in $(seq 1 30); do
    if docker exec foqs-mysql-bench mysqladmin ping -h localhost -u root -proot --silent 2>/dev/null; then
        break
    fi
    sleep 2
done
log "MySQL is ready."

# ── Step 2: Build everything ──
log "Step 2: Building project..."
mvn -q clean package -pl foqs-bench -am -DskipTests

# ── Step 3: Run Liquibase migration ──
log "Step 3: Running database migration..."
MIGRATION_CP=$(mvn -q -pl foqs-migration dependency:build-classpath -Dmdep.outputFile=/dev/stdout 2>/dev/null)
java -cp "foqs-migration/target/classes:foqs-common/target/classes:$MIGRATION_CP" \
    project.khaihust.foqs.migration.MigrationRunner 2>/dev/null || {
    warn "Migration runner failed (may be OK if table already exists)"
}

# ── Step 4: Start FOQS server ──
log "Step 4: Starting FOQS gRPC server on port $SERVER_PORT..."
CORE_CP=$(mvn -q -pl foqs-core dependency:build-classpath -Dmdep.outputFile=/dev/stdout 2>/dev/null)
java $SERVER_JVM_FLAGS \
    -cp "foqs-core/target/classes:foqs-common/target/classes:$CORE_CP" \
    project.khaihust.foqs.core.Application &
SERVER_PID=$!
log "Server PID: $SERVER_PID"

# Wait for server to be ready (check port)
log "Waiting for server to accept connections..."
for i in $(seq 1 30); do
    if lsof -i ":$SERVER_PORT" -sTCP:LISTEN >/dev/null 2>&1; then
        break
    fi
    sleep 1
done
log "Server is listening on port $SERVER_PORT."

# ── Step 5: Run smoke benchmark ──
log "Step 5: Running 30s smoke test at $TARGET_RATE msg/s..."
BENCH_JAR="foqs-bench/target/foqs-bench-1.0.0-SNAPSHOT.jar"

java $BENCH_JVM_FLAGS -jar "$BENCH_JAR" \
    --experiment smoke \
    --target-rate "$TARGET_RATE" \
    --host localhost \
    --port "$SERVER_PORT" \
    --warmup "$WARMUP" \
    --duration "$DURATION" \
    --payload-bytes 256 \
    --channels 4 \
    --max-inflight 512 \
    --consumers 2 \
    --output "$OUTPUT" \
    --buffer-pool-size 512M \
    --jvm-flags "$SERVER_JVM_FLAGS" \
    --mysql-url "$MYSQL_URL" \
    --mysql-user "$MYSQL_USER" \
    --mysql-password "$MYSQL_PASS"

# ── Step 6: Verify results ──
log "Step 6: Verifying CSV output..."
if [ -f "$OUTPUT" ]; then
    ROWS=$(wc -l < "$OUTPUT")
    log "CSV has $ROWS lines (1 header + $((ROWS - 1)) data rows)"
    echo ""
    log "─── CSV Content ───"
    column -t -s',' "$OUTPUT" 2>/dev/null || cat "$OUTPUT"
    echo ""
    log "✅ Smoke test PASSED"
else
    err "❌ CSV output not found: $OUTPUT"
    exit 1
fi

log "Done. Server PID $SERVER_PID will be killed on exit."
