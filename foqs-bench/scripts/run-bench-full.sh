#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────
# FOQS Bench — Full Experiment Matrix (overnight run)
#
# Experiments:
#   1. Baseline ramp   — find the p99 knee
#   2. Batch sweep      — batchThreshold × flushIntervalMs grid
#   3. Backlog          — enqueue >> dequeue, small buffer pool
#   4. Lease recovery   — SIGKILL consumers, measure redelivery
#
# Total estimated runtime: ~5-6 hours
#
# Usage:  ./foqs-bench/scripts/run-bench-full.sh
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
OUTPUT="bench-results.csv"
REPEATS="${REPEATS:-2}"
WARMUP="${WARMUP:-20}"
DURATION="${DURATION:-60}"
BENCH_JAR="foqs-bench/target/foqs-bench-1.0.0-SNAPSHOT.jar"

SERVER_JVM_FLAGS="-Xms4g -Xmx4g -XX:+UseG1GC -XX:MaxGCPauseMillis=20"
BENCH_JVM_FLAGS="-Xms2g -Xmx2g"
BENCH_TEMP_DIR="/tmp/foqs-bench-config"

# ── Colors ──
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
RED='\033[0;31m'
NC='\033[0m'

log()    { echo -e "${GREEN}[BENCH]${NC} $(date '+%H:%M:%S') $*"; }
info()   { echo -e "${CYAN}[BENCH]${NC} $(date '+%H:%M:%S') $*"; }
warn()   { echo -e "${YELLOW}[BENCH]${NC} $(date '+%H:%M:%S') $*"; }
err()    { echo -e "${RED}[BENCH]${NC} $(date '+%H:%M:%S') $*"; }

SERVER_PID=""

cleanup() {
    log "Cleaning up..."
    stop_server
    rm -rf "$BENCH_TEMP_DIR"
}
trap cleanup EXIT

# ═══════════════════════════════════════════════════════════════
#  Helper functions
# ═══════════════════════════════════════════════════════════════

start_mysql() {
    local buffer_pool_bytes="$1"
    log "Starting MySQL (buffer_pool=$1 bytes)..."
    # Stop existing container
    docker compose -f docker/docker-compose-bench.yml down 2>/dev/null || true
    docker compose -f docker/docker-compose.yml down 2>/dev/null || true
    docker stop foqs-mysql-shard-0 foqs-mysql-bench 2>/dev/null || true
    docker rm foqs-mysql-shard-0 foqs-mysql-bench 2>/dev/null || true

    INNODB_BUFFER_POOL_SIZE="$buffer_pool_bytes" \
        docker compose -f docker/docker-compose-bench.yml up -d

    log "Waiting for MySQL..."
    for i in $(seq 1 60); do
        if docker exec foqs-mysql-bench mysqladmin ping -h localhost -u root -proot --silent 2>/dev/null; then
            log "MySQL ready."
            return
        fi
        sleep 2
    done
    err "MySQL failed to start within 120s!"
    exit 1
}

resize_buffer_pool() {
    local size_bytes="$1"
    log "Resizing InnoDB buffer pool to $size_bytes bytes..."
    docker exec foqs-mysql-bench mysql -u root -proot -e \
        "SET GLOBAL innodb_buffer_pool_size = $size_bytes;" 2>/dev/null
    sleep 5  # Wait for resize to take effect
    docker exec foqs-mysql-bench mysql -u root -proot -e \
        "SELECT @@innodb_buffer_pool_size AS buffer_pool_size;" 2>/dev/null
}

run_migration() {
    log "Running Liquibase migration..."
    local migration_cp
    migration_cp=$(mvn -q -pl foqs-migration dependency:build-classpath -Dmdep.outputFile=/dev/stdout 2>/dev/null)
    java -cp "foqs-migration/target/classes:foqs-common/target/classes:$migration_cp" \
        project.khaihust.foqs.migration.MigrationRunner 2>/dev/null || {
        warn "Migration failed (table may already exist)"
    }
}

start_server() {
    local batch_threshold="$1"
    local flush_interval_ms="$2"
    local max_pool_size="${3:-20}"

    stop_server

    # Create profile-specific properties file on a temp classpath
    mkdir -p "$BENCH_TEMP_DIR"
    cat > "$BENCH_TEMP_DIR/application-bench.properties" << EOF
foqs.profile=bench
foqs.buffer.batch-size-threshold=$batch_threshold
foqs.buffer.flush-interval-ms=$flush_interval_ms
foqs.datasource.max-pool-size=$max_pool_size
foqs.server.port=$SERVER_PORT
foqs.shards.count=1
foqs.shards.0.url=$MYSQL_URL
foqs.shards.0.username=$MYSQL_USER
foqs.shards.0.password=$MYSQL_PASS
EOF

    local core_cp
    core_cp=$(mvn -q -pl foqs-core dependency:build-classpath -Dmdep.outputFile=/dev/stdout 2>/dev/null)

    log "Starting FOQS server: batchThreshold=$batch_threshold flushInterval=$flush_interval_ms maxPool=$max_pool_size"
    java $SERVER_JVM_FLAGS \
        -Dfoqs.profile=bench \
        -cp "$BENCH_TEMP_DIR:foqs-core/target/classes:foqs-common/target/classes:$core_cp" \
        project.khaihust.foqs.core.Application &
    SERVER_PID=$!

    # Wait for server to be ready
    for i in $(seq 1 30); do
        if lsof -i ":$SERVER_PORT" -sTCP:LISTEN >/dev/null 2>&1; then
            log "Server PID=$SERVER_PID listening on port $SERVER_PORT"
            return
        fi
        sleep 1
    done
    err "Server failed to start!"
    exit 1
}

stop_server() {
    if [ -n "$SERVER_PID" ]; then
        log "Stopping server PID=$SERVER_PID..."
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
        SERVER_PID=""
        sleep 2
    fi
}

run_bench() {
    local experiment="$1"
    local target_rate="$2"
    local repeat_index="$3"
    local batch_threshold="$4"
    local flush_interval_ms="$5"
    local max_pool_size="$6"
    local buffer_pool_size="$7"
    local warmup="${8:-$WARMUP}"
    local duration="${9:-$DURATION}"
    local consumer_rate="${10:-0}"
    local extra_args="${11:-}"

    info "▶ $experiment | rate=$target_rate | bt=$batch_threshold fi=$flush_interval_ms | repeat=$repeat_index"

    java $BENCH_JVM_FLAGS -jar "$BENCH_JAR" \
        --experiment "$experiment" \
        --target-rate "$target_rate" \
        --host localhost \
        --port "$SERVER_PORT" \
        --warmup "$warmup" \
        --duration "$duration" \
        --payload-bytes 256 \
        --channels 8 \
        --max-inflight 2048 \
        --consumers 4 \
        --consumer-rate "$consumer_rate" \
        --output "$OUTPUT" \
        --shard-count 1 \
        --buffer-pool-size "$buffer_pool_size" \
        --batch-threshold "$batch_threshold" \
        --flush-interval-ms "$flush_interval_ms" \
        --max-pool-size "$max_pool_size" \
        --jvm-flags "$SERVER_JVM_FLAGS" \
        --repeat-index "$repeat_index" \
        --mysql-url "$MYSQL_URL" \
        --mysql-user "$MYSQL_USER" \
        --mysql-password "$MYSQL_PASS" \
        $extra_args

    info "✓ Completed: $experiment rate=$target_rate repeat=$repeat_index"
}

# ═══════════════════════════════════════════════════════════════
#  Build
# ═══════════════════════════════════════════════════════════════

log "╔══════════════════════════════════════════════════════╗"
log "║       FOQS BENCHMARK — FULL EXPERIMENT MATRIX       ║"
log "║       Started: $(date)        ║"
log "╚══════════════════════════════════════════════════════╝"

log "Building project..."
mvn -q clean package -pl foqs-bench -am -DskipTests

# ═══════════════════════════════════════════════════════════════
#  MySQL & Migration
# ═══════════════════════════════════════════════════════════════

start_mysql 4294967296  # 4GB buffer pool
run_migration

# ═══════════════════════════════════════════════════════════════
#  Experiment 1: Baseline Ramp (Already completed, skipping)
# ═══════════════════════════════════════════════════════════════

log "━━━ EXPERIMENT 1: BASELINE RAMP (Skipped - already captured in bench-results.csv) ━━━"

# ═══════════════════════════════════════════════════════════════
#  Experiment 2: Batch Sweep
#  Run at ~80% of the knee rate (~4,800 msg/s).
# ═══════════════════════════════════════════════════════════════

log "━━━ EXPERIMENT 2: BATCH SWEEP ━━━"
MP=20
KNEE_RATE="${KNEE_RATE:-6000}"
SWEEP_RATE="${SWEEP_RATE:-$((KNEE_RATE * 80 / 100))}"
log "Using sweep rate: $SWEEP_RATE (80% of knee=$KNEE_RATE)"

if [ "${GRID_MODE:-full}" = "fast" ]; then
    BATCH_THRESHOLDS=(10 100 500)
    FLUSH_INTERVALS=(1 10 50)
else
    BATCH_THRESHOLDS=(10 50 100 500)
    FLUSH_INTERVALS=(1 5 10 50)
fi

for bt in "${BATCH_THRESHOLDS[@]}"; do
    for fi in "${FLUSH_INTERVALS[@]}"; do
        # Restart server with new config
        start_server "$bt" "$fi" "$MP"

        for repeat in $(seq 0 $((REPEATS - 1))); do
            run_bench "batch_sweep" "$SWEEP_RATE" "$repeat" "$bt" "$fi" "$MP" "4G"
        done
    done
done

log "Batch sweep complete."

# ═══════════════════════════════════════════════════════════════
#  Experiment 3: Backlog (small buffer pool)
# ═══════════════════════════════════════════════════════════════

log "━━━ EXPERIMENT 3: BACKLOG ━━━"
# Resize MySQL buffer pool to 512M to force working set spill at ~800k rows
resize_buffer_pool 536870912  # 512 MB

start_server 100 10 20

BACKLOG_RATE="${BACKLOG_RATE:-$SWEEP_RATE}"

for repeat in $(seq 0 $((REPEATS - 1))); do
    run_bench "backlog" "$BACKLOG_RATE" "$repeat" 100 10 20 "512M" 60 180 "$((BACKLOG_RATE / 2))"
done

log "Backlog experiment complete."

# Restore buffer pool
resize_buffer_pool 4294967296  # 4GB

# ═══════════════════════════════════════════════════════════════
#  Experiment 4: Lease Recovery
# ═══════════════════════════════════════════════════════════════

log "━━━ EXPERIMENT 4: LEASE RECOVERY ━━━"
start_server 100 10 20

for repeat in $(seq 0 $((REPEATS - 1))); do
    run_bench "lease_recovery" 5000 "$repeat" 100 10 20 "4G" 0 60
done

log "Lease recovery experiment complete."

# ═══════════════════════════════════════════════════════════════
#  Summary
# ═══════════════════════════════════════════════════════════════

stop_server

TOTAL_ROWS=$(($(wc -l < "$OUTPUT") - 1))
log "╔══════════════════════════════════════════════════════╗"
log "║       ALL EXPERIMENTS COMPLETE                      ║"
log "║       Total runs: $TOTAL_ROWS                              ║"
log "║       Results in: $OUTPUT                    ║"
log "║       Finished: $(date)        ║"
log "╚══════════════════════════════════════════════════════╝"
