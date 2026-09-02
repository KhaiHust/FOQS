#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$PROJECT_ROOT"

OUTPUT="foqs-bench/src/main/resources/bench-results.csv"
BENCH_JAR="foqs-bench/target/foqs-bench-1.0.0-SNAPSHOT.jar"
RATES=(5000 10000 15000 20000 25000)
REPEATS=2
WARMUP=20
DURATION=60

echo "Starting E2 3-shard ramp benchmark..."
for rate in "${RATES[@]}"; do
    for repeat in $(seq 0 $((REPEATS - 1))); do
        echo "================================================================"
        echo "Running 3-shard ramp: rate=$rate, repeat=$repeat, warmup=${WARMUP}s, duration=${DURATION}s"
        echo "================================================================"
        java -Xms2g -Xmx2g -jar "$BENCH_JAR" \
            --experiment baseline \
            --target-rate "$rate" \
            --host localhost \
            --port 8080 \
            --warmup "$WARMUP" \
            --duration "$DURATION" \
            --payload-bytes 256 \
            --channels 8 \
            --max-inflight 2048 \
            --consumers 4 \
            --consumer-rate 0 \
            --topics 60 \
            --shard-count 3 \
            --buffer-pool-size 2G \
            --batch-threshold 100 \
            --flush-interval-ms 10 \
            --max-pool-size 20 \
            --repeat-index "$repeat" \
            --output "$OUTPUT"
        echo "Completed rate=$rate repeat=$repeat"
        sleep 2
    done
done
echo "E2 3-shard ramp benchmark complete!"
