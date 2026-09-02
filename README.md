# FOQS (Facebook Ordered Queueing Service)

FOQS is an enterprise-grade, sharded priority message queue service built with Java 17, gRPC/Protobuf, and MySQL InnoDB.

> Inspired by Meta's production architecture: [**FOQS: Scaling a distributed priority queue** (Meta Engineering Blog)](https://engineering.fb.com/2021/02/22/production-engineering/foqs-scaling-a-distributed-priority-queue/).

## Key Features

- **High-Throughput Enqueue Pipeline**: In-memory ring buffer (`ProducerBatch`) with asynchronous micro-batch database insertion.
- **Low-Latency Dequeue Pipeline**: Proactive in-memory Min-Heap prefetching (`PrefetchBatch`) maintaining priority ordering.
- **Atomic Lease & Acknowledgment Lifecycle**:
  - `BatchAck`: Atomic completion marking (`status = 2`).
  - `BatchNack`: Exponential/fixed retry backoff with automatic Dead-Letter Queue (`status = 3`) routing.
- **Automated Lease Reclamation**: Background `LeaseReclaimer` monitoring expired leases and returning them to `READY` status.

## Quick Start & How to Run

### Prerequisites

- **Java 17+** (JDK)
- **Maven 3.8+**
- **Docker & Docker Compose**

---

### Step 1: Start MySQL Database Shard

Launch the MySQL 8.0 shard container:

```bash
docker compose -f docker/docker-compose.yml up -d
```

---

### Step 2: Run Database Migrations

Apply Liquibase schema migrations (`queue_messages` table and composite indexes) across all configured shards:

```bash
mvn clean compile exec:java -pl foqs-migration -Dexec.mainClass="project.khaihust.foqs.migration.MigrationRunner"
```

---

### Step 3: Build & Run Test Suite

Run all unit and full-flow end-to-end integration tests:

```bash
mvn clean test
```

---

### Step 4: Start FOQS gRPC Server

Start the core FOQS service (listens on port `8080` by default):

```bash
mvn exec:java -pl foqs-core -Dexec.mainClass="project.khaihust.foqs.core.Application"
```

---

### Step 5: Test with `grpcurl`

#### 1. Enqueue a message:
```bash
grpcurl -plaintext -d '{
  "topic": "orders",
  "priority": 1,
  "payload": "eydvcmRlcklkJzogOTkwMX0=",
  "deliver_after": 0
}' localhost:8080 project.khaihust.foqs.core.proto.EnqueueService/Enqueue
```

#### 2. Dequeue messages:
```bash
grpcurl -plaintext -d '{
  "topic": "orders",
  "count": 5,
  "timeout": 3000
}' localhost:8080 project.khaihust.foqs.core.proto.DequeueService/Dequeue
```

#### 3. Batch Acknowledge (ACK):
```bash
grpcurl -plaintext -d '{
  "message_ids": ["<MESSAGE_UUID>"]
}' localhost:8080 project.khaihust.foqs.core.proto.DequeueService/BatchAck
```

#### 4. Batch Negative Acknowledge (NACK):
```bash
grpcurl -plaintext -d '{
  "message_ids": ["<MESSAGE_UUID>"],
  "retry_delay_ms": 5000,
  "max_retry_count": 3
}' localhost:8080 project.khaihust.foqs.core.proto.DequeueService/BatchNack
```

---

## Performance & Benchmarking (`foqs-bench`)

FOQS includes an open-loop load testing module (`foqs-bench`) designed to avoid coordinated omission by scheduling sends at intended timestamps and capturing high-resolution latency percentiles with **HdrHistogram**.

### Running Benchmarks

#### Quick Smoke Test (30 seconds)
```bash
./foqs-bench/scripts/run-bench-smoke.sh
```

#### Full Experiment Matrix
```bash
./foqs-bench/scripts/run-bench-full.sh
```

### Empirical Results (Single MySQL 8.0 Shard, 4GB Buffer Pool, MacBook M4)

| Benchmark Scenario | Target Rate | Achieved Rate | p50 Latency | p95 Latency | p99 Latency | Key Takeaway |
| :--- | :---: | :---: | :---: | :---: | :---: | :--- |
| **Baseline (Linear Regime)** | 1,000 msg/s | 999.98 msg/s | **7.4 ms** | **14.3 ms** | **24.6 ms** | Sub-25ms p99 latency with zero queue buildup |
| **Baseline (High Efficiency)** | 5,000 msg/s | 4,999.98 msg/s | **6.4 ms** | **16.5 ms** | **33.4 ms** | Micro-batching keeps tail latency tightly bounded |
| **Baseline (Sweet Spot)** | 6,000 msg/s | 5,999.97 msg/s | **7.3 ms** | **25.9 ms** | **94.9 ms** | Sub-100ms p99 with 8ms p50 |
| **Baseline (High Load)** | 8,000 msg/s | 7,999.98 msg/s | **8.1 ms** | **37.1 ms** | **80.7 ms** | High ingestion throughput sustained cleanly |
| **Baseline (Max Single Shard)** | 10,000 msg/s | 9,999.98 msg/s | **8.6 ms** | **79.0 ms** | **167.6 ms** | 10k msg/s sustained with 13.3k msg/s dequeue capacity |
| **Optimal Batching** (`bt=50, fi=5ms`) | 4,800 msg/s | 4,799.97 msg/s | **3.2 ms** | **12.3 ms** | **25.3 ms** | Lowest p99 in grid (25.3ms) with zero filesort |
| **Backlog (560k depth, 512M pool)** | 4,800 msg/s | 4,799.99 msg/s | **7.3 ms** | **16.8 ms** | **23.6 ms** | Filesort completely eliminated via optimized index |
| **Lease Recovery (150k messages)** | 5,000 msg/s | — | — | — | **~30.0 s** | 100% of 150k un-acked messages reclaimed upon expiry |

---

## Documentation

Full architectural design, sequence diagrams, design mapping with Meta's paper, gRPC API reference, and detailed benchmark analysis are available in [DOCS.md](DOCS.md).


