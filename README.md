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
| **Baseline (Linear Regime)** | 1,000 msg/s | 999.99 msg/s | **7.4 ms** | **14.4 ms** | **29.1 ms** | Predictable sub-30ms p99 latency |
| **Baseline (Sustained Sweet Spot)** | 6,000 msg/s | 5,999.97 msg/s | **9.5 ms** | **58.2 ms** | **102.2 ms** | Clean throughput with bounded sub-105ms p99 |
| **Baseline (Latency Knee)** | 7,000 msg/s | 6,999.97 msg/s | **12.0 ms** | **168.1 ms** | **571.9 ms** | Latency knee inflection point (p99 > 500ms) |
| **Baseline (Throughput Ceiling)** | 8,000 msg/s | 7,999.98 msg/s | **13.7 ms** | **184.2 ms** | **239.1 ms** | Max single-shard capacity (~8k sustained; 10.6k dequeue) |
| **Baseline (Saturation Cliff)**| 10,000 msg/s | 8,980.44 msg/s | 9.2 s | 33.9 s | 38.7 s | Saturation cliff; queuing runaway ($p99 > 38\text{s}$) |
| **Optimal Batching** (`bt=100, fi=1ms`) | 4,800 msg/s | 4,799.97 msg/s | **1.2 ms** | **28.9 ms** | **48.6 ms** | >60% p99 reduction vs small batches (`bt=10`) |
| **Lease Recovery** | 5,000 msg/s | — | — | — | **~225 ms** | Consumer SIGKILL redelivery with zero message loss |

---

## Documentation

Full architectural design, sequence diagrams, design mapping with Meta's paper, gRPC API reference, and detailed benchmark analysis are available in [DOCS.md](DOCS.md).


