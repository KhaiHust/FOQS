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

## Documentation

Full architectural design, sequence diagrams, design mapping with Meta's paper, and gRPC API reference are available in [DOCS.md](file:///Users/khaitran/Projects/FOQS/DOCS.md).

