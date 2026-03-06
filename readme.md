# Banking CDC Streaming Pipeline 🚀

Real-time Change Data Capture (CDC) pipeline demonstrating streaming data engineering for banking use cases including AML screening, fraud detection, and Customer 360.

## 🎯 Project Overview

**Purpose:** Showcase real-time data engineering capabilities.

**Architecture:**
```
PostgreSQL → CDC Events → Kafka → Spark Streaming → Delta Lake → Real-time Analytics
```

**Key Technologies:**
- **Apache Kafka**: Message streaming platform
- **Spark Structured Streaming**: Real-time data processing
- **Delta Lake**: Lakehouse storage with ACID transactions
- **MinIO**: S3-compatible object storage
- **PostgreSQL**: Source database simulation

## 🏗️ Architecture
```
┌─────────────────┐
│   PostgreSQL    │  Source database (simulated)
│  Banking Core   │
└────────┬────────┘
         │ CDC Events (Debezium format)
         ▼
┌─────────────────┐
│  Kafka Cluster  │  3 topics: transactions, customers, accounts
│   (3 brokers)   │
└────────┬────────┘
         │ Stream processing
         ▼
┌─────────────────┐
│ Spark Streaming │  Real-time transformations & AML screening
│  (3 workers)    │
└────────┬────────┘
         │ Delta Lake writes
         ▼
┌─────────────────┐
│   MinIO (S3)    │  Bronze → Silver → Gold layers
│  Lakehouse      │
└─────────────────┘
```

## 🚀 Quick Start

### Prerequisites
- Docker & Docker Compose
- 16GB RAM (recommended)
- 20GB free disk space

### 1. Start Infrastructure
```bash
cd docker
docker-compose up -d
```

**Verify services:**
```bash
docker-compose ps
```

Expected services: zookeeper, kafka, kafka-ui, minio, spark-master, spark-worker-1, spark-worker-2, postgres, python-env

### 2. Create Kafka Topics
```bash
cd ..  # Back to project root
./scripts/create_kafka_topics.sh
```

### 3. Run CDC Simulator
```bash
# 10 minutes at 20 events/minute
docker exec -it cdc-python-env python /app/scripts/cdc_simulator.py \
    --duration 10 \
    --rate 20

# For higher volume testing (30 events/minute for 30 minutes)
docker exec -it cdc-python-env python /app/scripts/cdc_simulator.py \
    --duration 30 \
    --rate 30
```

### 4. Monitor in Real-Time

- **Kafka UI**: http://localhost:8080
- **MinIO Console**: http://localhost:9001 (admin/admin123)
- **Spark Master**: http://localhost:8081

## 📊 What Gets Generated

### Bootstrap Phase
- **100 customers** with realistic banking profiles
- **200 accounts** (savings, checking, credit cards, loans)

### Streaming Phase
Event mix (configurable rate):
- **60%** Regular transactions
- **10%** High-risk transactions (AML testing)
- **10%** Customer profile updates
- **15%** Account balance updates
- **5%** New account creation

### CDC Event Format

Example transaction event (Debezium format):
```json
{
  "payload": {
    "op": "c",
    "source": {
      "db": "banking_core",
      "table": "transactions",
      "ts_ms": 1709467200000
    },
    "after": {
      "transaction_id": "TXN123456789012",
      "account_id": "ACC9876543210",
      "amount": 75000.00,
      "merchant_category": "CRYPTO",
      "country": "US",
      "is_international": true
    }
  }
}
```

## 🔧 Configuration

### Environment Variables

Edit `.env` file for custom configuration:
```bash
# CDC Simulator
CDC_EVENTS_PER_MINUTE=20
CDC_DURATION_MINUTES=60
CDC_BOOTSTRAP_CUSTOMERS=100
CDC_BOOTSTRAP_ACCOUNTS=200

# Kafka
KAFKA_BOOTSTRAP_SERVERS_INTERNAL=kafka:9093

# Streaming
STREAMING_TRIGGER_INTERVAL=10 seconds
STREAMING_MAX_OFFSETS_PER_TRIGGER=1000
```

### Port Mappings

| Service | External Port | Internal Port |
|---------|---------------|---------------|
| Kafka | 9092 | 9093 |
| Kafka UI | 8080 | 8080 |
| MinIO API | 9000 | 9000 |
| MinIO Console | 9001 | 9001 |
| Spark Master UI | 8081 | 8080 |
| Spark App UI | 4040 | 4040 |
| PostgreSQL | 5433 | 5432 |

## 📁 Project Structure
```
banking-cdc-streaming/
├── docker/
│   ├── docker-compose.yml       # Infrastructure definition
│   ├── postgres/init.sql        # Database schema
│   └── python/                  # Python environment
├── spark-streaming-jobs/        # Spark applications (Phase 2+)
├── scripts/
│   ├── cdc_simulator.py        # Event generator
│   └── create_kafka_topics.sh  # Topic setup
├── monitoring/                  # Dashboards (Phase 5)
├── data/
│   ├── lakehouse/              # Delta Lake storage
│   └── checkpoints/            # Streaming checkpoints
└── README.md
```

## 🎯 Use Cases Demonstrated

### 1. Real-Time AML Screening
- Detect high-value transactions (>AED 50K)
- Flag suspicious merchant categories (crypto, gambling)
- Geographic anomaly detection
- Velocity checks (transaction frequency)

### 2. Fraud Detection
- Duplicate transaction detection
- Unusual spending patterns
- Cross-border anomalies

### 3. Customer 360
- Real-time profile updates
- Transaction history aggregation
- Risk score recalculation

## 🧪 Testing & Verification

### Check Kafka Topics
```bash
# List topics
docker exec -it cdc-kafka kafka-topics --list \
    --bootstrap-server localhost:9092

# Consume messages
docker exec -it cdc-kafka kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic banking.transactions \
    --from-beginning \
    --max-messages 10
```

### Check MinIO Buckets
Visit http://localhost:9001, login (admin/admin123), verify buckets:
- `bronze`, `silver`, `gold`, `checkpoints`

### Verify PostgreSQL
```bash
docker exec -it cdc-postgres psql -U banking_user -d banking_core

# List tables
\dt

# Check table structure
\d customers
```

## 🛠️ Troubleshooting

### Kafka Connection Issues
```bash
# Check Kafka is running
docker logs cdc-kafka

# Test connectivity
docker exec -it cdc-kafka kafka-broker-api-versions \
    --bootstrap-server localhost:9092
```

### CDC Simulator Not Publishing
```bash
# Check Python environment
docker exec -it cdc-python-env pip list | grep kafka

# Check Kafka from Python container
docker exec -it cdc-python-env python -c "from kafka import KafkaProducer; print('OK')"
```

### Disk Space Issues
```bash
# Check Docker volumes
docker system df

# Clean up if needed
docker system prune -a --volumes
```

## 📈 What's Next (Future Phases)

- **Phase 2**: Spark Structured Streaming consumer (Bronze layer)
- **Phase 3**: Silver layer transformations (MERGE/UPSERT)
- **Phase 4**: Real-time AML screening (Gold layer)
- **Phase 5**: Monitoring dashboard (Streamlit)

## 🎓 Interview Talking Points

**Technical Depth:**
- "Built a CDC pipeline using Kafka for reliable, scalable event streaming"
- "Debezium-format events capture full before/after state for audit trails"
- "Kafka topics partitioned by entity for parallelism and ordered processing"

**Banking Domain:**
- "AML screening requires sub-second latency - CDC enables real-time monitoring"
- "Full transaction history maintained for CBUAE regulatory compliance"
- "High-risk patterns detected immediately: high value, crypto, offshore transactions"

**Architecture Decisions:**
- "Separate project from batch lakehouse shows understanding of both paradigms"
- "Kafka provides durability and replay capability for exactly-once semantics"
- "Delta Lake enables time travel for regulatory point-in-time queries"

**Author**: Belal  
**Date**: March 2026