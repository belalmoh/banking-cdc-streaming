#!/bin/bash
# Create Kafka topics for CDC streaming pipeline

set -e

KAFKA_CONTAINER="cdc-kafka"
BOOTSTRAP_SERVER="localhost:9092"

echo "======================================================================"
echo "Creating Kafka Topics for CDC Pipeline"
echo "======================================================================"

# Check if Kafka container is running
if ! docker ps | grep -q $KAFKA_CONTAINER; then
    echo "❌ Error: Kafka container '$KAFKA_CONTAINER' is not running"
    echo "   Start it with: cd docker && docker-compose up -d kafka"
    exit 1
fi

# Wait for Kafka to be ready
echo "⏳ Waiting for Kafka to be ready..."
sleep 5

# Function to create topic
create_topic() {
    local topic_name=$1
    local partitions=$2
    local retention_ms=$3
    local description=$4
    
    echo ""
    echo "📝 Creating topic: $topic_name"
    echo "   Partitions: $partitions"
    echo "   Retention: $retention_ms ms (~$((retention_ms / 86400000)) days)"
    echo "   Purpose: $description"
    
    docker exec -it $KAFKA_CONTAINER kafka-topics --create \
        --topic $topic_name \
        --bootstrap-server $BOOTSTRAP_SERVER \
        --partitions $partitions \
        --replication-factor 1 \
        --config retention.ms=$retention_ms \
        --if-not-exists
    
    if [ $? -eq 0 ]; then
        echo "   ✅ Topic '$topic_name' created successfully"
    else
        echo "   ⚠️  Topic '$topic_name' may already exist"
    fi
}

# Create topics
create_topic "banking.transactions" 3 604800000 "Transaction CDC events (high volume)"
create_topic "banking.customers" 2 2592000000 "Customer CDC events"
create_topic "banking.accounts" 2 2592000000 "Account CDC events"
create_topic "banking.aml_alerts" 1 7776000000 "AML alert events (compliance)"

# List all topics
echo ""
echo "======================================================================"
echo "📋 All Kafka Topics"
echo "======================================================================"
docker exec -it $KAFKA_CONTAINER kafka-topics --list \
    --bootstrap-server $BOOTSTRAP_SERVER

echo ""
echo "======================================================================"
echo "✅ Kafka topics created successfully!"
echo ""
echo "Next steps:"
echo "  1. Run CDC simulator:"
echo "     docker exec -it cdc-python-env python /app/scripts/cdc_simulator.py --duration 10 --rate 20"
echo ""
echo "  2. Monitor topics in Kafka UI:"
echo "     http://localhost:8080"
echo "======================================================================"