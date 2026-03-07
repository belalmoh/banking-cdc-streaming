#!/bin/bash
# Setup and start all infrastructure

set -e

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

echo "======================================================================"
echo "🚀 Banking CDC Streaming - Infrastructure Setup"
echo "======================================================================"

# Stop any existing containers
echo "🛑 Stopping existing containers..."
cd docker
docker-compose down -v 2>/dev/null || true

# Build and start services
echo "🏗️  Building services..."
docker-compose build

echo "🚀 Starting services..."
docker-compose up -d

echo "⏳ Waiting for services to be healthy..."
sleep 30

# Check service health
echo ""
echo "📊 Service Health Check:"
echo "----------------------------------------------------------------------"

check_service() {
    local service=$1
    local port=$2
    if docker ps | grep -q "$service.*Up"; then
        echo "✅ $service: Running"
        if [ -n "$port" ]; then
            if curl -s "http://localhost:$port" > /dev/null 2>&1; then
                echo "   └─ HTTP endpoint: ✅ Responsive"
            fi
        fi
    else
        echo "❌ $service: Not running"
        return 1
    fi
}

check_service "cdc-zookeeper" "2181"
check_service "cdc-kafka" ""
check_service "cdc-schema-registry" "8081"
check_service "cdc-kafka-connect" "8083"
check_service "cdc-postgres" ""
check_service "cdc-minio" "9000"
check_service "cdc-spark-master" "8082"
check_service "cdc-spark-worker-1" ""
check_service "cdc-spark-worker-2" ""
check_service "cdc-python-env" ""

echo ""
echo "======================================================================"
echo "✅ Infrastructure Setup Complete!"
echo "======================================================================"
echo ""
echo "📊 Service URLs:"
echo "  • Kafka UI:        http://localhost:8080"
echo "  • Schema Registry: http://localhost:8081"
echo "  • Kafka Connect:   http://localhost:8083"
echo "  • MinIO Console:   http://localhost:9001 (admin/admin123)"
echo "  • Spark Master:    http://localhost:8082"
echo "  • PostgreSQL:      localhost:5433"
echo ""
echo "📝 Next Steps:"
echo "  1. Deploy Debezium connector:"
echo "     ./scripts/deploy_debezium_connector.sh"
echo "  2. Verify CDC events flowing:"
echo "     docker exec -it cdc-kafka kafka-console-consumer \\"
echo "       --bootstrap-server localhost:9092 \\"
echo "       --topic cdc.public.transactions \\"
echo "       --from-beginning --max-messages 5"
echo ""
echo "======================================================================"