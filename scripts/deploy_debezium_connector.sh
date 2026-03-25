#!/bin/bash
# Deploy Debezium PostgreSQL connector

set -e

CONNECT_URL="http://localhost:8083"
CONNECTOR_CONFIG="./config/debezium-connector.json"

echo "======================================================================"
echo "Deploying Debezium PostgreSQL Connector"
echo "======================================================================"

# Wait for Kafka Connect to be ready
echo "⏳ Waiting for Kafka Connect..."
until curl -s "$CONNECT_URL/" > /dev/null; do
    echo "   Kafka Connect not ready yet. Retrying in 5 seconds..."
    sleep 5
done

echo "✅ Kafka Connect is ready!"

# Check if connector already exists
CONNECTOR_NAME="banking-postgres-connector"
if curl -s "$CONNECT_URL/connectors/$CONNECTOR_NAME" | grep -q "error_code"; then
    echo "📝 Creating new connector: $CONNECTOR_NAME"
    
    curl -X POST "$CONNECT_URL/connectors" \
        -H "Content-Type: application/json" \
        -d @"$CONNECTOR_CONFIG"
    
    echo -e "\n✅ Connector created successfully!"
else
    echo "🔄 Connector already exists. Updating configuration..."
    
    curl -X PUT "$CONNECT_URL/connectors/$CONNECTOR_NAME/config" \
        -H "Content-Type: application/json" \
        -d "$(cat $CONNECTOR_CONFIG | jq '.config')"
    
    echo -e "\n✅ Connector updated successfully!"
fi

# Wait for connector to start
echo ""
echo "⏳ Waiting for connector to start..."
sleep 10

# Check connector status
echo ""
echo "📊 Connector Status:"
echo "----------------------------------------------------------------------"
curl -s "$CONNECT_URL/connectors/$CONNECTOR_NAME/status" | jq '.'

# List topics created by Debezium
echo ""
echo "📋 Debezium Topics Created:"
echo "----------------------------------------------------------------------"
docker exec -it cdc-kafka kafka-topics --list \
    --bootstrap-server localhost:9092 | grep "^cdc\."

echo ""
echo "======================================================================"
echo "✅ Debezium Connector Deployed!"
echo ""
echo "🔍 Verify CDC events:"
echo "  docker exec -it cdc-kafka kafka-console-consumer \\"
echo "    --bootstrap-server localhost:9092 \\"
echo "    --topic cdc.public.transactions \\"
echo "    --from-beginning --max-messages 3"
echo ""
echo "📊 Monitor connector:"
echo "  curl http://localhost:8083/connectors/banking-postgres-connector/status | jq '.'"
echo "======================================================================"