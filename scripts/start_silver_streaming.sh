#!/bin/bash
# Start all Silver layer streaming jobs

set -e

echo "======================================================================"
echo "Starting Silver Layer Streaming Jobs"
echo "======================================================================"

# Start transactions MERGE in background
echo "🚀 Starting Transactions MERGE..."
docker exec -d cdc-spark-master spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --name "Silver-Transactions-Merge" \
    --packages io.delta:delta-spark_2.12:3.0.0,org.apache.hadoop:hadoop-aws:3.3.4 \
    --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
    --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
    --conf "spark.cores.max=1" \
    --driver-memory 512m \
    --executor-memory 512m \
    --executor-cores 1 \
    /opt/spark-jobs/silver_transactions_merge.py

echo "✅ Transactions job submitted"
sleep 5

# Start customers SCD2 in background
echo "🚀 Starting Customers SCD2..."
docker exec -d cdc-spark-master spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --name "Silver-Customers-SCD2" \
    --packages io.delta:delta-spark_2.12:3.0.0,org.apache.hadoop:hadoop-aws:3.3.4 \
    --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
    --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
    --conf "spark.cores.max=1" \
    --driver-memory 512m \
    --executor-memory 512m \
    --executor-cores 1 \
    /opt/spark-jobs/silver_customers_scd2.py

echo "✅ Customers SCD2 job submitted"

echo ""
echo "======================================================================"
echo "✅ All Silver streaming jobs started!"
echo ""
echo "Monitor at: http://localhost:4040"
echo ""
echo "To stop jobs:"
echo "  docker exec cdc-spark-master pkill -f silver"
echo "======================================================================"