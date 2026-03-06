#!/bin/bash
# Start Silver Transactions MERGE job

echo "======================================================================"
echo "Starting Silver Transactions Streaming MERGE"
echo "======================================================================"

docker exec -it cdc-spark-master spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --name "Silver-Transactions-Merge" \
    --packages io.delta:delta-spark_2.12:3.0.0,org.apache.hadoop:hadoop-aws:3.3.4 \
    --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
    --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
    --driver-memory 1g \
    --executor-memory 1g \
    /opt/spark-jobs/silver_transactions_merge.py

echo "======================================================================"
echo "Streaming job finished"
echo "======================================================================"
