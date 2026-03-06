#!/bin/bash
# Start Bronze CDC streaming consumer on Spark cluster

set -e

SPARK_MASTER="spark://spark-master:7077"
JOB_FILE="/opt/spark-jobs/bronze_cdc_consumer.py"

echo "======================================================================"
echo "Starting Bronze CDC Streaming Consumer"
echo "======================================================================"

# Check if Spark master is accessible
echo "🔍 Checking Spark master: $SPARK_MASTER"

# Submit Spark Structured Streaming job
docker exec -it cdc-spark-master spark-submit \
    --master $SPARK_MASTER \
    --deploy-mode client \
    --name "Banking-CDC-Bronze-Consumer" \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.0.0,org.apache.hadoop:hadoop-aws:3.3.4 \
    --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
    --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
    --conf "spark.hadoop.fs.s3a.endpoint=http://minio:9000" \
    --conf "spark.hadoop.fs.s3a.access.key=admin" \
    --conf "spark.hadoop.fs.s3a.secret.key=admin123" \
    --conf "spark.hadoop.fs.s3a.path.style.access=true" \
    --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
    --conf "spark.hadoop.fs.s3a.connection.ssl.enabled=false" \
    --conf "spark.cores.max=1" \
    --driver-memory 512m \
    --executor-memory 512m \
    --executor-cores 1 \
    $JOB_FILE

echo "======================================================================"
echo "✅ Streaming job submitted!"
echo "======================================================================"