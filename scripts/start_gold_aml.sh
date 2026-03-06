#!/bin/bash
# Start Gold Layer AML Screening

echo "======================================================================"
echo "Starting Gold Layer - Real-Time AML Screening"
echo "======================================================================"

docker exec -it cdc-spark-master spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --name "Gold-AML-Screening" \
    --packages io.delta:delta-spark_2.12:3.0.0,org.apache.hadoop:hadoop-aws:3.3.4 \
    --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
    --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
    --driver-memory 1g \
    --executor-memory 1g \
    --executor-cores 1 \
    /opt/spark-jobs/gold_aml_screening.py

echo "======================================================================"
echo "AML Screening job finished"
echo "======================================================================"