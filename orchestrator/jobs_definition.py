"""
Streaming Job Definitions

Defines all Spark streaming jobs and their dependencies.
"""

SPARK_SUBMIT_TEMPLATE = """
spark-submit \\
  --master {spark_master} \\
  --deploy-mode client \\
  --name {job_name} \\
  --packages io.delta:delta-spark_2.12:3.0.0,org.apache.hadoop:hadoop-aws:3.3.4,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \\
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \\
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \\
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \\
  --conf spark.hadoop.fs.s3a.access.key=admin \\
  --conf spark.hadoop.fs.s3a.secret.key=admin123 \\
  --conf spark.hadoop.fs.s3a.path.style.access=true \\
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \\
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \\
  --conf spark.cores.max=1 \\
  --driver-memory 1g \\
  --executor-memory 1g \\
  --executor-cores 1 \\
  {script_path}
"""

JOBS = {
    "bronze": {
        "name": "Bronze-CDC-Consumer",
        "script": "/opt/streaming-jobs/bronze_consumer.py",
        "description": "Consumes CDC events from Kafka into Bronze Delta layer",
        "depends_on": [],
        "restart_on_failure": True,
        "health_check": {
            "type": "delta_table",
            "path": "s3a://bronze/transactions_cdc",
            "min_records": 1
        }
    },
    "silver_transactions": {
        "name": "Silver-Transactions-MERGE",
        "script": "/opt/streaming-jobs/silver_transactions.py",
        "description": "Streaming MERGE from Bronze to Silver (transactions)",
        "depends_on": ["bronze"],
        "restart_on_failure": True,
        "health_check": {
            "type": "delta_table",
            "path": "s3a://silver/transactions",
            "min_records": 1
        }
    },
    "silver_customers": {
        "name": "Silver-Customers-SCD2",
        "script": "/opt/streaming-jobs/silver_customers.py",
        "description": "SCD Type 2 for customers (Bronze to Silver)",
        "depends_on": ["bronze"],
        "restart_on_failure": True,
        "health_check": {
            "type": "delta_table",
            "path": "s3a://silver/customers",
            "min_records": 1
        }
    },
    "gold_aml": {
        "name": "Gold-AML-Screening",
        "script": "/opt/streaming-jobs/gold_aml_streaming.py",
        "description": "Real-time AML screening and alert generation",
        "depends_on": ["silver_transactions"],
        "restart_on_failure": True,
        "health_check": {
            "type": "delta_table",
            "path": "s3a://gold/aml_alerts",
            "min_records": 0  # May have no alerts initially
        }
    }
}

DEBEZIUM_CONNECTOR = {
    "name": "banking-postgres-connector",
    "config_path": "/app/config/debezium-connector.json",
    "health_check": {
        "url": "http://kafka-connect:8083/connectors/banking-postgres-connector/status",
        "expected_state": "RUNNING"
    }
}