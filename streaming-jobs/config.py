"""
Shared Configuration for Streaming Jobs
"""

import os

class Config:
    """Central configuration for all streaming jobs"""
    
    # Kafka
    KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9093')
    
    # MinIO (S3)
    MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'http://minio:9000')
    MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'admin')
    MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'admin123')
    
    # Spark
    SPARK_MASTER = os.getenv('SPARK_MASTER', 'spark://spark-master:7077')
    
    # Delta Lake Paths
    BRONZE_PATH = os.getenv('BRONZE_PATH', 's3a://bronze')
    SILVER_PATH = os.getenv('SILVER_PATH', 's3a://silver')
    GOLD_PATH = os.getenv('GOLD_PATH', 's3a://gold')
    CHECKPOINT_PATH = os.getenv('CHECKPOINT_PATH', 's3a://checkpoints')
    
    # Debezium Topics
    DEBEZIUM_TOPICS = {
        'transactions': 'cdc.public.transactions',
        'customers': 'cdc.public.customers',
        'accounts': 'cdc.public.accounts'
    }
    
    @staticmethod
    def get_spark_configs():
        """Get Spark configuration as dict"""
        return {
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.hadoop.fs.s3a.endpoint": Config.MINIO_ENDPOINT,
            "spark.hadoop.fs.s3a.access.key": Config.MINIO_ACCESS_KEY,
            "spark.hadoop.fs.s3a.secret.key": Config.MINIO_SECRET_KEY,
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
            "spark.cores.max": "1",
            "spark.executor.cores": "1"
        }