#!/usr/bin/env python3
"""
Bronze Layer CDC Consumer - Spark Structured Streaming

Reads CDC events from Kafka topics and writes to Delta Lake Bronze layer.
Preserves full CDC event history with audit trails.

Usage:
    spark-submit --master spark://spark-master:7077 \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-core_2.12:2.4.0 \
        bronze_cdc_consumer.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_json, struct, current_timestamp, 
    lit, sha2, concat_ws, to_date
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    BooleanType, TimestampType, LongType
)
from delta import configure_spark_with_delta_pip
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BronzeCDCConsumer:
    """Consumes CDC events from Kafka and writes to Bronze layer"""
    
    def __init__(self, 
                 kafka_bootstrap_servers: str = "kafka:9093",
                 checkpoint_location: str = "s3a://checkpoints/bronze-cdc",
                 minio_endpoint: str = "http://minio:9000"):
        
        self.kafka_servers = kafka_bootstrap_servers
        self.checkpoint_location = checkpoint_location
        
        # Initialize Spark with Delta Lake
        builder = SparkSession.builder \
            .appName("Banking-CDC-Bronze-Consumer") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
            .config("spark.hadoop.fs.s3a.access.key", "admin") \
            .config("spark.hadoop.fs.s3a.secret.key", "admin123") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .config("spark.sql.streaming.checkpointLocation", checkpoint_location)
        
        self.spark = configure_spark_with_delta_pip(builder).getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")
        
        logger.info("✅ Spark session initialized with Delta Lake")
        logger.info(f"📡 Kafka servers: {kafka_bootstrap_servers}")
        logger.info(f"💾 Checkpoint location: {checkpoint_location}")
    
    def get_transaction_schema(self):
        """Define schema for transaction CDC events"""
        return StructType([
            StructField("op", StringType(), True),
            StructField("source", StructType([
                StructField("table", StringType(), True),
                StructField("db", StringType(), True),
                StructField("ts_ms", LongType(), True)
            ]), True),
            StructField("ts_ms", LongType(), True),
            StructField("before", StructType([
                StructField("transaction_id", StringType(), True),
                StructField("account_id", StringType(), True),
                StructField("transaction_type", StringType(), True),
                StructField("amount", DoubleType(), True),
                StructField("currency", StringType(), True),
                StructField("merchant_name", StringType(), True),
                StructField("merchant_category", StringType(), True),
                StructField("transaction_timestamp", StringType(), True),
                StructField("country", StringType(), True),
                StructField("is_international", BooleanType(), True),
                StructField("status", StringType(), True)
            ]), True),
            StructField("after", StructType([
                StructField("transaction_id", StringType(), True),
                StructField("account_id", StringType(), True),
                StructField("transaction_type", StringType(), True),
                StructField("amount", DoubleType(), True),
                StructField("currency", StringType(), True),
                StructField("merchant_name", StringType(), True),
                StructField("merchant_category", StringType(), True),
                StructField("transaction_timestamp", StringType(), True),
                StructField("country", StringType(), True),
                StructField("is_international", BooleanType(), True),
                StructField("status", StringType(), True)
            ]), True)
        ])
    
    def get_customer_schema(self):
        """Define schema for customer CDC events"""
        return StructType([
            StructField("op", StringType(), True),
            StructField("source", StructType([
                StructField("table", StringType(), True),
                StructField("db", StringType(), True),
                StructField("ts_ms", LongType(), True)
            ]), True),
            StructField("ts_ms", LongType(), True),
            StructField("before", StructType([
                StructField("customer_id", StringType(), True),
                StructField("first_name", StringType(), True),
                StructField("last_name", StringType(), True),
                StructField("email", StringType(), True),
                StructField("phone", StringType(), True),
                StructField("date_of_birth", StringType(), True),
                StructField("nationality", StringType(), True),
                StructField("risk_score", LongType(), True),
                StructField("kyc_status", StringType(), True),
                StructField("onboarding_date", StringType(), True),
                StructField("customer_segment", StringType(), True),
                StructField("country_residence", StringType(), True)
            ]), True),
            StructField("after", StructType([
                StructField("customer_id", StringType(), True),
                StructField("first_name", StringType(), True),
                StructField("last_name", StringType(), True),
                StructField("email", StringType(), True),
                StructField("phone", StringType(), True),
                StructField("date_of_birth", StringType(), True),
                StructField("nationality", StringType(), True),
                StructField("risk_score", LongType(), True),
                StructField("kyc_status", StringType(), True),
                StructField("onboarding_date", StringType(), True),
                StructField("customer_segment", StringType(), True),
                StructField("country_residence", StringType(), True)
            ]), True)
        ])
    
    def get_account_schema(self):
        """Define schema for account CDC events"""
        return StructType([
            StructField("op", StringType(), True),
            StructField("source", StructType([
                StructField("table", StringType(), True),
                StructField("db", StringType(), True),
                StructField("ts_ms", LongType(), True)
            ]), True),
            StructField("ts_ms", LongType(), True),
            StructField("before", StructType([
                StructField("account_id", StringType(), True),
                StructField("customer_id", StringType(), True),
                StructField("account_type", StringType(), True),
                StructField("currency", StringType(), True),
                StructField("balance", DoubleType(), True),
                StructField("status", StringType(), True),
                StructField("opened_date", StringType(), True),
                StructField("last_activity_date", StringType(), True)
            ]), True),
            StructField("after", StructType([
                StructField("account_id", StringType(), True),
                StructField("customer_id", StringType(), True),
                StructField("account_type", StringType(), True),
                StructField("currency", StringType(), True),
                StructField("balance", DoubleType(), True),
                StructField("status", StringType(), True),
                StructField("opened_date", StringType(), True),
                StructField("last_activity_date", StringType(), True)
            ]), True)
        ])
    
    def read_kafka_stream(self, topic: str):
        """
        Read streaming data from Kafka topic
        
        Args:
            topic: Kafka topic name
            
        Returns:
            Streaming DataFrame
        """
        logger.info(f"📖 Reading from Kafka topic: {topic}")
        
        df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_servers) \
            .option("subscribe", topic) \
            .option("startingOffsets", "earliest") \
            .option("maxOffsetsPerTrigger", 1000) \
            .option("failOnDataLoss", "false") \
            .load()
        
        return df
    
    def parse_cdc_event(self, df, schema, entity_name: str):
        """
        Parse CDC event JSON and add audit columns
        
        Args:
            df: Raw Kafka DataFrame
            schema: CDC event schema
            entity_name: Entity name (transactions, customers, accounts)
            
        Returns:
            Parsed DataFrame with audit columns
        """
        # Parse JSON from Kafka value
        parsed_df = df.select(
            col("key").cast("string").alias("kafka_key"),
            col("topic").alias("kafka_topic"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            col("timestamp").alias("kafka_timestamp"),
            from_json(col("value").cast("string"), schema).alias("cdc_event")
        )
        
        # Extract CDC fields
        result_df = parsed_df.select(
            col("kafka_key"),
            col("kafka_topic"),
            col("kafka_partition"),
            col("kafka_offset"),
            col("kafka_timestamp"),
            col("cdc_event.op").alias("cdc_operation"),
            col("cdc_event.source.table").alias("source_table"),
            col("cdc_event.source.db").alias("source_database"),
            col("cdc_event.ts_ms").alias("cdc_timestamp_ms"),
            col("cdc_event.before").alias("before_state"),
            col("cdc_event.after").alias("after_state")
        )
        
        # Add audit columns
        result_df = result_df \
            .withColumn("_ingestion_timestamp", current_timestamp()) \
            .withColumn("_entity_type", lit(entity_name)) \
            .withColumn("_data_classification", lit("CONFIDENTIAL")) \
            .withColumn("_data_residency", lit("UAE")) \
            .withColumn("_ingestion_date", to_date(current_timestamp()))
        
        # Add record hash for deduplication
        result_df = result_df.withColumn(
            "_record_hash",
            sha2(
                concat_ws("|", 
                    col("kafka_topic"),
                    col("kafka_partition"),
                    col("kafka_offset")
                ),
                256
            )
        )
        
        return result_df
    
    def write_to_bronze(self, df, entity_name: str, checkpoint_suffix: str):
        """
        Write streaming DataFrame to Bronze layer (Delta Lake)
        
        Args:
            df: Streaming DataFrame
            entity_name: Entity name for path
            checkpoint_suffix: Unique checkpoint identifier
        """
        bronze_path = f"s3a://bronze/{entity_name}_cdc"
        checkpoint_path = f"{self.checkpoint_location}/{entity_name}_{checkpoint_suffix}"
        
        logger.info(f"💾 Writing to Bronze: {bronze_path}")
        logger.info(f"🔖 Checkpoint: {checkpoint_path}")
        
        query = df.writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", checkpoint_path) \
            .partitionBy("_ingestion_date") \
            .trigger(processingTime="10 seconds") \
            .start(bronze_path)
        
        return query
    
    def start_streaming(self):
        """Start all streaming queries"""
        logger.info("=" * 70)
        logger.info("🚀 Starting Bronze CDC Streaming Consumers")
        logger.info("=" * 70)
        
        queries = []
        
        # 1. Transactions stream
        logger.info("\n📊 Starting Transactions consumer...")
        txn_stream = self.read_kafka_stream("banking.transactions")
        txn_parsed = self.parse_cdc_event(
            txn_stream, 
            self.get_transaction_schema(), 
            "transactions"
        )
        txn_query = self.write_to_bronze(txn_parsed, "transactions", "v1")
        queries.append(txn_query)
        
        # 2. Customers stream
        logger.info("\n👥 Starting Customers consumer...")
        cust_stream = self.read_kafka_stream("banking.customers")
        cust_parsed = self.parse_cdc_event(
            cust_stream,
            self.get_customer_schema(),
            "customers"
        )
        cust_query = self.write_to_bronze(cust_parsed, "customers", "v1")
        queries.append(cust_query)
        
        # 3. Accounts stream
        logger.info("\n💳 Starting Accounts consumer...")
        acc_stream = self.read_kafka_stream("banking.accounts")
        acc_parsed = self.parse_cdc_event(
            acc_stream,
            self.get_account_schema(),
            "accounts"
        )
        acc_query = self.write_to_bronze(acc_parsed, "accounts", "v1")
        queries.append(acc_query)
        
        logger.info("\n" + "=" * 70)
        logger.info("✅ All streaming queries started!")
        logger.info("=" * 70)
        logger.info("\n📊 Streaming Metrics:")
        logger.info("   • Trigger interval: 10 seconds")
        logger.info("   • Max offsets per trigger: 1000")
        logger.info("   • Output mode: Append")
        logger.info("   • Partitioning: _ingestion_date")
        logger.info("\n🔍 Monitor progress:")
        logger.info("   • Spark UI: http://localhost:4040")
        logger.info("   • MinIO Console: http://localhost:9001")
        logger.info("\n⏹️  Press Ctrl+C to stop all queries")
        logger.info("=" * 70 + "\n")
        
        # Wait for termination
        try:
            for query in queries:
                query.awaitTermination()
        except KeyboardInterrupt:
            logger.info("\n🛑 Stopping all streaming queries...")
            for query in queries:
                query.stop()
            logger.info("✅ All queries stopped gracefully")


def main():
    """Main entry point"""
    consumer = BronzeCDCConsumer(
        kafka_bootstrap_servers="kafka:9093",
        checkpoint_location="s3a://checkpoints/bronze-cdc",
        minio_endpoint="http://minio:9000"
    )
    
    consumer.start_streaming()


if __name__ == "__main__":
    main()