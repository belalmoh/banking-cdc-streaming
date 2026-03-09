#!/usr/bin/env python3
"""
Bronze Layer CDC Consumer - Debezium Format

Reads CDC events from Kafka (Debezium format) and writes to Delta Lake Bronze layer.

Usage:
    spark-submit --packages ... bronze_consumer.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, current_timestamp, lit, to_date, concat_ws, sha2
)
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
import logging

from config import Config
from schemas import DebeziumSchemas, DeltaSchemas

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BronzeCDCConsumer:
    """Consumes Debezium CDC events from Kafka into Bronze Delta layer"""
    
    def __init__(self):
        self.config = Config()
        
        # Initialize Spark with Delta Lake
        builder = SparkSession.builder \
            .appName("Bronze-CDC-Consumer-Debezium") \
            .config("spark.cores.max", "2") \
            .config("spark.executor.cores", "1")
        
        # Apply Spark configs
        for key, value in self.config.get_spark_configs().items():
            builder = builder.config(key, value)
        
        self.spark = configure_spark_with_delta_pip(builder).getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")
        
        logger.info("✅ Spark session initialized for Bronze CDC Consumer")
        logger.info(f"📡 Kafka: {self.config.KAFKA_BOOTSTRAP_SERVERS}")
        logger.info(f"💾 Bronze path: {self.config.BRONZE_PATH}")
    
    def initialize_bronze_table(self, entity: str):
        """Create Bronze table if it doesn't exist"""
        bronze_path = f"{self.config.BRONZE_PATH}/{entity}_cdc"
        
        try:
            DeltaTable.forPath(self.spark, bronze_path)
            logger.info(f"✅ Bronze table exists: {bronze_path}")
        except Exception:
            logger.info(f"📝 Creating Bronze table: {bronze_path}")
            
            # Get schema based on entity
            if entity == 'transactions':
                schema = DeltaSchemas.get_bronze_transactions()
            else:
                # For now, use transactions schema as template
                # You can create specific schemas for customers/accounts
                schema = DeltaSchemas.get_bronze_transactions()
            
            empty_df = self.spark.createDataFrame([], schema)
            empty_df.write.format("delta").partitionBy("_ingestion_date").save(bronze_path)
            logger.info(f"✅ Bronze table created: {bronze_path}")
    
    def read_kafka_stream(self, topic: str):
        """Read streaming data from Kafka topic"""
        logger.info(f"📖 Reading from Kafka topic: {topic}")
        
        df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.config.KAFKA_BOOTSTRAP_SERVERS) \
            .option("subscribe", topic) \
            .option("startingOffsets", "earliest") \
            .option("maxOffsetsPerTrigger", 1000) \
            .option("failOnDataLoss", "false") \
            .load()
        
        return df
    
    def parse_debezium_event(self, df, entity: str):
        """
        Parse Debezium CDC event — UNWRAPPED format (ExtractNewRecordState SMT).

        Because debezium-connector.json uses ExtractNewRecordState with
        add.fields=op,source.ts_ms,source.db,source.table, each Kafka message
        is already flat. Example:
        {
          "transaction_id": "TXN123", "amount": 1939.52, ...,
          "__op": "c",
          "__source_ts_ms": 1234567890,
          "__source_db": "banking_core",
          "__source_table": "transactions",
          "__deleted": "false"
        }
        """
        if entity == 'transactions':
            payload_schema = DebeziumSchemas.get_transaction_payload()
        elif entity == 'customers':
            payload_schema = DebeziumSchemas.get_customer_payload()
        elif entity == 'accounts':
            payload_schema = DebeziumSchemas.get_account_payload()
        else:
            raise ValueError(f"Unknown entity: {entity}")

        # Parse the flat JSON value directly
        parsed_df = df.select(
            from_json(col("value").cast("string"), payload_schema).alias("data"),
            col("offset").alias("kafka_offset"),
            col("partition").alias("kafka_partition"),
            col("timestamp").alias("kafka_timestamp"),
        )

        # Flatten payload + remap Debezium metadata fields
        final_df = parsed_df.select(
            "data.*",
            col("kafka_offset"),
            col("kafka_partition"),
            col("kafka_timestamp"),
        )

        # Rename Debezium __ prefix fields and add audit columns
        final_df = final_df \
            .withColumnRenamed("__op", "cdc_operation") \
            .withColumnRenamed("__source_ts_ms", "cdc_timestamp_ms") \
            .withColumnRenamed("__source_db", "source_database") \
            .withColumnRenamed("__source_table", "source_table") \
            .withColumn("_ingestion_timestamp", current_timestamp()) \
            .withColumn("_entity_type", lit(entity)) \
            .withColumn("_data_classification", lit("CONFIDENTIAL")) \
            .withColumn("_data_residency", lit("UAE")) \
            .withColumn("_ingestion_date", to_date(current_timestamp()))

        # Drop internal Debezium markers not needed in Bronze
        for drop_col in ["__deleted", "kafka_offset", "kafka_partition", "kafka_timestamp"]:
            if drop_col in [f.name for f in final_df.schema.fields]:
                final_df = final_df.drop(drop_col)

        # Filter null primary keys (malformed/tombstone messages)
        id_col = f"{entity[:-1]}_id"   # transactions->transaction_id, etc.
        final_df = final_df.filter(col(id_col).isNotNull())

        return final_df

    def write_to_bronze(self, df, entity: str):
        """Write streaming DataFrame to Bronze layer"""
        bronze_path = f"{self.config.BRONZE_PATH}/{entity}_cdc"
        checkpoint_path = f"{self.config.CHECKPOINT_PATH}/bronze-{entity}"
        
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
        """Start all Bronze streaming consumers"""
        logger.info("=" * 70)
        logger.info("🚀 Starting Bronze CDC Consumers (Debezium)")
        logger.info("=" * 70)
        
        queries = []
        
        # 1. Transactions
        logger.info("\n📊 Starting Transactions consumer...")
        self.initialize_bronze_table("transactions")
        
        txn_stream = self.read_kafka_stream(self.config.DEBEZIUM_TOPICS['transactions'])
        txn_parsed = self.parse_debezium_event(txn_stream, "transactions")
        txn_query = self.write_to_bronze(txn_parsed, "transactions")
        queries.append(txn_query)
        
        # 2. Customers
        logger.info("\n👥 Starting Customers consumer...")
        self.initialize_bronze_table("customers")
        
        cust_stream = self.read_kafka_stream(self.config.DEBEZIUM_TOPICS['customers'])
        cust_parsed = self.parse_debezium_event(cust_stream, "customers")
        cust_query = self.write_to_bronze(cust_parsed, "customers")
        queries.append(cust_query)
        
        # 3. Accounts
        logger.info("\n💳 Starting Accounts consumer...")
        self.initialize_bronze_table("accounts")
        
        acc_stream = self.read_kafka_stream(self.config.DEBEZIUM_TOPICS['accounts'])
        acc_parsed = self.parse_debezium_event(acc_stream, "accounts")
        acc_query = self.write_to_bronze(acc_parsed, "accounts")
        queries.append(acc_query)
        
        logger.info("\n" + "=" * 70)
        logger.info("✅ All Bronze CDC consumers started!")
        logger.info("=" * 70)
        logger.info("\n📊 Configuration:")
        logger.info(f"   • Trigger interval: 10 seconds")
        logger.info(f"   • Max offsets per trigger: 1000")
        logger.info(f"   • Output mode: Append")
        logger.info(f"   • Partitioning: _ingestion_date")
        logger.info("\n🔍 Monitor:")
        logger.info("   • Spark UI: http://localhost:4040")
        logger.info("   • Kafka UI: http://localhost:8080")
        logger.info("\n⏹️  Press Ctrl+C to stop")
        logger.info("=" * 70 + "\n")
        
        # Wait for termination
        try:
            for query in queries:
                query.awaitTermination()
        except KeyboardInterrupt:
            logger.info("\n🛑 Stopping all Bronze consumers...")
            for query in queries:
                query.stop()
            logger.info("✅ All consumers stopped gracefully")


def main():
    """Main entry point"""
    consumer = BronzeCDCConsumer()
    consumer.start_streaming()


if __name__ == "__main__":
    main()