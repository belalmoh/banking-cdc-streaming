#!/usr/bin/env python3
"""
Silver Layer - Transactions (Streaming MERGE)

Reads CDC events from Bronze and maintains current state in Silver.
Handles INSERT/UPDATE/DELETE operations.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, current_timestamp, to_timestamp,
    coalesce, trim, upper, round as spark_round
)
from pyspark.sql.types import DecimalType
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SilverTransactionsMerge:
    """Streaming MERGE for transactions Silver layer"""
    
    def __init__(self,
                 bronze_path: str = "s3a://bronze/transactions_cdc",
                 silver_path: str = "s3a://silver/transactions",
                 checkpoint_location: str = "s3a://checkpoints/silver-transactions",
                 minio_endpoint: str = "http://minio:9000"):
        
        self.bronze_path = bronze_path
        self.silver_path = silver_path
        self.checkpoint_location = checkpoint_location
        
        # Initialize Spark
        builder = SparkSession.builder \
            .appName("Silver-Transactions-Merge") \
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
        
        logger.info("✅ Spark session initialized for Silver Transactions")
    
    def initialize_silver_table(self):
        """Create Silver table if it doesn't exist"""
        try:
            DeltaTable.forPath(self.spark, self.silver_path)
            logger.info(f"✅ Silver table exists: {self.silver_path}")
        except Exception:
            logger.info(f"📝 Creating Silver table: {self.silver_path}")
            
            from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType, BooleanType
            
            schema = StructType([
                StructField("transaction_id", StringType(), False),
                StructField("account_id", StringType(), True),
                StructField("transaction_type", StringType(), True),
                StructField("amount", DecimalType(18,2), True),
                StructField("currency", StringType(), True),
                StructField("merchant_name", StringType(), True),
                StructField("merchant_category", StringType(), True),
                StructField("transaction_timestamp", TimestampType(), True),
                StructField("country", StringType(), True),
                StructField("is_international", BooleanType(), True),
                StructField("status", StringType(), True),
                StructField("_created_at", TimestampType(), True),
                StructField("_updated_at", TimestampType(), True),
                StructField("_is_deleted", BooleanType(), True),
                StructField("_silver_processed_at", TimestampType(), True)
            ])
            
            empty_df = self.spark.createDataFrame([], schema)
            empty_df.write.format("delta").save(self.silver_path)
            logger.info("✅ Silver table created")
    
    def transform_bronze_to_silver(self, bronze_df):
        """Transform Bronze CDC events to Silver format"""
        
        silver_df = bronze_df.select(
            col("after_state.transaction_id").alias("transaction_id"),
            col("after_state.account_id").alias("account_id"),
            upper(trim(col("after_state.transaction_type"))).alias("transaction_type"),
            spark_round(col("after_state.amount").cast(DecimalType(18, 2)), 2).alias("amount"),
            upper(trim(col("after_state.currency"))).alias("currency"),
            trim(col("after_state.merchant_name")).alias("merchant_name"),
            upper(trim(col("after_state.merchant_category"))).alias("merchant_category"),
            to_timestamp(col("after_state.transaction_timestamp")).alias("transaction_timestamp"),
            upper(trim(col("after_state.country"))).alias("country"),
            col("after_state.is_international").alias("is_international"),
            upper(trim(col("after_state.status"))).alias("status"),
            col("cdc_operation"),
            col("_ingestion_timestamp")
        )
        
        silver_df = silver_df \
            .withColumn("_created_at", current_timestamp()) \
            .withColumn("_updated_at", current_timestamp()) \
            .withColumn("_is_deleted", 
                when(col("cdc_operation") == "d", lit(True)).otherwise(lit(False))
            ) \
            .withColumn("_silver_processed_at", current_timestamp())
        
        # Data quality filters
        silver_df = silver_df.filter(
            col("transaction_id").isNotNull() &
            col("amount").isNotNull() & 
            (col("amount") >= 0) &
            col("transaction_type").isin("DEBIT", "CREDIT")
        )
        
        return silver_df
    
    def merge_to_silver(self, batch_df, batch_id):
        """Merge batch into Silver table using Delta MERGE"""
        
        if batch_df.count() == 0:
            logger.info(f"Batch {batch_id}: Empty batch, skipping")
            return
        
        logger.info(f"Batch {batch_id}: Processing {batch_df.count()} records")
        
        silver_batch = self.transform_bronze_to_silver(batch_df)
        
        if silver_batch.count() == 0:
            logger.info(f"Batch {batch_id}: No valid records after transformation")
            return
        
        silver_table = DeltaTable.forPath(self.spark, self.silver_path)
        
        # MERGE logic
        silver_table.alias("target").merge(
            silver_batch.alias("source"),
            "target.transaction_id = source.transaction_id"
        ).whenMatchedUpdate(
            condition="source.cdc_operation IN ('u', 'd')",
            set={
                "account_id": "source.account_id",
                "transaction_type": "source.transaction_type",
                "amount": "source.amount",
                "currency": "source.currency",
                "merchant_name": "source.merchant_name",
                "merchant_category": "source.merchant_category",
                "transaction_timestamp": "source.transaction_timestamp",
                "country": "source.country",
                "is_international": "source.is_international",
                "status": "source.status",
                "_updated_at": "source._updated_at",
                "_is_deleted": "source._is_deleted",
                "_silver_processed_at": "source._silver_processed_at"
            }
        ).whenNotMatchedInsert(
            condition="source.cdc_operation = 'c'",
            values={
                "transaction_id": "source.transaction_id",
                "account_id": "source.account_id",
                "transaction_type": "source.transaction_type",
                "amount": "source.amount",
                "currency": "source.currency",
                "merchant_name": "source.merchant_name",
                "merchant_category": "source.merchant_category",
                "transaction_timestamp": "source.transaction_timestamp",
                "country": "source.country",
                "is_international": "source.is_international",
                "status": "source.status",
                "_created_at": "source._created_at",
                "_updated_at": "source._updated_at",
                "_is_deleted": "source._is_deleted",
                "_silver_processed_at": "source._silver_processed_at"
            }
        ).execute()
        
        logger.info(f"Batch {batch_id}: ✅ MERGE completed successfully")
    
    def start_streaming(self):
        """Start streaming MERGE from Bronze to Silver"""
        logger.info("=" * 70)
        logger.info("🚀 Starting Silver Transactions Streaming MERGE")
        logger.info("=" * 70)
        logger.info(f"📖 Bronze source: {self.bronze_path}")
        logger.info(f"💾 Silver target: {self.silver_path}")
        logger.info(f"🔖 Checkpoint: {self.checkpoint_location}")
        logger.info("=" * 70)
        
        self.initialize_silver_table()
        
        bronze_stream = self.spark.readStream \
            .format("delta") \
            .load(self.bronze_path)
        
        logger.info("✅ Reading from Bronze Delta table")
        
        query = bronze_stream.writeStream \
            .foreachBatch(self.merge_to_silver) \
            .outputMode("update") \
            .option("checkpointLocation", self.checkpoint_location) \
            .trigger(processingTime="15 seconds") \
            .start()
        
        logger.info("✅ Streaming MERGE query started")
        logger.info("\n🔍 Monitor at: http://localhost:4040")
        logger.info("⏹️  Press Ctrl+C to stop\n")
        
        try:
            query.awaitTermination()
        except KeyboardInterrupt:
            logger.info("\n🛑 Stopping streaming query...")
            query.stop()
            logger.info("✅ Query stopped gracefully")


def main():
    """Main entry point"""
    processor = SilverTransactionsMerge(
        bronze_path="s3a://bronze/transactions_cdc",
        silver_path="s3a://silver/transactions",
        checkpoint_location="s3a://checkpoints/silver-transactions",
        minio_endpoint="http://minio:9000"
    )
    
    processor.start_streaming()


if __name__ == "__main__":
    main()