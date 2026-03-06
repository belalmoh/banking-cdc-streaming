#!/usr/bin/env python3
"""
Silver Layer - Customers (SCD Type 2 with History Tracking)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, current_timestamp, to_date,
    coalesce, trim, upper
)
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SilverCustomersSCD2:
    """Streaming SCD Type 2 for customers Silver layer"""
    
    def __init__(self,
                 bronze_path: str = "s3a://bronze/customers_cdc",
                 silver_path: str = "s3a://silver/customers",
                 checkpoint_location: str = "s3a://checkpoints/silver-customers",
                 minio_endpoint: str = "http://minio:9000"):
        
        self.bronze_path = bronze_path
        self.silver_path = silver_path
        self.checkpoint_location = checkpoint_location
        
        builder = SparkSession.builder \
            .appName("Silver-Customers-SCD2") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
            .config("spark.hadoop.fs.s3a.access.key", "admin") \
            .config("spark.hadoop.fs.s3a.secret.key", "admin123") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        
        self.spark = configure_spark_with_delta_pip(builder).getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")
        
        logger.info("✅ Spark session initialized for Silver Customers (SCD2)")
    
    def initialize_silver_table(self):
        """Create Silver SCD2 table if it doesn't exist"""
        try:
            DeltaTable.forPath(self.spark, self.silver_path)
            logger.info(f"✅ Silver SCD2 table exists: {self.silver_path}")
        except Exception:
            logger.info(f"📝 Creating Silver SCD2 table: {self.silver_path}")
            
            from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, TimestampType, BooleanType
            
            schema = StructType([
                StructField("customer_id", StringType(), False),
                StructField("first_name", StringType(), True),
                StructField("last_name", StringType(), True),
                StructField("email", StringType(), True),
                StructField("phone", StringType(), True),
                StructField("date_of_birth", DateType(), True),
                StructField("nationality", StringType(), True),
                StructField("risk_score", IntegerType(), True),
                StructField("kyc_status", StringType(), True),
                StructField("onboarding_date", TimestampType(), True),
                StructField("customer_segment", StringType(), True),
                StructField("country_residence", StringType(), True),
                StructField("effective_from", TimestampType(), True),
                StructField("effective_to", TimestampType(), True),
                StructField("is_current", BooleanType(), True),
                StructField("_created_at", TimestampType(), True),
                StructField("_updated_at", TimestampType(), True),
                StructField("_silver_processed_at", TimestampType(), True)
            ])
            
            empty_df = self.spark.createDataFrame([], schema)
            empty_df.write.format("delta").save(self.silver_path)
            logger.info("✅ Silver SCD2 table created")
    
    def transform_bronze_to_silver(self, bronze_df):
        """Transform Bronze CDC events to Silver SCD2 format"""
        
        silver_df = bronze_df.select(
            col("after_state.customer_id").alias("customer_id"),
            trim(col("after_state.first_name")).alias("first_name"),
            trim(col("after_state.last_name")).alias("last_name"),
            trim(col("after_state.email")).alias("email"),
            trim(col("after_state.phone")).alias("phone"),
            to_date(col("after_state.date_of_birth")).alias("date_of_birth"),
            upper(trim(col("after_state.nationality"))).alias("nationality"),
            col("after_state.risk_score").cast("int").alias("risk_score"),
            upper(trim(col("after_state.kyc_status"))).alias("kyc_status"),
            col("after_state.onboarding_date").cast("timestamp").alias("onboarding_date"),
            upper(trim(col("after_state.customer_segment"))).alias("customer_segment"),
            upper(trim(col("after_state.country_residence"))).alias("country_residence"),
            col("cdc_operation"),
            col("_ingestion_timestamp")
        )
        
        silver_df = silver_df \
            .withColumn("effective_from", current_timestamp()) \
            .withColumn("effective_to", lit(None).cast("timestamp")) \
            .withColumn("is_current", lit(True)) \
            .withColumn("_created_at", current_timestamp()) \
            .withColumn("_updated_at", current_timestamp()) \
            .withColumn("_silver_processed_at", current_timestamp())
        
        silver_df = silver_df.filter(
            col("customer_id").isNotNull() &
            col("email").isNotNull()
        )
        
        return silver_df
    
    def merge_scd2(self, batch_df, batch_id):
        """SCD Type 2 MERGE logic"""
        
        if batch_df.count() == 0:
            logger.info(f"Batch {batch_id}: Empty batch, skipping")
            return
        
        logger.info(f"Batch {batch_id}: Processing {batch_df.count()} customer changes")
        
        silver_batch = self.transform_bronze_to_silver(batch_df)
        
        if silver_batch.count() == 0:
            logger.info(f"Batch {batch_id}: No valid records after transformation")
            return
        
        silver_table = DeltaTable.forPath(self.spark, self.silver_path)
        
        # Close current records
        silver_table.alias("target").merge(
            silver_batch.alias("source"),
            "target.customer_id = source.customer_id AND target.is_current = true"
        ).whenMatchedUpdate(
            set={
                "effective_to": "source.effective_from",
                "is_current": "false",
                "_updated_at": "source._updated_at"
            }
        ).execute()
        
        # Insert new versions
        silver_batch.write.format("delta").mode("append").save(self.silver_path)
        
        logger.info(f"Batch {batch_id}: ✅ SCD2 MERGE completed")
    
    def start_streaming(self):
        """Start streaming SCD2 processing"""
        logger.info("=" * 70)
        logger.info("🚀 Starting Silver Customers SCD2 Streaming")
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
            .foreachBatch(self.merge_scd2) \
            .outputMode("update") \
            .option("checkpointLocation", self.checkpoint_location) \
            .trigger(processingTime="15 seconds") \
            .start()
        
        logger.info("✅ SCD2 streaming query started")
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
    processor = SilverCustomersSCD2(
        bronze_path="s3a://bronze/customers_cdc",
        silver_path="s3a://silver/customers",
        checkpoint_location="s3a://checkpoints/silver-customers",
        minio_endpoint="http://minio:9000"
    )
    
    processor.start_streaming()


if __name__ == "__main__":
    main()