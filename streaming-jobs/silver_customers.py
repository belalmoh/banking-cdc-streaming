#!/usr/bin/env python3
"""
Silver Layer - Customers (SCD Type 2)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, current_timestamp, to_date, trim, upper
)
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
import logging

from config import Config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class SilverCustomersSCD2:
    """Streaming SCD Type 2 for customers Silver layer"""
    
    def __init__(self):
        self.config = Config()
        
        builder = SparkSession.builder.appName("Silver-Customers-SCD2")
        for key, value in self.config.get_spark_configs().items():
            builder = builder.config(key, value)
        
        self.spark = configure_spark_with_delta_pip(builder).getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")
        
        self.bronze_path = f"{self.config.BRONZE_PATH}/customers_cdc"
        self.silver_path = f"{self.config.SILVER_PATH}/customers"
        self.checkpoint_path = f"{self.config.CHECKPOINT_PATH}/silver-customers"
        
        logger.info("✅ Spark session initialized for Silver Customers (SCD2)")
    
    def initialize_silver_table(self):
        """Create Silver SCD2 table if doesn't exist"""
        try:
            DeltaTable.forPath(self.spark, self.silver_path)
            logger.info(f"✅ Silver SCD2 table exists")
        except Exception:
            logger.info(f"📝 Creating Silver SCD2 table")
            
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
        """Transform Bronze to Silver SCD2 format"""
        
        silver_df = bronze_df.select(
            col("customer_id").alias("customer_id"),
            trim(col("first_name")).alias("first_name"),
            trim(col("last_name")).alias("last_name"),
            trim(col("email")).alias("email"),
            trim(col("phone")).alias("phone"),
            to_date(col("date_of_birth")).alias("date_of_birth"),
            upper(trim(col("nationality"))).alias("nationality"),
            col("risk_score").cast("int").alias("risk_score"),
            upper(trim(col("kyc_status"))).alias("kyc_status"),
            col("onboarding_date").cast("timestamp").alias("onboarding_date"),
            upper(trim(col("customer_segment"))).alias("customer_segment"),
            upper(trim(col("country_residence"))).alias("country_residence"),
            col("cdc_operation"),
            col("_ingestion_timestamp")
        )
        
        # SCD2 fields
        silver_df = silver_df \
            .withColumn("effective_from", current_timestamp()) \
            .withColumn("effective_to", lit(None).cast("timestamp")) \
            .withColumn("is_current", lit(True)) \
            .withColumn("_created_at", current_timestamp()) \
            .withColumn("_updated_at", current_timestamp()) \
            .withColumn("_silver_processed_at", current_timestamp())
        
        # Filter valid records
        silver_df = silver_df.filter(
            col("customer_id").isNotNull() &
            col("email").isNotNull()
        )
        
        return silver_df
    
    def merge_scd2(self, batch_df, batch_id):
        """SCD Type 2 MERGE logic"""
        
        if batch_df.count() == 0:
            logger.info(f"Batch {batch_id}: Empty, skipping")
            return
        
        logger.info(f"Batch {batch_id}: Processing {batch_df.count()} customer changes")
        
        silver_batch = self.transform_bronze_to_silver(batch_df)
        
        if silver_batch.count() == 0:
            logger.info(f"Batch {batch_id}: No valid records")
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
        
        self.initialize_silver_table()
        
        bronze_stream = self.spark.readStream.format("delta").load(self.bronze_path)
        
        query = bronze_stream.writeStream \
            .foreachBatch(self.merge_scd2) \
            .outputMode("update") \
            .option("checkpointLocation", self.checkpoint_path) \
            .trigger(processingTime="15 seconds") \
            .start()
        
        logger.info("✅ SCD2 streaming started")
        logger.info("⏹️  Press Ctrl+C to stop\n")
        
        try:
            query.awaitTermination()
        except KeyboardInterrupt:
            logger.info("\n🛑 Stopping...")
            query.stop()


def main():
    processor = SilverCustomersSCD2()
    processor.start_streaming()


if __name__ == "__main__":
    main()