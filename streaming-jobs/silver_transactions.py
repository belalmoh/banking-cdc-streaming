#!/usr/bin/env python3
"""
Silver Layer - Transactions (Streaming MERGE)
Simplified version using shared config
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, current_timestamp, to_timestamp,
    trim, upper, round as spark_round
)
from pyspark.sql.types import DecimalType
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
import logging

from config import Config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class SilverTransactionsMerge:
    """Streaming MERGE for transactions Silver layer"""
    
    def __init__(self):
        self.config = Config()
        
        builder = SparkSession.builder.appName("Silver-Transactions-MERGE")
        for key, value in self.config.get_spark_configs().items():
            builder = builder.config(key, value)
        
        self.spark = configure_spark_with_delta_pip(builder).getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")
        
        self.bronze_path = f"{self.config.BRONZE_PATH}/transactions_cdc"
        self.silver_path = f"{self.config.SILVER_PATH}/transactions"
        self.checkpoint_path = f"{self.config.CHECKPOINT_PATH}/silver-transactions"
        
        logger.info("✅ Spark session initialized for Silver Transactions")
    
    def initialize_silver_table(self):
        """Create Silver table if doesn't exist"""
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
        """Transform Bronze to Silver format"""
        
        silver_df = bronze_df.select(
            col("transaction_id").alias("transaction_id"),
            col("account_id").alias("account_id"),
            upper(trim(col("transaction_type"))).alias("transaction_type"),
            spark_round(col("amount").cast(DecimalType(18, 2)), 2).alias("amount"),
            upper(trim(col("currency"))).alias("currency"),
            trim(col("merchant_name")).alias("merchant_name"),
            upper(trim(col("merchant_category"))).alias("merchant_category"),
            to_timestamp(col("transaction_timestamp")).alias("transaction_timestamp"),
            upper(trim(col("country"))).alias("country"),
            col("is_international").alias("is_international"),
            upper(trim(col("status"))).alias("status"),
            col("cdc_operation"),
            col("_ingestion_timestamp")
        )
        
        silver_df = silver_df \
            .withColumn("_created_at", current_timestamp()) \
            .withColumn("_updated_at", current_timestamp()) \
            .withColumn("_is_deleted", when(col("cdc_operation") == "d", lit(True)).otherwise(lit(False))) \
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
        """Merge batch into Silver using Delta MERGE"""
        
        if batch_df.count() == 0:
            logger.info(f"Batch {batch_id}: Empty, skipping")
            return
        
        logger.info(f"Batch {batch_id}: Processing {batch_df.count()} records")
        
        silver_batch = self.transform_bronze_to_silver(batch_df)
        
        if silver_batch.count() == 0:
            logger.info(f"Batch {batch_id}: No valid records after transformation")
            return
        
        silver_table = DeltaTable.forPath(self.spark, self.silver_path)
        
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
        
        logger.info(f"Batch {batch_id}: ✅ MERGE completed")
    
    def start_streaming(self):
        """Start streaming MERGE"""
        logger.info("=" * 70)
        logger.info("🚀 Starting Silver Transactions Streaming MERGE")
        logger.info("=" * 70)
        
        self.initialize_silver_table()
        
        bronze_stream = self.spark.readStream.format("delta").load(self.bronze_path)
        
        query = bronze_stream.writeStream \
            .foreachBatch(self.merge_to_silver) \
            .outputMode("update") \
            .option("checkpointLocation", self.checkpoint_path) \
            .trigger(processingTime="15 seconds") \
            .start()
        
        logger.info("✅ Streaming MERGE started")
        logger.info("⏹️  Press Ctrl+C to stop\n")
        
        try:
            query.awaitTermination()
        except KeyboardInterrupt:
            logger.info("\n🛑 Stopping...")
            query.stop()


def main():
    processor = SilverTransactionsMerge()
    processor.start_streaming()


if __name__ == "__main__":
    main()