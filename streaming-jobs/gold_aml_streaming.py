#!/usr/bin/env python3
"""
Gold Layer - Real-Time AML Screening
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, window, count, sum as spark_sum, avg, max as spark_max,
    current_timestamp, lit, when, concat_ws, array, array_remove
)
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
import logging

from config import Config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class GoldAMLScreening:
    """Real-time AML screening and alert generation"""
    
    # AML Configuration
    HIGH_VALUE_THRESHOLD = 50000
    VELOCITY_THRESHOLD = 10
    CUMULATIVE_THRESHOLD = 100000
    ALERT_RISK_THRESHOLD = 70
    
    def __init__(self):
        self.config = Config()
        
        builder = SparkSession.builder.appName("Gold-AML-Screening")
        for key, value in self.config.get_spark_configs().items():
            builder = builder.config(key, value)
        
        self.spark = configure_spark_with_delta_pip(builder).getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")
        
        self.silver_path = f"{self.config.SILVER_PATH}/transactions"
        self.gold_alerts_path = f"{self.config.GOLD_PATH}/aml_alerts"
        self.gold_metrics_path = f"{self.config.GOLD_PATH}/transaction_metrics"
        self.checkpoint_alerts = f"{self.config.CHECKPOINT_PATH}/gold-aml-alerts"
        self.checkpoint_metrics = f"{self.config.CHECKPOINT_PATH}/gold-metrics"
        
        logger.info("✅ Spark session initialized for AML Screening")
        logger.info(f"🚨 High Value Threshold: >AED {self.HIGH_VALUE_THRESHOLD:,}")
        logger.info(f"🚨 Velocity Threshold: >{self.VELOCITY_THRESHOLD} txns/hour")
        logger.info(f"🚨 Alert Threshold: risk_score >{self.ALERT_RISK_THRESHOLD}")
    
    def initialize_gold_tables(self):
        """Create Gold tables if they don't exist"""
        
        # Alerts table
        try:
            DeltaTable.forPath(self.spark, self.gold_alerts_path)
        except Exception:
            logger.info(f"📝 Creating AML alerts table")
            
            from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, ArrayType
            
            schema = StructType([
                StructField("alert_id", StringType(), False),
                StructField("account_id", StringType(), False),
                StructField("alert_type", StringType(), True),
                StructField("severity", StringType(), True),
                StructField("risk_score", IntegerType(), True),
                StructField("window_start", TimestampType(), True),
                StructField("window_end", TimestampType(), True),
                StructField("transaction_count", IntegerType(), True),
                StructField("total_amount", DoubleType(), True),
                StructField("avg_amount", DoubleType(), True),
                StructField("max_amount", DoubleType(), True),
                StructField("risk_factors", ArrayType(StringType()), True),
                StructField("alert_timestamp", TimestampType(), True),
                StructField("_gold_processed_at", TimestampType(), True)
            ])
            
            empty_df = self.spark.createDataFrame([], schema)
            empty_df.write.format("delta").save(self.gold_alerts_path)
            logger.info("✅ AML alerts table created")
        
        # Metrics table
        try:
            DeltaTable.forPath(self.spark, self.gold_metrics_path)
        except Exception:
            logger.info(f"📝 Creating transaction metrics table")
            
            from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
            
            schema = StructType([
                StructField("account_id", StringType(), False),
                StructField("window_start", TimestampType(), False),
                StructField("window_end", TimestampType(), False),
                StructField("transaction_count", IntegerType(), True),
                StructField("total_amount", DoubleType(), True),
                StructField("avg_amount", DoubleType(), True),
                StructField("max_amount", DoubleType(), True),
                StructField("_gold_processed_at", TimestampType(), True)
            ])
            
            empty_df = self.spark.createDataFrame([], schema)
            empty_df.write.format("delta").save(self.gold_metrics_path)
            logger.info("✅ Transaction metrics table created")
    
    def start_aml_screening(self):
        """Start real-time AML screening"""
        
        logger.info("=" * 70)
        logger.info("🚀 Starting Real-Time AML Screening")
        logger.info("=" * 70)
        
        self.initialize_gold_tables()
        
        # Read from Silver
        silver_stream = self.spark.readStream \
            .format("delta") \
            .load(self.silver_path) \
            .filter(col("_is_deleted") == False) \
            .filter(col("status") == "COMPLETED")
        
        logger.info("✅ Reading from Silver transactions")
        
        # Windowed aggregations
        windowed_agg = silver_stream \
            .withWatermark("transaction_timestamp", "10 minutes") \
            .groupBy(
                col("account_id"),
                window(col("transaction_timestamp"), "1 hour")
            ) \
            .agg(
                count("*").alias("transaction_count"),
                spark_sum("amount").alias("total_amount"),
                avg("amount").alias("avg_amount"),
                spark_max("amount").alias("max_amount")
            ) \
            .select(
                col("account_id"),
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("transaction_count"),
                col("total_amount"),
                col("avg_amount"),
                col("max_amount")
            )
        
        # Calculate risk scores
        risk_scored = windowed_agg.withColumn(
            "risk_score",
            lit(0)
            + when(col("max_amount") > self.HIGH_VALUE_THRESHOLD, 40).otherwise(0)
            + when(col("transaction_count") > self.VELOCITY_THRESHOLD, 30).otherwise(0)
            + when(col("total_amount") > self.CUMULATIVE_THRESHOLD, 25).otherwise(0)
        )
        
        # Cap at 100
        risk_scored = risk_scored.withColumn(
            "risk_score",
            when(col("risk_score") > 100, 100).otherwise(col("risk_score"))
        )
        
        # Determine alert type and severity
        classified = risk_scored \
            .withColumn(
                "alert_type",
                when(col("max_amount") > self.HIGH_VALUE_THRESHOLD, "HIGH_VALUE_TRANSACTION")
                .when(col("transaction_count") > self.VELOCITY_THRESHOLD, "VELOCITY_ATTACK")
                .when(col("total_amount") > self.CUMULATIVE_THRESHOLD, "CUMULATIVE_THRESHOLD_EXCEEDED")
                .otherwise("SUSPICIOUS_PATTERN")
            ) \
            .withColumn(
                "severity",
                when(col("risk_score") >= 90, "CRITICAL")
                .when(col("risk_score") >= 80, "HIGH")
                .when(col("risk_score") >= 70, "MEDIUM")
                .otherwise("LOW")
            )
        
        # Add risk factors
        enriched = classified.withColumn(
            "risk_factors",
            array_remove(
                array(
                    when(col("max_amount") > self.HIGH_VALUE_THRESHOLD, lit("HIGH_VALUE")).otherwise(lit(None)),
                    when(col("transaction_count") > self.VELOCITY_THRESHOLD, lit("HIGH_VELOCITY")).otherwise(lit(None)),
                    when(col("total_amount") > self.CUMULATIVE_THRESHOLD, lit("HIGH_CUMULATIVE")).otherwise(lit(None))
                ),
                None
            )
        )
        
        # Filter for alerts
        alerts = enriched.filter(col("risk_score") > self.ALERT_RISK_THRESHOLD)
        
        # Add alert metadata
        alerts = alerts \
            .withColumn("alert_id", concat_ws("-", lit("AML"), col("account_id"), col("window_start").cast("long"))) \
            .withColumn("alert_timestamp", current_timestamp()) \
            .withColumn("_gold_processed_at", current_timestamp())
        
        # Write alerts
        alerts_query = alerts.writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", self.checkpoint_alerts) \
            .trigger(processingTime="10 seconds") \
            .start(self.gold_alerts_path)
        
        logger.info("✅ AML alerts stream started")
        
        # Write metrics
        metrics_query = windowed_agg \
            .withColumn("_gold_processed_at", current_timestamp()) \
            .writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", self.checkpoint_metrics) \
            .trigger(processingTime="10 seconds") \
            .start(self.gold_metrics_path)
        
        logger.info("✅ Transaction metrics stream started")
        logger.info("⏹️  Press Ctrl+C to stop\n")
        
        try:
            alerts_query.awaitTermination()
        except KeyboardInterrupt:
            logger.info("\n🛑 Stopping...")
            alerts_query.stop()
            metrics_query.stop()


def main():
    screener = GoldAMLScreening()
    screener.start_aml_screening()


if __name__ == "__main__":
    main()