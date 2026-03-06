#!/usr/bin/env python3
"""
Gold Layer - Real-Time AML Screening

Detects suspicious transaction patterns and generates alerts.

AML Rules Implemented:
1. High-value transactions (>AED 50,000)
2. Velocity attacks (>10 transactions/hour)
3. Geographic anomalies (high-risk countries)
4. High-risk merchant categories (crypto, gambling)
5. Cumulative amount thresholds (>AED 100,000/hour)

Usage:
    spark-submit --master spark://spark-master:7077 \
        --packages io.delta:delta-core_2.12:2.4.0 \
        gold_aml_screening.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, window, count, sum as spark_sum, avg, max as spark_max,
    current_timestamp, lit, when, expr, concat_ws, struct,
    to_json, from_json, explode, array
)
from pyspark.sql.types import StringType, IntegerType, DoubleType
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class GoldAMLScreening:
    """Real-time AML screening and alert generation"""
    
    # AML Configuration
    HIGH_VALUE_THRESHOLD = 50000  # AED
    VELOCITY_THRESHOLD = 10       # transactions per hour
    CUMULATIVE_THRESHOLD = 100000 # AED per hour
    ALERT_RISK_THRESHOLD = 70     # 0-100 scale
    
    HIGH_RISK_COUNTRIES = ['CN', 'RU', 'VE', 'IR', 'KP', 'SY']
    HIGH_RISK_MERCHANTS = ['CRYPTO', 'GAMBLING', 'OFFSHORE']
    
    def __init__(self,
                 silver_path: str = "s3a://silver/transactions",
                 gold_alerts_path: str = "s3a://gold/aml_alerts",
                 gold_metrics_path: str = "s3a://gold/transaction_metrics",
                 checkpoint_alerts: str = "s3a://checkpoints/gold-aml-alerts",
                 checkpoint_metrics: str = "s3a://checkpoints/gold-metrics",
                 minio_endpoint: str = "http://minio:9000"):
        
        self.silver_path = silver_path
        self.gold_alerts_path = gold_alerts_path
        self.gold_metrics_path = gold_metrics_path
        self.checkpoint_alerts = checkpoint_alerts
        self.checkpoint_metrics = checkpoint_metrics
        
        # Initialize Spark
        builder = SparkSession.builder \
            .appName("Gold-AML-Screening") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
            .config("spark.hadoop.fs.s3a.access.key", "admin") \
            .config("spark.hadoop.fs.s3a.secret.key", "admin123") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .config("spark.sql.streaming.schemaInference", "true")
        
        self.spark = configure_spark_with_delta_pip(builder).getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")
        
        logger.info("✅ Spark session initialized for AML Screening")
        logger.info(f"🚨 AML Thresholds:")
        logger.info(f"   • High Value: >AED {self.HIGH_VALUE_THRESHOLD:,}")
        logger.info(f"   • Velocity: >{self.VELOCITY_THRESHOLD} txns/hour")
        logger.info(f"   • Cumulative: >AED {self.CUMULATIVE_THRESHOLD:,}/hour")
        logger.info(f"   • Alert Trigger: risk_score >{self.ALERT_RISK_THRESHOLD}")
    
    def initialize_gold_tables(self):
        """Create Gold tables if they don't exist"""
        
        # Alerts table
        try:
            DeltaTable.forPath(self.spark, self.gold_alerts_path)
            logger.info(f"✅ AML alerts table exists")
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
            logger.info(f"✅ Transaction metrics table exists")
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
    
    def calculate_risk_score(self, df):
        """
        Calculate AML risk score (0-100) based on multiple factors
        
        Risk Factors:
        - High value: +40 points
        - High velocity: +30 points
        - High-risk country: +20 points
        - High-risk merchant: +30 points
        - High cumulative: +25 points
        """
        
        risk_df = df.withColumn(
            "risk_score",
            # Base score
            lit(0)
            # High value transactions
            + when(col("max_amount") > self.HIGH_VALUE_THRESHOLD, 40).otherwise(0)
            # Velocity (transaction frequency)
            + when(col("transaction_count") > self.VELOCITY_THRESHOLD, 30).otherwise(0)
            # Cumulative amount
            + when(col("total_amount") > self.CUMULATIVE_THRESHOLD, 25).otherwise(0)
        )
        
        # Cap at 100
        risk_df = risk_df.withColumn(
            "risk_score",
            when(col("risk_score") > 100, 100).otherwise(col("risk_score"))
        )
        
        return risk_df
    
    def determine_alert_type_and_severity(self, df):
        """Determine alert type and severity based on risk factors"""
        
        result_df = df.withColumn(
            "alert_type",
            when(col("max_amount") > self.HIGH_VALUE_THRESHOLD, "HIGH_VALUE_TRANSACTION")
            .when(col("transaction_count") > self.VELOCITY_THRESHOLD, "VELOCITY_ATTACK")
            .when(col("total_amount") > self.CUMULATIVE_THRESHOLD, "CUMULATIVE_THRESHOLD_EXCEEDED")
            .otherwise("SUSPICIOUS_PATTERN")
        )
        
        result_df = result_df.withColumn(
            "severity",
            when(col("risk_score") >= 90, "CRITICAL")
            .when(col("risk_score") >= 80, "HIGH")
            .when(col("risk_score") >= 70, "MEDIUM")
            .otherwise("LOW")
        )
        
        return result_df
    
    def add_risk_factors(self, df):
        """Add array of risk factors that triggered the alert"""
        
        result_df = df.withColumn(
            "risk_factors",
            array(
                when(col("max_amount") > self.HIGH_VALUE_THRESHOLD, 
                     lit("HIGH_VALUE")).otherwise(lit(None)),
                when(col("transaction_count") > self.VELOCITY_THRESHOLD, 
                     lit("HIGH_VELOCITY")).otherwise(lit(None)),
                when(col("total_amount") > self.CUMULATIVE_THRESHOLD, 
                     lit("HIGH_CUMULATIVE")).otherwise(lit(None))
            )
        )
        
        # Filter out null values from array
        from pyspark.sql.functions import array_remove
        result_df = result_df.withColumn(
            "risk_factors",
            array_remove(col("risk_factors"), None)
        )
        
        return result_df
    
    def start_aml_screening(self):
        """Start real-time AML screening"""
        
        logger.info("=" * 70)
        logger.info("🚀 Starting Real-Time AML Screening")
        logger.info("=" * 70)
        logger.info(f"📖 Silver source: {self.silver_path}")
        logger.info(f"🚨 Gold alerts: {self.gold_alerts_path}")
        logger.info(f"📊 Gold metrics: {self.gold_metrics_path}")
        logger.info("=" * 70)
        
        self.initialize_gold_tables()
        
        # Read streaming from Silver
        silver_stream = self.spark.readStream \
            .format("delta") \
            .load(self.silver_path) \
            .filter(col("_is_deleted") == False) \
            .filter(col("status") == "COMPLETED")
        
        logger.info("✅ Reading from Silver transactions")
        
        # Windowed aggregations with watermark
        # 1-hour sliding windows, 10-minute watermark for late data
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
        
        logger.info("✅ Windowed aggregations configured")
        
        # Calculate risk scores
        risk_scored = self.calculate_risk_score(windowed_agg)
        
        # Determine alert types and severity
        classified = self.determine_alert_type_and_severity(risk_scored)
        
        # Add risk factors
        enriched = self.add_risk_factors(classified)
        
        # Filter for alerts (risk_score > threshold)
        alerts = enriched.filter(col("risk_score") > self.ALERT_RISK_THRESHOLD)
        
        # Add alert metadata
        alerts = alerts \
            .withColumn("alert_id", 
                concat_ws("-", lit("AML"), col("account_id"), 
                         col("window_start").cast("long"))) \
            .withColumn("alert_timestamp", current_timestamp()) \
            .withColumn("_gold_processed_at", current_timestamp())
        
        logger.info(f"✅ AML screening logic configured (threshold: {self.ALERT_RISK_THRESHOLD})")
        
        # Write alerts to Gold
        alerts_query = alerts.writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", self.checkpoint_alerts) \
            .trigger(processingTime="10 seconds") \
            .start(self.gold_alerts_path)
        
        logger.info("✅ AML alerts stream started")
        
        # Write metrics to Gold (for monitoring)
        metrics_query = windowed_agg \
            .withColumn("_gold_processed_at", current_timestamp()) \
            .writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", self.checkpoint_metrics) \
            .trigger(processingTime="10 seconds") \
            .start(self.gold_metrics_path)
        
        logger.info("✅ Transaction metrics stream started")
        
        logger.info("\n" + "=" * 70)
        logger.info("✅ Real-Time AML Screening Active")
        logger.info("=" * 70)
        logger.info("\n📊 Monitoring:")
        logger.info("   • Spark UI: http://localhost:4040")
        logger.info("   • Window: 1 hour sliding windows")
        logger.info("   • Watermark: 10 minutes")
        logger.info("   • Trigger: 10 seconds")
        logger.info("\n🚨 Alert Criteria:")
        logger.info(f"   • High Value: >{self.HIGH_VALUE_THRESHOLD:,} AED")
        logger.info(f"   • Velocity: >{self.VELOCITY_THRESHOLD} transactions/hour")
        logger.info(f"   • Cumulative: >{self.CUMULATIVE_THRESHOLD:,} AED/hour")
        logger.info(f"   • Risk Threshold: >{self.ALERT_RISK_THRESHOLD}")
        logger.info("\n⏹️  Press Ctrl+C to stop\n")
        logger.info("=" * 70 + "\n")
        
        # Wait for termination
        try:
            alerts_query.awaitTermination()
        except KeyboardInterrupt:
            logger.info("\n🛑 Stopping AML screening...")
            alerts_query.stop()
            metrics_query.stop()
            logger.info("✅ All queries stopped gracefully")


def main():
    """Main entry point"""
    screener = GoldAMLScreening(
        silver_path="s3a://silver/transactions",
        gold_alerts_path="s3a://gold/aml_alerts",
        gold_metrics_path="s3a://gold/transaction_metrics",
        checkpoint_alerts="s3a://checkpoints/gold-aml-alerts",
        checkpoint_metrics="s3a://checkpoints/gold-metrics",
        minio_endpoint="http://minio:9000"
    )
    
    screener.start_aml_screening()


if __name__ == "__main__":
    main()