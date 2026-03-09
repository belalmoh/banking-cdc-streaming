"""
Schema Definitions for Debezium CDC Events and Delta Tables
"""

from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, 
    DoubleType, BooleanType, TimestampType, IntegerType, DateType, DecimalType
)


class DebeziumSchemas:
    """Debezium CDC event schemas"""
    
    @staticmethod
    def get_debezium_envelope():
        """Common Debezium envelope structure"""
        return StructType([
            StructField("before", StringType(), True),  # JSON string
            StructField("after", StringType(), True),   # JSON string
            StructField("source", StructType([
                StructField("version", StringType(), True),
                StructField("connector", StringType(), True),
                StructField("name", StringType(), True),
                StructField("ts_ms", LongType(), True),
                StructField("db", StringType(), True),
                StructField("schema", StringType(), True),
                StructField("table", StringType(), True),
                StructField("txId", LongType(), True),
                StructField("lsn", LongType(), True)
            ]), True),
            StructField("op", StringType(), True),  # c, u, d, r
            StructField("ts_ms", LongType(), True),
            StructField("transaction", StructType([
                StructField("id", StringType(), True),
                StructField("total_order", LongType(), True),
                StructField("data_collection_order", LongType(), True)
            ]), True)
        ])
    
    @staticmethod
    def get_transaction_payload():
        """Transaction payload schema (after ExtractNewRecordState unwrap)"""
        return StructType([
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
            StructField("status", StringType(), True),
            StructField("created_at", StringType(), True),
            # Debezium ExtractNewRecordState metadata fields
            StructField("__op", StringType(), True),
            StructField("__source_ts_ms", LongType(), True),
            StructField("__source_db", StringType(), True),
            StructField("__source_table", StringType(), True),
            StructField("__deleted", StringType(), True),
        ])
    
    @staticmethod
    def get_customer_payload():
        """Customer payload schema (after ExtractNewRecordState unwrap)"""
        return StructType([
            StructField("customer_id", StringType(), True),
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("phone", StringType(), True),
            StructField("date_of_birth", StringType(), True),
            StructField("nationality", StringType(), True),
            StructField("risk_score", IntegerType(), True),
            StructField("kyc_status", StringType(), True),
            StructField("onboarding_date", StringType(), True),
            StructField("customer_segment", StringType(), True),
            StructField("country_residence", StringType(), True),
            StructField("created_at", StringType(), True),
            StructField("updated_at", StringType(), True),
            # Debezium ExtractNewRecordState metadata fields
            StructField("__op", StringType(), True),
            StructField("__source_ts_ms", LongType(), True),
            StructField("__source_db", StringType(), True),
            StructField("__source_table", StringType(), True),
            StructField("__deleted", StringType(), True),
        ])
    
    @staticmethod
    def get_account_payload():
        """Account payload schema (after ExtractNewRecordState unwrap)"""
        return StructType([
            StructField("account_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("account_type", StringType(), True),
            StructField("currency", StringType(), True),
            StructField("balance", DoubleType(), True),
            StructField("status", StringType(), True),
            StructField("opened_date", StringType(), True),
            StructField("last_activity_date", StringType(), True),
            StructField("created_at", StringType(), True),
            StructField("updated_at", StringType(), True),
            # Debezium ExtractNewRecordState metadata fields
            StructField("__op", StringType(), True),
            StructField("__source_ts_ms", LongType(), True),
            StructField("__source_db", StringType(), True),
            StructField("__source_table", StringType(), True),
            StructField("__deleted", StringType(), True),
        ])


class DeltaSchemas:
    """Delta Lake table schemas"""
    
    @staticmethod
    def get_bronze_transactions():
        """Bronze layer transactions schema"""
        return StructType([
            StructField("transaction_id", StringType(), False),
            StructField("account_id", StringType(), True),
            StructField("transaction_type", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("currency", StringType(), True),
            StructField("merchant_name", StringType(), True),
            StructField("merchant_category", StringType(), True),
            StructField("transaction_timestamp", StringType(), True),
            StructField("country", StringType(), True),
            StructField("is_international", BooleanType(), True),
            StructField("status", StringType(), True),
            StructField("created_at", StringType(), True),
            # CDC metadata
            StructField("cdc_operation", StringType(), True),
            StructField("cdc_timestamp_ms", LongType(), True),
            StructField("source_table", StringType(), True),
            StructField("source_database", StringType(), True),
            # Audit columns
            StructField("_ingestion_timestamp", TimestampType(), True),
            StructField("_ingestion_date", DateType(), True),
            StructField("_entity_type", StringType(), True),
            StructField("_data_classification", StringType(), True),
            StructField("_data_residency", StringType(), True)
        ])

    @staticmethod
    def get_bronze_customers():
        """Bronze layer customers schema"""
        return StructType([
            StructField("customer_id", StringType(), False),
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("phone", StringType(), True),
            StructField("date_of_birth", StringType(), True),
            StructField("nationality", StringType(), True),
            StructField("risk_score", IntegerType(), True),
            StructField("kyc_status", StringType(), True),
            StructField("onboarding_date", StringType(), True),
            StructField("customer_segment", StringType(), True),
            StructField("country_residence", StringType(), True),
            StructField("created_at", StringType(), True),
            StructField("updated_at", StringType(), True),
            # CDC metadata
            StructField("cdc_operation", StringType(), True),
            StructField("cdc_timestamp_ms", LongType(), True),
            StructField("source_table", StringType(), True),
            StructField("source_database", StringType(), True),
            # Audit columns
            StructField("_ingestion_timestamp", TimestampType(), True),
            StructField("_ingestion_date", DateType(), True),
            StructField("_entity_type", StringType(), True),
            StructField("_data_classification", StringType(), True),
            StructField("_data_residency", StringType(), True)
        ])

    @staticmethod
    def get_bronze_accounts():
        """Bronze layer accounts schema"""
        return StructType([
            StructField("account_id", StringType(), False),
            StructField("customer_id", StringType(), True),
            StructField("account_type", StringType(), True),
            StructField("currency", StringType(), True),
            StructField("balance", DoubleType(), True),
            StructField("status", StringType(), True),
            StructField("opened_date", StringType(), True),
            StructField("last_activity_date", StringType(), True),
            StructField("created_at", StringType(), True),
            StructField("updated_at", StringType(), True),
            # CDC metadata
            StructField("cdc_operation", StringType(), True),
            StructField("cdc_timestamp_ms", LongType(), True),
            StructField("source_table", StringType(), True),
            StructField("source_database", StringType(), True),
            # Audit columns
            StructField("_ingestion_timestamp", TimestampType(), True),
            StructField("_ingestion_date", DateType(), True),
            StructField("_entity_type", StringType(), True),
            StructField("_data_classification", StringType(), True),
            StructField("_data_residency", StringType(), True)
        ])


    @staticmethod
    def get_silver_customers():
        """Silver layer customers schema"""
        return StructType([
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