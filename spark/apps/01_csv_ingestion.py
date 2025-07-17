"""
Simple CSV File Ingestion Example
=================================

This script demonstrates how to ingest CSV files into MinIO using Spark.
It reads a local CSV file and stores it in the data lake.

Usage:
    docker exec spark-master /opt/spark/bin/spark-submit \
        --jars /opt/spark/jars-extra/hadoop-aws-3.3.4.jar,/opt/spark/jars-extra/aws-java-sdk-bundle-1.11.1026.jar \
        /opt/spark/apps/01_csv_ingestion.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
import os

from ingestion_service import IngestionService
from validation_service import ValidationService

def create_spark_session():
    """Create Spark session with MinIO configuration"""
    # Get credentials from environment variables
    minio_access_key = os.getenv("MINIO_ROOT_USER", "minioadmin")
    minio_secret_key = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123")
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    
    return SparkSession.builder \
        .appName("CSV_Ingestion_Example") \
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()

def main():
    print("Starting CSV Ingestion Example")
    print("=" * 50)
    
    # Initialize Spark
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Initialize services
        ingestion_service = IngestionService(spark)
        validation_service = ValidationService(spark)
        
        # Read CSV file using ingestion service
        print("Reading CSV file: sample_customers.csv")
        customers_df = ingestion_service.read_from_local("/opt/spark/data/sample_customers.csv")
        
        print("Schema:")
        customers_df.printSchema()
        
        print("\nSample data:")
        customers_df.show(5)
        
        print("\n🚀 USING TRANSIENT LANDING ZONE PIPELINE WITH VALIDATION")
        print("=" * 60)
        
        # 1. Validate data quality before ingestion
        print("1. Validating data quality...")
        validation_results = validation_service.validate_data(customers_df, "customers")
        
        # 2. Decide whether to proceed based on validation
        if validation_results.get("success", False):
            print("\n✅ Data quality validation passed - proceeding with ingestion")
            
            # 3. Ingest to Transient Zone
            print("\n2. Ingesting to Transient Zone...")
            transient_path = ingestion_service.ingest_to_transient_zone(customers_df, "customers")
            
            # 4. Read from Transient Zone
            print("\n3. Reading from Transient Zone...")
            transient_df = ingestion_service.read_from_bucket("transient-landing-zone", "customers", format="csv")
            
            print("\nSample data from Transient Zone:")
            transient_df.show(3)
            
        else:
            print("\n❌ Data quality validation failed - stopping ingestion")
            print("Data needs to be reviewed and corrected before proceeding")
        
        # Cleanup services
        validation_service.cleanup()
        
        print("\nCSV Ingestion completed successfully!")
        
    except Exception as e:
        print(f"Error during CSV ingestion: {e}")
        
    finally:
        spark.stop()
        print("Spark session stopped")

if __name__ == "__main__":
    main()