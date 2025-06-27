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
        # Read CSV file from local filesystem
        print("Reading CSV file: sample_customers.csv")
        customers_df = spark.read.option("header", "true").option("inferSchema", "true").csv("file:///opt/spark/data/sample_customers.csv")
        
        print("CSV file loaded successfully!")
        print(f"Total records: {customers_df.count()}")
        print("Schema:")
        customers_df.printSchema()
        
        print("\nSample data:")
        customers_df.show(5)
        
        # Store raw data in original CSV format
        output_path = "s3a://test-bucket/raw-data/customers/"
        print(f"Writing raw CSV data to MinIO: {output_path}")
        
        customers_df.write.mode("overwrite").option("header", "true").csv(output_path)
        print("Raw CSV data successfully stored!")
        
        # Verify the ingestion
        print("\nVerifying data in MinIO...")
        read_back_df = spark.read.option("header", "true").option("inferSchema", "true").csv(output_path)
        print(f"Verification successful! Records in MinIO: {read_back_df.count()}")
        
        print("\nSample data from MinIO:")
        read_back_df.show(3)
        
        print("\nCSV Ingestion completed successfully!")
        
    except Exception as e:
        print(f"Error during CSV ingestion: {e}")
        
    finally:
        spark.stop()
        print("Spark session stopped")

if __name__ == "__main__":
    main()