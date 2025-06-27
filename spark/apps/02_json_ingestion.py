"""
Simple JSON File Ingestion Example
==================================

This script demonstrates how to ingest JSON files into MinIO using Spark.
It reads a local JSON file and stores it in the data lake.

Usage:
    docker exec spark-master /opt/spark/bin/spark-submit \
        --jars /opt/spark/jars-extra/hadoop-aws-3.3.4.jar,/opt/spark/jars-extra/aws-java-sdk-bundle-1.11.1026.jar \
        /opt/spark/apps/02_json_ingestion.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, col
import os

def create_spark_session():
    """Create Spark session with MinIO configuration"""
    # Get credentials from environment variables
    minio_access_key = os.getenv("MINIO_ROOT_USER", "minioadmin")
    minio_secret_key = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123")
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    
    return SparkSession.builder \
        .appName("JSON_Ingestion_Example") \
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()

def main():
    print("🚀 Starting JSON Ingestion Example")
    print("=" * 50)
    
    # Initialize Spark
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Read JSON file from local filesystem
        print("📖 Reading JSON file: sample_transactions.json")
        transactions_df = spark.read.json("file:///opt/spark/data/sample_transactions.json")
        
        print("✅ JSON file loaded successfully!")
        print(f"📊 Total records: {transactions_df.count()}")
        print("📋 Schema:")
        transactions_df.printSchema()
        
        print("\n🔍 Sample data:")
        transactions_df.show(3, truncate=False)
        
        # Store raw data in original JSON format
        output_path = "s3a://test-bucket/raw-data/transactions/"
        print(f"Writing raw JSON data to MinIO: {output_path}")
        
        transactions_df.write.mode("overwrite").json(output_path)
        print("Raw JSON data successfully stored!")
        
        # Analytics on original data
        print("\nQuick Analytics:")
        print("Transaction status distribution:")
        transactions_df.groupBy("status").count().orderBy("count", ascending=False).show()
        
        print("Transaction amounts by currency:")
        transactions_df.groupBy("currency").agg({"amount": "sum"}).show()
        
        # Verify the ingestion
        print("\nVerifying data in MinIO...")
        read_back_df = spark.read.json(output_path)
        print(f"Verification successful! Records in MinIO: {read_back_df.count()}")
        
        print("\nSample data from MinIO:")
        read_back_df.show(2, truncate=False)
        
        print("\n🎉 JSON Ingestion completed successfully!")
        
    except Exception as e:
        print(f"❌ Error during JSON ingestion: {e}")
        
    finally:
        spark.stop()
        print("🔄 Spark session stopped")

if __name__ == "__main__":
    main()