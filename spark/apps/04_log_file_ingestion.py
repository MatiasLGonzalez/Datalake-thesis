"""
Log File Ingestion Example
==========================

This script demonstrates how to ingest unstructured log files into MinIO using Spark.
It parses log files and extracts structured data.

Usage:
    docker exec spark-master /opt/spark/bin/spark-submit \
        --jars /opt/spark/jars-extra/hadoop-aws-3.3.4.jar,/opt/spark/jars-extra/aws-java-sdk-bundle-1.11.1026.jar \
        /opt/spark/apps/04_log_file_ingestion.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, col, regexp_extract, split, when
import os

def create_spark_session():
    """Create Spark session with MinIO configuration"""
    # Get credentials from environment variables
    minio_access_key = os.getenv("MINIO_ROOT_USER", "minioadmin")
    minio_secret_key = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123")
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    
    return SparkSession.builder \
        .appName("Log_File_Ingestion") \
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()

def main():
    print("Starting Log File Ingestion Example")
    print("=" * 50)
    
    # Initialize Spark
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Read log file as text
        print("Reading log file: sample_logs.log")
        logs_raw = spark.read.text("file:///opt/spark/data/sample_logs.log")
        
        print("Log file loaded successfully!")
        print(f"Total log lines: {logs_raw.count()}")
        
        print("\nRaw log sample:")
        logs_raw.show(3, truncate=False)
        
        # Store data in original format
        output_path = "s3a://test-bucket/raw-data/logs/"
        print(f"Writing data (text format) to MinIO: {output_path}")
        
        logs_raw.write.mode("overwrite").text(output_path)
        print("Data successfully stored in original text format!")
        
        # Parse log entries using regex
        # Log format: YYYY-MM-DD HH:MM:SS LEVEL [Service] Message
        logs_parsed = logs_raw.select(
            regexp_extract(col("value"), r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", 1).alias("timestamp"),
            regexp_extract(col("value"), r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} (\w+)", 1).alias("log_level"),
            regexp_extract(col("value"), r"\[(\w+)\]", 1).alias("service"),
            regexp_extract(col("value"), r"\] (.+)$", 1).alias("message"),
            col("value").alias("raw_log")
        ).filter(col("timestamp") != "")  # Filter out empty matches
        
        print("Log parsing completed!")
        print("Parsed Schema:")
        logs_parsed.printSchema()
        
        print("\nParsed log sample:")
        logs_parsed.show(5, truncate=False)
        
        # STEP 2: Process and store in optimized format with metadata
        logs_enhanced = logs_parsed \
            .withColumn("user_id", regexp_extract(col("message"), r"user_id=(\d+)", 1)) \
            .withColumn("transaction_id", regexp_extract(col("message"), r"transaction_id=([a-zA-Z0-9_]+)", 1)) \
            .withColumn("product_id", regexp_extract(col("message"), r"product_id=([a-zA-Z0-9_]+)", 1)) \
            .withColumn("ip_address", regexp_extract(col("message"), r"ip=([0-9.]+)", 1)) \
            .withColumn("ingested_at", current_timestamp()) \
            .withColumn("source_file", lit("sample_logs.log")) \
            .withColumn("file_format", lit("LOG")) \
            .withColumn("processing_timestamp", current_timestamp())
        
        processed_output_path = "s3a://test-bucket/processed-zone/logs/"
        print(f"Step 2: Writing processed data (Parquet format) to MinIO: {processed_output_path}")
        
        logs_enhanced.write.mode("overwrite").parquet(processed_output_path)
        print("Processed data successfully written in optimized Parquet format!")
        
        # STEP 3: Verify both zones and show architecture demo
        print("\nVerifying data in both zones...")
        
        # Verify raw zone
        raw_read_back = spark.read.text(raw_output_path)
        print(f"Raw zone verification: {raw_read_back.count()} records (text format)")
        
        # Verify processed zone
        processed_read_back = spark.read.parquet(processed_output_path)
        print(f"Processed zone verification: {processed_read_back.count()} records (Parquet format)")
        
        print("\nData Lake Architecture Demo:")
        print("Raw Zone (Text):")
        raw_read_back.show(3, truncate=False)
        
        print("Processed Zone (Parquet with metadata and parsing):")
        processed_read_back.show(3, truncate=False)
        
        # Show some analytics
        print("\nLog Analytics:")
        print("Log levels distribution:")
        logs_enhanced.groupBy("log_level").count().orderBy("count", ascending=False).show()
        
        print("Service activity:")
        logs_enhanced.groupBy("service").count().orderBy("count", ascending=False).show()
        
        print("Error messages:")
        logs_enhanced.filter(col("log_level") == "ERROR").select("timestamp", "service", "message").show(truncate=False)
        
        print("\nLog File Ingestion completed successfully!")
        
    except Exception as e:
        print(f"Error during log file ingestion: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        spark.stop()
        print("Spark session stopped")

if __name__ == "__main__":
    main()