"""
Delimited File Ingestion Example (Pipe-separated)
================================================

This script demonstrates how to ingest pipe-delimited files into MinIO using Spark.
It reads a local delimited file and stores it in the data lake.

Usage:
    docker exec spark-master /opt/spark/bin/spark-submit \
        --jars /opt/spark/jars-extra/hadoop-aws-3.3.4.jar,/opt/spark/jars-extra/aws-java-sdk-bundle-1.11.1026.jar \
        /opt/spark/apps/03_delimited_file_ingestion.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import os

def create_spark_session():
    """Create Spark session with MinIO configuration"""
    # Get credentials from environment variables
    minio_access_key = os.getenv("MINIO_ROOT_USER", "minioadmin")
    minio_secret_key = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123")
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    
    return SparkSession.builder \
        .appName("Delimited_File_Ingestion") \
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()

def main():
    print("Starting Delimited File Ingestion Example")
    print("=" * 50)
    
    # Initialize Spark
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Define schema for better type inference
        products_schema = StructType([
            StructField("product_id", StringType(), True),
            StructField("product_name", StringType(), True),
            StructField("category", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("stock_quantity", IntegerType(), True),
            StructField("supplier", StringType(), True),
            StructField("description", StringType(), True)
        ])
        
        # Read pipe-delimited file from local filesystem
        print("Reading pipe-delimited file: sample_products.txt")
        products_df = spark.read \
            .option("header", "true") \
            .option("delimiter", "|") \
            .schema(products_schema) \
            .csv("file:///opt/spark/data/sample_products.txt")
        
        print("Delimited file loaded successfully!")
        print(f"Total records: {products_df.count()}")
        print("Schema:")
        products_df.printSchema()
        
        print("\nSample data:")
        products_df.show(5, truncate=False)
        
        # Store data in original format
        output_path = "s3a://test-bucket/raw-data/products/"
        print(f"Writing data (pipe-delimited format) to MinIO: {output_path}")
        
        products_df.write.mode("overwrite").option("header", "true").option("delimiter", "|").csv(output_path)
        print("Data successfully stored in original pipe-delimited format!")
        
        # Verify data storage
        print("\nVerifying stored data...")
        read_back = spark.read.option("header", "true").option("delimiter", "|").csv(output_path)
        print(f"Verification: {read_back.count()} records stored")
        
        print("\nStored data sample:")
        read_back.show(3, truncate=False)
        
        # Show some analytics on original data
        print("\nQuick Analytics:")
        print("Products by category:")
        read_back.groupBy("category").count().orderBy("count", ascending=False).show()
        
        print("Price statistics by category:")
        read_back.groupBy("category").agg(
            {"price": "avg", "price": "min", "price": "max"}
        ).show()
        
        print("Stock levels:")
        read_back.select("product_name", "stock_quantity").orderBy("stock_quantity").show()
        
        print("\nDelimited File Ingestion completed successfully!")
        
    except Exception as e:
        print(f"Error during delimited file ingestion: {e}")
        
    finally:
        spark.stop()
        print("Spark session stopped")

if __name__ == "__main__":
    main()