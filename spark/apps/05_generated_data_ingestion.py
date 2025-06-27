"""
Generated Data Ingestion Example
================================

This script demonstrates how to generate synthetic data and ingest it into MinIO.
Useful for testing, prototyping, and creating sample datasets.

Usage:
    docker exec spark-master /opt/spark/bin/spark-submit \
        --jars /opt/spark/jars-extra/hadoop-aws-3.3.4.jar,/opt/spark/jars-extra/aws-java-sdk-bundle-1.11.1026.jar \
        /opt/spark/apps/05_generated_data_ingestion.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, col, rand, when, expr, date_add, current_date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
import random
import os

def create_spark_session():
    """Create Spark session with MinIO configuration"""
    # Get credentials from environment variables
    minio_access_key = os.getenv("MINIO_ROOT_USER", "minioadmin")
    minio_secret_key = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123")
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    
    return SparkSession.builder \
        .appName("Generated_Data_Ingestion") \
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()

def main():
    print("🚀 Starting Generated Data Ingestion Example")
    print("=" * 50)
    
    # Initialize Spark
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Generate synthetic sales data
        print("📊 Generating synthetic sales data...")
        
        # Create base DataFrame with sequential IDs
        sales_df = spark.range(1, 1001).select(col("id").alias("sale_id"))
        
        # Add random customer data
        sales_df = sales_df \
            .withColumn("customer_id", (rand() * 100).cast("int") + 1) \
            .withColumn("product_category", 
                when(col("sale_id") % 5 == 0, "Electronics")
                .when(col("sale_id") % 5 == 1, "Clothing")
                .when(col("sale_id") % 5 == 2, "Books")
                .when(col("sale_id") % 5 == 3, "Sports")
                .otherwise("Home")) \
            .withColumn("sale_amount", (rand() * 500 + 10).cast("double")) \
            .withColumn("quantity", (rand() * 10 + 1).cast("int")) \
            .withColumn("sale_date", date_add(current_date(), -(rand() * 365).cast("int"))) \
            .withColumn("payment_method",
                when(rand() < 0.4, "credit_card")
                .when(rand() < 0.7, "debit_card")
                .when(rand() < 0.9, "paypal")
                .otherwise("bank_transfer")) \
            .withColumn("store_location",
                when(rand() < 0.25, "New York")
                .when(rand() < 0.5, "Los Angeles")
                .when(rand() < 0.75, "Chicago")
                .otherwise("Houston"))
        
        print("✅ Synthetic data generated successfully!")
        print(f"📊 Total records: {sales_df.count()}")
        print("📋 Schema:")
        sales_df.printSchema()
        
        print("\n🔍 Sample generated data:")
        sales_df.show(10)
        
        # Add metadata
        sales_with_metadata = sales_df \
            .withColumn("ingested_at", current_timestamp()) \
            .withColumn("data_source", lit("synthetic_generator")) \
            .withColumn("file_format", lit("GENERATED"))
        
        # Write to MinIO
        output_path = "s3a://test-bucket/raw-data/sales/"
        print(f"💾 Writing generated data to MinIO: {output_path}")
        
        sales_with_metadata.write.mode("overwrite").parquet(output_path)
        print("✅ Data successfully written to MinIO!")
        
        # Generate some analytics
        print("\n📈 Generated Data Analytics:")
        
        print("Sales by category:")
        sales_df.groupBy("product_category").agg(
            {"sale_amount": "sum", "sale_amount": "count"}
        ).show()
        
        print("Sales by payment method:")
        sales_df.groupBy("payment_method").count().show()
        
        print("Sales by store location:")
        sales_df.groupBy("store_location").agg(
            {"sale_amount": "sum", "quantity": "sum"}
        ).show()
        
        print("Top customers by total spending:")
        sales_df.groupBy("customer_id").agg(
            {"sale_amount": "sum"}
        ).orderBy(col("sum(sale_amount)").desc()).show(10)
        
        # Generate time-series data
        print("\n📈 Generating time-series IoT sensor data...")
        
        # Generate IoT sensor readings
        iot_df = spark.range(1, 5001).select(col("id").alias("reading_id"))
        
        iot_df = iot_df \
            .withColumn("sensor_id", ((col("reading_id") - 1) % 50 + 1).cast("string")) \
            .withColumn("sensor_type",
                when(col("sensor_id").cast("int") <= 20, "temperature")
                .when(col("sensor_id").cast("int") <= 35, "humidity")
                .otherwise("pressure")) \
            .withColumn("value",
                when(col("sensor_type") == "temperature", rand() * 40 + 10)  # 10-50°C
                .when(col("sensor_type") == "humidity", rand() * 100)        # 0-100%
                .otherwise(rand() * 200 + 800)) \
            .withColumn("unit",
                when(col("sensor_type") == "temperature", "celsius")
                .when(col("sensor_type") == "humidity", "percent")
                .otherwise("hPa")) \
            .withColumn("timestamp", 
                expr("current_timestamp() - interval " + str(random.randint(1, 1440)) + " minutes")) \
            .withColumn("location",
                when(rand() < 0.33, "Building_A")
                .when(rand() < 0.66, "Building_B")
                .otherwise("Building_C")) \
            .withColumn("ingested_at", current_timestamp()) \
            .withColumn("data_source", lit("iot_sensor_network")) \
            .withColumn("file_format", lit("GENERATED"))
        
        # Write IoT data to MinIO
        iot_output_path = "s3a://test-bucket/raw-data/iot-sensors/"
        print(f"💾 Writing IoT data to MinIO: {iot_output_path}")
        
        iot_df.write.mode("overwrite").parquet(iot_output_path)
        print("✅ IoT data successfully written to MinIO!")
        
        print("IoT sensor statistics:")
        iot_df.groupBy("sensor_type", "location").agg(
            {"value": "avg", "value": "min", "value": "max"}
        ).show()
        
        # Verify writes
        print("🔍 Verifying all data in MinIO...")
        sales_read_back = spark.read.parquet(output_path)
        iot_read_back = spark.read.parquet(iot_output_path)
        
        print(f"✅ Sales data verification: {sales_read_back.count()} records")
        print(f"✅ IoT data verification: {iot_read_back.count()} records")
        
        print("\n🎉 Generated Data Ingestion completed successfully!")
        
    except Exception as e:
        print(f"❌ Error during generated data ingestion: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        spark.stop()
        print("🔄 Spark session stopped")

if __name__ == "__main__":
    main()