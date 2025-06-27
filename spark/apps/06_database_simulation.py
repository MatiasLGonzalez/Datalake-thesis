"""
Database Simulation Ingestion Example
=====================================

This script simulates database table ingestion into MinIO using Spark.
In a real scenario, you would connect to actual databases using JDBC.

Note: For real database connections, you would need:
- Database JDBC drivers (e.g., postgresql.jar, mysql-connector.jar)
- Database connection details
- Proper authentication

Usage:
    docker exec spark-master /opt/spark/bin/spark-submit \
        --jars /opt/spark/jars-extra/hadoop-aws-3.3.4.jar,/opt/spark/jars-extra/aws-java-sdk-bundle-1.11.1026.jar \
        /opt/spark/apps/06_database_simulation.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, col, date_format, year, month, dayofmonth
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType
import os

def create_spark_session():
    """Create Spark session with MinIO configuration"""
    # Get credentials from environment variables
    minio_access_key = os.getenv("MINIO_ROOT_USER", "minioadmin")
    minio_secret_key = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123")
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    
    return SparkSession.builder \
        .appName("Database_Simulation_Ingestion") \
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()

def simulate_users_table(spark):
    """Simulate a users table from a relational database"""
    users_data = [
        (1, "john_doe", "John", "Doe", "john.doe@email.com", "2023-01-15", True, "premium"),
        (2, "jane_smith", "Jane", "Smith", "jane.smith@email.com", "2023-02-20", True, "standard"),
        (3, "carlos_garcia", "Carlos", "Garcia", "carlos.garcia@email.com", "2023-03-10", False, "premium"),
        (4, "marie_dubois", "Marie", "Dubois", "marie.dubois@email.com", "2023-04-05", True, "standard"),
        (5, "hiroshi_tanaka", "Hiroshi", "Tanaka", "hiroshi.tanaka@email.com", "2023-05-12", True, "premium"),
        (6, "anna_mueller", "Anna", "Mueller", "anna.mueller@email.com", "2023-06-08", False, "standard"),
        (7, "roberto_silva", "Roberto", "Silva", "roberto.silva@email.com", "2023-07-14", True, "premium"),
        (8, "priya_sharma", "Priya", "Sharma", "priya.sharma@email.com", "2023-08-21", True, "standard"),
        (9, "ahmed_hassan", "Ahmed", "Hassan", "ahmed.hassan@email.com", "2023-09-18", True, "premium"),
        (10, "elena_popov", "Elena", "Popov", "elena.popov@email.com", "2023-10-25", False, "standard")
    ]
    
    users_schema = StructType([
        StructField("user_id", IntegerType(), True),
        StructField("username", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("is_active", StringType(), True),  # Simulating boolean as string
        StructField("subscription_tier", StringType(), True)
    ])
    
    return spark.createDataFrame(users_data, users_schema)

def simulate_orders_table(spark):
    """Simulate an orders table from a relational database"""
    orders_data = [
        (1001, 1, "2023-11-01 10:30:00", 999.99, "shipped", "credit_card"),
        (1002, 2, "2023-11-02 14:15:00", 89.50, "delivered", "debit_card"),
        (1003, 3, "2023-11-03 09:45:00", 120.75, "shipped", "paypal"),
        (1004, 1, "2023-11-04 16:20:00", 199.99, "processing", "credit_card"),
        (1005, 4, "2023-11-05 11:10:00", 45.30, "cancelled", "bank_transfer"),
        (1006, 5, "2023-11-06 13:25:00", 299.99, "delivered", "credit_card"),
        (1007, 2, "2023-11-07 08:40:00", 75.25, "shipped", "paypal"),
        (1008, 6, "2023-11-08 15:55:00", 150.00, "delivered", "debit_card"),
        (1009, 7, "2023-11-09 12:20:00", 89.99, "processing", "credit_card"),
        (1010, 8, "2023-11-10 17:30:00", 245.50, "shipped", "bank_transfer")
    ]
    
    orders_schema = StructType([
        StructField("order_id", IntegerType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("order_date", StringType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("status", StringType(), True),
        StructField("payment_method", StringType(), True)
    ])
    
    return spark.createDataFrame(orders_data, orders_schema)

def simulate_products_table(spark):
    """Simulate a products table from a relational database"""
    products_data = [
        (101, "Laptop Pro", "Electronics", 999.99, 25, True),
        (102, "Coffee Maker", "Kitchen", 89.50, 50, True),
        (103, "Running Shoes", "Sports", 120.75, 100, True),
        (104, "Wireless Headphones", "Electronics", 199.99, 30, True),
        (105, "Recipe Book", "Books", 45.30, 75, False),
        (106, "Yoga Mat", "Sports", 29.99, 200, True),
        (107, "Smart Watch", "Electronics", 299.99, 15, True),
        (108, "Water Bottle", "Sports", 19.99, 150, True),
        (109, "Desk Lamp", "Home", 89.99, 40, True),
        (110, "Bluetooth Speaker", "Electronics", 129.99, 60, True)
    ]
    
    products_schema = StructType([
        StructField("product_id", IntegerType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("stock_quantity", IntegerType(), True),
        StructField("is_available", StringType(), True)  # Simulating boolean as string
    ])
    
    return spark.createDataFrame(products_data, products_schema)

def main():
    print("🚀 Starting Database Simulation Ingestion Example")
    print("=" * 60)
    print("📝 Note: This simulates database table ingestion.")
    print("📝 For real databases, add JDBC drivers and connection details.")
    print("=" * 60)
    
    # Initialize Spark
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Simulate database tables
        print("🏗️  Simulating database tables...")
        
        users_df = simulate_users_table(spark)
        orders_df = simulate_orders_table(spark)
        products_df = simulate_products_table(spark)
        
        print("✅ Database tables simulated successfully!")
        
        # Show table schemas and sample data
        print("\n📋 USERS Table:")
        users_df.printSchema()
        users_df.show(5)
        
        print("\n📋 ORDERS Table:")
        orders_df.printSchema()
        orders_df.show(5)
        
        print("\n📋 PRODUCTS Table:")
        products_df.printSchema()
        products_df.show(5)
        
        # Add metadata for data lake ingestion
        current_ts = current_timestamp()
        ingestion_batch = lit("batch_2023_11_10")
        
        # Process users table
        users_processed = users_df \
            .withColumn("ingested_at", current_ts) \
            .withColumn("source_system", lit("user_management_db")) \
            .withColumn("table_name", lit("users")) \
            .withColumn("ingestion_batch", ingestion_batch)
        
        # Process orders table
        orders_processed = orders_df \
            .withColumn("ingested_at", current_ts) \
            .withColumn("source_system", lit("order_management_db")) \
            .withColumn("table_name", lit("orders")) \
            .withColumn("ingestion_batch", ingestion_batch) \
            .withColumn("order_year", year(col("order_date"))) \
            .withColumn("order_month", month(col("order_date")))
        
        # Process products table
        products_processed = products_df \
            .withColumn("ingested_at", current_ts) \
            .withColumn("source_system", lit("inventory_db")) \
            .withColumn("table_name", lit("products")) \
            .withColumn("ingestion_batch", ingestion_batch)
        
        # Write to MinIO with partitioning
        print("💾 Writing database tables to MinIO...")
        
        # Users table
        users_path = "s3a://test-bucket/database-tables/users/"
        users_processed.write.mode("overwrite").parquet(users_path)
        print(f"✅ Users table written to: {users_path}")
        
        # Orders table (partitioned by year and month)
        orders_path = "s3a://test-bucket/database-tables/orders/"
        orders_processed.write \
            .mode("overwrite") \
            .partitionBy("order_year", "order_month") \
            .parquet(orders_path)
        print(f"✅ Orders table written to: {orders_path}")
        
        # Products table
        products_path = "s3a://test-bucket/database-tables/products/"
        products_processed.write.mode("overwrite").parquet(products_path)
        print(f"✅ Products table written to: {products_path}")
        
        # Perform some database-style analytics
        print("\n📈 Database Analytics:")
        
        # Join orders with users
        print("Orders by subscription tier:")
        user_orders = orders_df.join(users_df, "user_id") \
            .groupBy("subscription_tier") \
            .agg({"total_amount": "sum", "order_id": "count"}) \
            .show()
        
        # Product categories analysis
        print("Product availability by category:")
        products_df.groupBy("category", "is_available").count().show()
        
        # Order status distribution
        print("Order status distribution:")
        orders_df.groupBy("status").count().show()
        
        # Example of incremental loading simulation
        print("\n🔄 Simulating incremental data loading...")
        print("📝 In production, you would:")
        print("   1. Use watermark columns (like updated_at)")
        print("   2. Track last ingestion timestamp")
        print("   3. Query only new/changed records")
        print("   4. Merge with existing data")
        
        # Verify data ingestion
        print("\n🔍 Verifying database table ingestion...")
        users_read = spark.read.parquet(users_path)
        orders_read = spark.read.parquet(orders_path)
        products_read = spark.read.parquet(products_path)
        
        print(f"✅ Users verification: {users_read.count()} records")
        print(f"✅ Orders verification: {orders_read.count()} records")
        print(f"✅ Products verification: {products_read.count()} records")
        
        print("\n📝 Real Database Connection Example:")
        print("""
# For real PostgreSQL connection:
users_df = spark.read \\
    .format("jdbc") \\
    .option("url", "jdbc:postgresql://localhost:5432/mydb") \\
    .option("dbtable", "users") \\
    .option("user", "username") \\
    .option("password", "password") \\
    .option("driver", "org.postgresql.Driver") \\
    .load()

# For MySQL:
orders_df = spark.read \\
    .format("jdbc") \\
    .option("url", "jdbc:mysql://localhost:3306/mydb") \\
    .option("dbtable", "orders") \\
    .option("user", "username") \\
    .option("password", "password") \\
    .option("driver", "com.mysql.cj.jdbc.Driver") \\
    .load()
        """)
        
        print("\n🎉 Database Simulation Ingestion completed successfully!")
        
    except Exception as e:
        print(f"❌ Error during database simulation: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        spark.stop()
        print("🔄 Spark session stopped")

if __name__ == "__main__":
    main()