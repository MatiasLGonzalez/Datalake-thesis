import os
from pyspark.sql import SparkSession
from spark.apps.enums import DataZone

def transient_to_raw():
    # Configure Spark with environment variables directly in Python
    spark = SparkSession.builder \
        .appName("UNA Data Lake - Transient to Raw") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.endpoint", os.environ.get("MINIO_ENDPOINT", "http://minio:9000")) \
        .config("spark.hadoop.fs.s3a.access.key", os.environ.get("MINIO_ROOT_USER", "minioadmin")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin123")) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    

    # Get list of files in the transient bucket
    input_path = "s3a://university-data-lake/fpuna/academic-department/transient/"
    print(f"Input path: {input_path}")
    if not input_path:
        print("Input path is not defined. Exiting.")
        spark.stop()
        return

    file_name_list = spark.sparkContext.wholeTextFiles(input_path).map(lambda x: x[0]).collect()

    if not file_name_list:
        print("No files found in the transient bucket.")
        spark.stop()
        return
    
    for file_name in file_name_list:
        try:
            base_name = os.path.basename(file_name)
            print(f"Processing file: {base_name}")
            
            # Read input file with format detection
            if file_name.endswith('.parquet'):
                df = spark.read.parquet(file_name)
                source_format = "parquet"
            elif file_name.endswith('.json'):
                df = spark.read.json(file_name)
                source_format = "json"
            elif file_name.endswith('.csv'):
                df = spark.read.format("csv").option("header", "true").load(file_name)
                source_format = "csv"
            else:
                print(f"Unsupported file format for {file_name}. Skipping.")
                continue

            # Do some basic validations
            # 
            # Do some security checks if needed

            # Write to raw bucket
            output_path = f"s3a://university-data-lake/fpuna/academic-department/raw/{base_name}"
            
            df.write.mode("overwrite").parquet(output_path)
            
            record_count = df.count()
            print(f"Successfully processed and saved: {base_name} to raw bucket.")
            print(f"  - Source format: {source_format}")
            print(f"  - Output format: parquet")
            print(f"  - Records processed: {record_count}")
        
            
            print(f"Successfully processed and removed: {base_name}")
            
        except Exception as e:
            print(f"Failed to process {os.path.basename(file_name)}: {e}")

    print("Files processing completed.")
    spark.stop()

if __name__ == "__main__":
    transient_to_raw()
