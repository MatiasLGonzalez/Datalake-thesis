# Thesis Data Lake Project

A modern, scalable data lake implementation using Apache Spark and MinIO for data storage, processing, and analytics. This project demonstrates practical data engineering techniques with containerized deployment and real-world data ingestion examples.

## Architecture Overview

This data lake implements a modern architecture for data ingestion, processing, and storage:

```
Data Sources        Processing         Storage Zones
┌─────────────┐    ┌─────────────┐    ┌─────────────────┐
│ CSV Files   │───▶│             │───▶│                 │
│ JSON Files  │───▶│ Apache      │───▶│ Data Lake       │
│ Log Files   │───▶│ Spark       │───▶│ Storage         │
│ Databases   │───▶│ 3.5.0       │───▶│ (MinIO S3)      │
│ APIs        │───▶│             │───▶│                 │
└─────────────┘    └─────────────┘    └─────────────────┘
```

### Data Lake Storage

**Unified Storage** - Flexible data storage with multiple format support:
- Stores data in various formats (CSV, JSON, Parquet, text, etc.)
- Maintains data lineage and processing metadata
- Optimized for both batch processing and analytical queries
- Schema-flexible with support for structured and semi-structured data

### Components

- **Apache Spark 3.5.0**: Distributed data processing engine
- **MinIO**: S3-compatible object storage for data lake storage
- **Docker Compose**: Container orchestration for easy deployment
- **PySpark**: Python API for Spark applications

## Project Structure

```
thesis_data_lake/
├── docker-compose.yml          # Container orchestration configuration
├── .env                        # Environment variables for credentials
├── README.md                   # Project documentation
├── spark/
│   ├── apps/                   # Data ingestion examples
│   │   ├── 01_csv_ingestion.py              # CSV file ingestion and processing
│   │   ├── 02_json_ingestion.py             # JSON file ingestion and processing
│   │   ├── 03_delimited_file_ingestion.py   # Pipe-delimited file processing
│   │   ├── 04_log_file_ingestion.py         # Log file parsing and structuring
│   │   ├── 05_generated_data_ingestion.py   # Synthetic data generation
│   │   └── 06_database_simulation.py        # Database table ingestion
│   ├── jars/                   # Required JAR dependencies
│   │   ├── hadoop-aws-3.3.4.jar              # Hadoop S3A filesystem
│   │   └── aws-java-sdk-bundle-1.11.1026.jar # AWS SDK for S3 operations
│   └── data/                   # Sample data files
│       ├── sample_customers.csv              # Customer data
│       ├── sample_transactions.json          # Transaction records
│       ├── sample_products.txt               # Product catalog (pipe-delimited)
│       └── sample_logs.log                   # Application logs
└── storage/
    └── minio/                  # MinIO data storage
        └── test-bucket/        # Primary data bucket for all ingested data
```

## Quick Start

### Prerequisites

- Docker Desktop installed and running
- At least 4GB RAM available for containers
- Git (for cloning the repository)

### 1. Clone and Setup

```bash
git clone <your-repository-url>
cd thesis_data_lake
```

### 2. Start the Data Lake

```bash
docker-compose up -d
```

This will start:
- **Spark Master** (Web UI: http://localhost:8080)
- **2 Spark Workers** (Web UIs: http://localhost:8081, http://localhost:8082)
- **MinIO Storage** (Console: http://localhost:9001)

### 3. Verify Installation

Run a simple ingestion example:

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --jars /opt/spark/jars-extra/hadoop-aws-3.3.4.jar,/opt/spark/jars-extra/aws-java-sdk-bundle-1.11.1026.jar \
  /opt/spark/apps/01_csv_ingestion.py
```

Expected output:
```
Writing customer data to MinIO: s3a://test-bucket/customers/
Data successfully ingested and stored!
Verification: 10 customer records processed
```

## Data Ingestion Examples

### 1. CSV File Ingestion

**File**: `01_csv_ingestion.py`  
**Source**: `sample_customers.csv`  
**Description**: Demonstrates CSV data ingestion with automatic schema detection and storage optimization

```bash
# Run CSV ingestion
docker exec spark-master /opt/spark/bin/spark-submit \
  --jars /opt/spark/jars-extra/hadoop-aws-3.3.4.jar,/opt/spark/jars-extra/aws-java-sdk-bundle-1.11.1026.jar \
  /opt/spark/apps/01_csv_ingestion.py
```

**Features**:
- Automatic CSV schema detection and validation
- Data quality checks and cleansing
- Metadata enrichment (ingestion timestamps, source tracking)
- **Sample Data**: 10 customer records with fields like customer_id, name, email, city, country

### 2. JSON File Ingestion

**File**: `02_json_ingestion.py`  
**Source**: `sample_transactions.json`  
**Description**: Demonstrates JSON data ingestion with nested data handling and schema inference

```bash
# Run JSON ingestion
docker exec spark-master /opt/spark/bin/spark-submit \
  --jars /opt/spark/jars-extra/hadoop-aws-3.3.4.jar,/opt/spark/jars-extra/aws-java-sdk-bundle-1.11.1026.jar \
  /opt/spark/apps/02_json_ingestion.py
```

**Features**:
- Automatic JSON schema inference and flattening
- Nested data structure handling
- Currency analysis and transaction status breakdown
- Data validation and error handling

### 3. Delimited File Ingestion

**File**: `03_delimited_file_ingestion.py`  
**Source**: `sample_products.txt`  
**Description**: Handles custom delimited files with flexible parsing and data transformation

```bash
# Run delimited file ingestion
docker exec spark-master /opt/spark/bin/spark-submit \
  --jars /opt/spark/jars-extra/hadoop-aws-3.3.4.jar,/opt/spark/jars-extra/aws-java-sdk-bundle-1.11.1026.jar \
  /opt/spark/apps/03_delimited_file_ingestion.py
```

**Features**:
- Custom delimiter handling (pipe-separated values)
- Data type inference and validation
- Product analytics by category, price and inventory analysis
- Error handling for malformed records

### 4. Log File Ingestion

**File**: `04_log_file_ingestion.py`  
**Source**: `sample_logs.log`  
**Description**: Parses unstructured application logs with regex-based extraction and structured output

```bash
# Run log file ingestion
docker exec spark-master /opt/spark/bin/spark-submit \
  --jars /opt/spark/jars-extra/hadoop-aws-3.3.4.jar,/opt/spark/jars-extra/aws-java-sdk-bundle-1.11.1026.jar \
  /opt/spark/apps/04_log_file_ingestion.py
```

**Features**:
- Regex-based log parsing and field extraction
- Timestamp parsing and standardization
- Log level classification and service identification
- Entity extraction (user IDs, transaction IDs, IP addresses)

### 5. Generated Data Ingestion

**File**: `05_generated_data_ingestion.py`  
**Description**: Creates synthetic datasets for testing and prototyping

```bash
# Run generated data ingestion
docker exec spark-master /opt/spark/bin/spark-submit \
  --jars /opt/spark/jars-extra/hadoop-aws-3.3.4.jar,/opt/spark/jars-extra/aws-java-sdk-bundle-1.11.1026.jar \
  /opt/spark/apps/05_generated_data_ingestion.py
```

**Generated Data**:
- **Sales Data**: 1,000 synthetic sales records
- **IoT Sensor Data**: 5,000 sensor readings (temperature, humidity, pressure)

### 6. Database Simulation

**File**: `06_database_simulation.py`  
**Description**: Simulates database table ingestion (Users, Orders, Products)

```bash
# Run database simulation
docker exec spark-master /opt/spark/bin/spark-submit \
  --jars /opt/spark/jars-extra/hadoop-aws-3.3.4.jar,/opt/spark/jars-extra/aws-java-sdk-bundle-1.11.1026.jar \
  /opt/spark/apps/06_database_simulation.py
```

**Features**:
- Simulates relational database tables
- Data partitioning strategies
- Includes real database connection examples (PostgreSQL, MySQL)

## Configuration

### Environment Variables

The `.env` file contains MinIO configuration:

```env
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin123
MINIO_ENDPOINT=http://minio:9000
```

### Spark Configuration

Standard configuration for Spark-MinIO integration is used across all examples using environment variables:

```python
import os

# Get configuration from environment variables
minio_access_key = os.getenv("MINIO_ROOT_USER", "minioadmin")
minio_secret_key = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123")
minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")

spark = SparkSession.builder \
    .appName("DataIngestionExample") \
    .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
    .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
    .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()
```

## Sample Data Overview

### Customer Data (`sample_customers.csv`)
- **Records**: 10 customers
- **Fields**: ID, name, email, age, city, country, registration date
- **Countries**: USA, UK, Spain, France, Japan, Germany, Brazil, India, Egypt, Russia

### Transaction Data (`sample_transactions.json`)
- **Records**: 5 transactions  
- **Fields**: transaction_id, customer_id, product, amount, currency, payment method, status
- **Total Value**: $1,456.53 across USD, EUR, GBP

### Product Data (`sample_products.txt`)
- **Records**: 10 products
- **Categories**: Electronics, Kitchen, Sports, Books, Furniture
- **Price Range**: $19.99 - $1,999.99

### Log Data (`sample_logs.log`)
- **Records**: 15 log entries
- **Services**: UserService, ProductService, PaymentService, etc.
- **Log Levels**: INFO, ERROR, WARN

## Monitoring and Management

### Spark Web UIs
- **Master UI**: http://localhost:8080 - Cluster overview and job monitoring
- **Worker UIs**: http://localhost:8081, http://localhost:8082 - Worker node details
- **Application UI**: http://localhost:4040 - Running application details (when jobs are active)

### MinIO Console
- **URL**: http://localhost:9001
- **Username**: `minioadmin`
- **Password**: `minioadmin123`
- **Features**: Browse ingested data, bucket management, metrics

### Container Management
```bash
# View all container logs
docker-compose logs

# View specific service logs
docker-compose logs spark-master
docker-compose logs minio

# Follow logs in real-time
docker-compose logs -f

# Check container status
docker-compose ps
```

## Adding Your Own Data Sources

### Creating New Ingestion Scripts

1. **Follow the naming pattern**: `XX_your_source_ingestion.py`
2. **Use the standard Spark configuration** from existing examples
3. **Add metadata columns**: `ingested_at`, `source_file`, `file_format`
4. **Include verification step**: Read back data from MinIO to confirm success

### Example Template

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
import os

def create_spark_session():
    # Get configuration from environment variables
    minio_access_key = os.getenv("MINIO_ROOT_USER", "minioadmin")
    minio_secret_key = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123")
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    
    return SparkSession.builder \
        .appName("Your_Data_Source_Ingestion") \
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()

def main():
    spark = create_spark_session()
    
    try:
        # Your data ingestion logic here
        df = spark.read.format("your_format").load("your_source")
        
        # Add metadata
        df_with_metadata = df \
            .withColumn("ingested_at", current_timestamp()) \
            .withColumn("source_file", lit("your_source_name")) \
            .withColumn("file_format", lit("YOUR_FORMAT"))
        
        # Write to MinIO
        df_with_metadata.write.mode("overwrite").parquet("s3a://test-bucket/your-data/")
        
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
```

## Troubleshooting

### Common Issues

#### 1. Container Connection Issues
```bash
# Check all containers are running
docker-compose ps

# Restart if needed
docker-compose restart
```

#### 2. File Not Found Errors
- Ensure data files are in the `spark/data/` directory
- Check file permissions and paths in scripts

#### 3. MinIO Access Issues
- Verify MinIO console access at http://localhost:9001
- Check bucket exists (create `test-bucket` if needed)
- Confirm credentials in `.env` file

#### 4. Memory Issues
```bash
# Increase worker memory in docker-compose.yml
environment:
  - SPARK_WORKER_MEMORY=4G
  - SPARK_WORKER_CORES=4
```

## Real Database Connections

For production use with real databases, add the appropriate JDBC drivers and modify connection strings:

### PostgreSQL Example
```python
# Add postgresql.jar to jars/ directory
df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/mydb") \
    .option("dbtable", "your_table") \
    .option("user", "username") \
    .option("password", "password") \
    .option("driver", "org.postgresql.Driver") \
    .load()
```

### MySQL Example
```python
# Add mysql-connector.jar to jars/ directory  
df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/mydb") \
    .option("dbtable", "your_table") \
    .option("user", "username") \
    .option("password", "password") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .load()
```

## Security Considerations

### Development Environment
- Default credentials for easy development
- All services on localhost only
- No encryption (development only)

### Production Recommendations
- Use strong, unique MinIO credentials
- Enable SSL/TLS for all communications
- Implement network security and access controls
- Use secrets management for credentials
- Enable MinIO server-side encryption

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/new-ingestion-source`)
3. Add your ingestion example following the established patterns
4. Test your changes with the provided infrastructure
5. Commit your changes (`git commit -am 'Add new ingestion source'`)
6. Push to the branch (`git push origin feature/new-ingestion-source`)
7. Create a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Apache Spark community for excellent documentation
- MinIO team for providing S3-compatible storage
- Docker community for containerization best practices

## Support

For questions or issues:
1. Check the troubleshooting section above
2. Review container logs: `docker-compose logs`
3. Verify data files are in the correct locations
4. Test with the provided sample data first
5. Open an issue in the project repository

---

**Built for practical data engineering learning**