"""
Data Lake Ingestion Service
==========================

Abstracted service for data lake operations including:
- Reading from various sources
- Writing to different zones (transient, bronze, silver, gold)
- Data quality validation
- Security and compliance measures
- Audit logging
"""

from pyspark.sql.types import *
import os
import json
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class DataLakeZones:
    """Constants for data lake zones"""
    TRANSIENT = "transient-landing-zone"
    RAW = "raw-zone"
    TRUSTED = "trusted-zone"
    REFINED = "refined-zone"
    SANDBOX = "sandbox"

class IngestionService:
    """
    Comprehensive data lake ingestion service
    """
    
    def __init__(self, spark_session, bucket_name="test-bucket"):
        self.spark = spark_session
        self.bucket_name = bucket_name

    
    def load_to_bucket(self, zone, entity_name, df, format="csv", mode="overwrite", options=None):
        """
        Load DataFrame to specified zone in data lake
        
        Args:
            zone: Target zone (transient, bronze, silver, gold)
            entity_name: Entity name (customers, students, etc.)
            df: DataFrame to write
            format: Output format (csv, json)
            mode: Write mode (overwrite, append)
            options: Additional write options
        
        Returns:
            str: Output path where data was written
        """
        output_path = f"s3a://{self.bucket_name}/{zone}/{entity_name}/"
        
        try:
            writer = df.write.mode(mode)
            
            # Apply format-specific options
            if options:
                for key, value in options.items():
                    writer = writer.option(key, value)
            
            # Write based on format
            if format.lower() == "csv":
                writer.option("header", "true").csv(output_path)
            elif format.lower() == "json":
                writer.json(output_path)
            else:
                raise ValueError(f"Unsupported format: {format}")
            
            record_count = df.count()
        

            print(f"✓ Successfully wrote {record_count} records to {output_path}")
            return output_path
            
        except Exception as e:
            print(f"❌ Failed to write data to {output_path}: {e}")
            raise
    
    def read_from_bucket(self, zone, entity_name, format="parquet", options=None):
        """
        Read DataFrame from specified zone in data lake
        
        Args:
            zone: Source zone
            entity_name: Entity name
            format: File format
            options: Additional read options
        
        Returns:
            DataFrame: Loaded data
        """
        input_path = f"s3a://{self.bucket_name}/{zone}/{entity_name}/"
        
        try:
            reader = self.spark.read
            
            # Apply format-specific options
            if options:
                for key, value in options.items():
                    reader = reader.option(key, value)
            
            # Read based on format
            if format.lower() == "csv":
                df = reader.option("header", "true").option("inferSchema", "true").csv(input_path)
            elif format.lower() == "json":
                df = reader.json(input_path)
            else:
                raise ValueError(f"Unsupported format: {format}")
            
            record_count = df.count()
            
            print(f"✓ Successfully read {record_count} records from {input_path}")
            return df
            
        except Exception as e:
            print(f"❌ Failed to read data from {input_path}: {e}")
            raise
    
    def read_from_local(self, file_path, format="csv", options=None):
        """
        Read data from local file system
        
        Args:
            file_path: Local file path
            format: File format
            options: Additional read options
        
        Returns:
            DataFrame: Loaded data
        """
        try:
            reader = self.spark.read
            
            # Apply options
            if options:
                for key, value in options.items():
                    reader = reader.option(key, value)
            
            # Read based on format
            if format.lower() == "csv":
                df = reader.option("header", "true").option("inferSchema", "true").csv(f"file://{file_path}")
            elif format.lower() == "json":
                df = reader.json(f"file://{file_path}")
            elif format.lower() == "parquet":
                df = reader.parquet(f"file://{file_path}")
            else:
                raise ValueError(f"Unsupported format: {format}")
            
            record_count = df.count()
        
            print(f"✓ Successfully read {record_count} records from {file_path}")
            return df
            
        except Exception as e:
            print(f"❌ Failed to read local file {file_path}: {e}")
            raise
    
    # ==================== ZONE-SPECIFIC OPERATIONS ====================
    
    def ingest_to_transient_zone(self, df, entity_name):
        """
        Ingest data to transient landing zone with basic processing
        
        Args:
            df: Source DataFrame
            entity_name: Entity name
        
        Returns:
            str: Output path
        """
        return self.load_to_bucket(DataLakeZones.TRANSIENT, entity_name, df)
    
    
    
    