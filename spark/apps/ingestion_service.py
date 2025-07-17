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
from validation_service import ValidationService
from security_service import SecurityService
from compliance_service import ComplianceService

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
        self.validation_service = ValidationService(spark_session)
        self.security_service = SecurityService(spark_session)
        self.compliance_service = ComplianceService(spark_session)

    
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
        Ingest data to transient landing zone with comprehensive processing
        
        Args:
            df: Source DataFrame
            entity_name: Entity name
        
        Returns:
            str: Output path
        """
        print(f"🚀 Starting transient zone ingestion for {entity_name}")
        
        # 1. Data Quality Validation
        print("1. Performing data quality validation...")
        validation_results = self.validation_service.validate_data(df, entity_name)
        
        if not validation_results.get("success", False):
            raise ValueError(f"Data validation failed for {entity_name}: {validation_results}")
        
        # 2. Security Transformations (PII masking, etc.)
        print("2. Applying security measures...")
        secured_df = self.security_service.apply_security_measures(df, entity_name)
        
        # 3. Compliance Checks (FERPA, GDPR)
        print("3. Verifying compliance...")
        compliance_results = self.compliance_service.verify_compliance(secured_df, entity_name)
        
        if not compliance_results.get("overall_compliant", False):
            raise ValueError(f"Compliance verification failed for {entity_name}: {compliance_results}")
        
        # 4. Store in transient zone (temporary)
        print("4. Storing to transient zone...")
        return self.load_to_bucket(DataLakeZones.TRANSIENT, entity_name, secured_df)
    
    def ingest_to_raw_zone(self, df, entity_name):
        """
        Ingest data to raw zone with minimal processing
        
        Args:
            df: Source DataFrame
            entity_name: Entity name
        
        Returns:
            str: Output path
        """
        # Load to Raw Zone (Data is technically clean)
        return self.load_to_bucket(DataLakeZones.RAW, entity_name, df)
    
    def cleanup(self):
        """Cleanup all services"""
        print("🧹 Cleaning up IngestionService...")
        self.validation_service.cleanup()
        self.security_service.cleanup()
        self.compliance_service.cleanup()
        print("✅ IngestionService cleanup completed")