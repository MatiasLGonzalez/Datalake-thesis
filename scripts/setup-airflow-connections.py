#!/usr/bin/env python3
"""
Script to set up Airflow connections for MinIO integration
"""

from airflow.models import Connection
from airflow.utils.db import provide_session
import os
import json

@provide_session
def create_minio_connection(session=None):
    """Create MinIO S3 connection in Airflow"""
    
    # Connection details for MinIO
    conn_id = 'my_s3_conn'
    conn_type = 's3'
    host = 'minio'  # Docker service name
    login = os.getenv('MINIO_ROOT_USER', 'admin')
    password = os.getenv('MINIO_ROOT_PASSWORD', 'password')
    port = 9000
    
    # Extra configuration for MinIO (must be JSON string)
    extra = json.dumps({
        "aws_access_key_id": login,
        "aws_secret_access_key": password,
        "endpoint_url": f"http://{host}:{port}",
        "region_name": "us-east-1"
    })
    
    # Check if connection already exists
    existing_conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()
    
    if existing_conn:
        print(f"Connection '{conn_id}' already exists. Updating...")
        existing_conn.conn_type = conn_type
        existing_conn.host = host
        existing_conn.login = login
        existing_conn.password = password
        existing_conn.port = port
        existing_conn.set_extra(extra)
    else:
        print(f"Creating new connection '{conn_id}'...")
        new_conn = Connection(
            conn_id=conn_id,
            conn_type=conn_type,
            host=host,
            login=login,
            password=password,
            port=port
        )
        new_conn.set_extra(extra)
        session.add(new_conn)
    
    session.commit()
    print(f"MinIO S3 connection '{conn_id}' configured successfully!")

@provide_session
def create_spark_connection(session=None):
    """Create Spark connection in Airflow"""
    
    # Connection details for Spark
    conn_id = 'spark_default'
    conn_type = 'spark'
    host = 'spark://spark-master:7077'  # Spark Master URL
    port = None  # Port is included in host URL
    
    # Extra configuration for Spark (must be JSON string)
    extra = json.dumps({
        "deploy-mode": "client",
        "spark_binary": "spark-submit"
    })
    
    # Check if connection already exists
    existing_conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()
    
    if existing_conn:
        print(f"Connection '{conn_id}' already exists. Updating...")
        existing_conn.conn_type = conn_type
        existing_conn.host = host
        existing_conn.port = port
        existing_conn.set_extra(extra)
    else:
        print(f"Creating new connection '{conn_id}'...")
        new_conn = Connection(
            conn_id=conn_id,
            conn_type=conn_type,
            host=host,
            port=port
        )
        new_conn.set_extra(extra)
        session.add(new_conn)
    
    session.commit()
    print(f"Spark connection '{conn_id}' configured successfully!")

if __name__ == "__main__":
    create_minio_connection()
    create_spark_connection()