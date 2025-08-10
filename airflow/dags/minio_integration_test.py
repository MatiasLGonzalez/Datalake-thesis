"""
MinIO Integration Test DAG

This DAG demonstrates the integration between Apache Airflow and MinIO
by performing various S3-compatible operations on MinIO buckets.
"""

import json
from datetime import datetime, timedelta
import pendulum

from airflow.sdk import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# DAG configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='minio_integration_test',
    default_args=default_args,
    description='Test DAG for MinIO integration with Airflow',
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=['minio', 'test', 'integration'],
)
def minio_integration_test_dag():
    """
    ### MinIO Integration Test DAG
    
    This DAG tests various MinIO operations:
    1. Create sample data
    2. Upload data to MinIO
    3. List objects in bucket
    4. Download data from MinIO
    5. Process and transform data
    6. Upload processed data back to MinIO
    """
    
    @task()
    def create_sample_data():
        """Create sample JSON data for testing"""
        import tempfile
        import os
        
        sample_data = {
            "students": [
                {"id": 1, "name": "Alice", "major": "Computer Science", "gpa": 3.8},
                {"id": 2, "name": "Bob", "major": "Mathematics", "gpa": 3.6},
                {"id": 3, "name": "Carol", "major": "Physics", "gpa": 3.9},
                {"id": 4, "name": "David", "major": "Engineering", "gpa": 3.7},
            ],
            "timestamp": datetime.now().isoformat(),
            "source": "airflow_test"
        }
        
        # Create temporary file
        temp_dir = tempfile.mkdtemp()
        file_path = os.path.join(temp_dir, "sample_students.json")
        
        with open(file_path, 'w') as f:
            json.dump(sample_data, f, indent=2)
        
        return file_path
    
    @task()
    def upload_to_minio(file_path: str):
        """Upload file to MinIO using S3Hook"""
        s3_hook = S3Hook(aws_conn_id='my_s3_conn')
        
        bucket_name = 'university-data-lake'
        s3_key = 'fpuna/academic-department/raw/students/sample_students.json'
        
        try:
            s3_hook.load_file(
                filename=file_path,
                key=s3_key,
                bucket_name=bucket_name,
                replace=True
            )
            print(f"Successfully uploaded {file_path} to s3://{bucket_name}/{s3_key}")
            return s3_key
        except Exception as e:
            print(f"Error uploading file: {str(e)}")
            raise
    
    @task()
    def list_minio_objects():
        """List objects in MinIO bucket"""
        s3_hook = S3Hook(aws_conn_id='my_s3_conn')
        
        bucket_name = 'university-data-lake'
        prefix = 'fpuna/academic-department/raw/students/'
        
        try:
            objects = s3_hook.list_keys(bucket_name=bucket_name, prefix=prefix)
            print(f"Objects found in s3://{bucket_name}/{prefix}:")
            for obj in objects or []:
                print(f"  - {obj}")
            return objects
        except Exception as e:
            print(f"Error listing objects: {str(e)}")
            raise
    
    @task()
    def download_and_process(s3_key: str):
        """Download file from MinIO and process it"""
        s3_hook = S3Hook(aws_conn_id='my_s3_conn')
        
        bucket_name = 'university-data-lake'
        
        try:
            # Download file content
            file_content = s3_hook.read_key(
                key=s3_key,
                bucket_name=bucket_name
            )
            
            # Parse JSON data
            data = json.loads(file_content)
            students = data.get('students', [])
            
            # Process data - calculate average GPA
            total_gpa = sum(student['gpa'] for student in students)
            avg_gpa = total_gpa / len(students) if students else 0
            
            # Create processed data
            processed_data = {
                "summary": {
                    "total_students": len(students),
                    "average_gpa": round(avg_gpa, 2),
                    "highest_gpa": max((s['gpa'] for s in students), default=0),
                    "lowest_gpa": min((s['gpa'] for s in students), default=0)
                },
                "majors": {}
            }
            
            # Count students by major
            for student in students:
                major = student['major']
                if major not in processed_data['majors']:
                    processed_data['majors'][major] = 0
                processed_data['majors'][major] += 1
            
            processed_data["processed_at"] = datetime.now().isoformat()
            
            print("Processed student data:")
            print(json.dumps(processed_data, indent=2))
            
            return processed_data
            
        except Exception as e:
            print(f"Error processing data: {str(e)}")
            raise
    
    @task()
    def upload_processed_data(processed_data: dict):
        """Upload processed data back to MinIO"""
        s3_hook = S3Hook(aws_conn_id='my_s3_conn')
        
        bucket_name = 'university-data-lake'
        s3_key = 'fpuna/academic-department/trusted/students/student_summary.json'
        
        try:
            # Convert to JSON string
            json_string = json.dumps(processed_data, indent=2)
            
            # Upload to MinIO
            s3_hook.load_string(
                string_data=json_string,
                key=s3_key,
                bucket_name=bucket_name,
                replace=True
            )
            
            print(f"Successfully uploaded processed data to s3://{bucket_name}/{s3_key}")
            return s3_key
            
        except Exception as e:
            print(f"Error uploading processed data: {str(e)}")
            raise
    
    @task()
    def data_quality_check():
        """Perform basic data quality checks"""
        s3_hook = S3Hook(aws_conn_id='my_s3_conn')
        
        bucket_name = 'university-data-lake'
        
        # Check both raw and processed data exist
        raw_key = 'fpuna/academic-department/raw/students/sample_students.json'
        processed_key = 'fpuna/academic-department/trusted/students/student_summary.json'
        
        try:
            # Check raw data exists
            raw_exists = s3_hook.check_for_key(key=raw_key, bucket_name=bucket_name)
            processed_exists = s3_hook.check_for_key(key=processed_key, bucket_name=bucket_name)
            
            if raw_exists and processed_exists:
                print("✅ Data quality check passed: Both raw and processed data exist")
                return True
            else:
                print(f"❌ Data quality check failed: raw_exists={raw_exists}, processed_exists={processed_exists}")
                return False
                
        except Exception as e:
            print(f"Error in data quality check: {str(e)}")
            raise
    
    # Define task dependencies
    sample_file = create_sample_data()
    uploaded_key = upload_to_minio(sample_file)
    objects_list = list_minio_objects()
    processed_data = download_and_process(uploaded_key)
    final_key = upload_processed_data(processed_data)
    quality_check = data_quality_check()
    
    # Set up task dependencies
    sample_file >> uploaded_key >> [objects_list, processed_data]
    processed_data >> final_key >> quality_check

# Create the DAG
minio_integration_test_dag()