#!/bin/bash

# Wait for MinIO
sleep 15

# Set alias
mc alias set myminio http://minio:9000 $MINIO_ROOT_USER $MINIO_ROOT_PASSWORD

# Create buckets
mc mb myminio/university-data-lake --ignore-existing
mc mb myminio/university-data-lake/fpuna/academic-department/transient/ --ignore-existing
mc mb myminio/university-data-lake/fpuna/academic-department/raw/ --ignore-existing
mc mb myminio/university-data-lake/fpuna/academic-department/trusted/ --ignore-existing
mc mb myminio/university-data-lake/fpuna/academic-department/refined/ --ignore-existing
mc mb myminio/university-data-lake/fpuna/academic-department/sandbox/ --ignore-existing
mc mb myminio/university-data-lake/fpuna/academic-department/quarantine/ --ignore-existing

# Set bucket policies - only transient allows uploads
mc anonymous set upload myminio/university-data-lake/fpuna/academic-department/transient/
mc anonymous set download myminio/university-data-lake/fpuna/academic-department/raw/
mc anonymous set download myminio/university-data-lake/fpuna/academic-department/trusted/
mc anonymous set download myminio/university-data-lake/fpuna/academic-department/refined/
mc anonymous set download myminio/university-data-lake/fpuna/academic-department/sandbox/
mc anonymous set none myminio/university-data-lake/fpuna/academic-department/quarantine/

echo "Buckets created with access policies applied successfully"