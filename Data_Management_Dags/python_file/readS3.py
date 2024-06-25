import boto3
from datetime import datetime
from collections import defaultdict

def get_oldest_s3_file_path(bucket_name, prefix, **kwargs):
    s3 = boto3.client('s3')

    # List objects within the source prefix
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    # Dictionary to hold files grouped by date
    date_files = defaultdict(list)

    # Iterate through the files and group them by date
    for obj in response.get('Contents', []):
        # Check if the object is a file (not a directory) and is a .csv file
        if not obj['Key'].endswith('/') and obj['Key'].endswith('.csv'):
            # Extract the last modified date of the object
            last_modified = obj['LastModified']
            # Group files by the date part only (ignoring time)
            date_key = last_modified.date()
            date_files[date_key].append(f's3://{bucket_name}/{obj["Key"]}')

    # Find the oldest date with files
    if date_files:
        oldest_date = min(date_files.keys())
        oldest_files = date_files[oldest_date]
    else:
        oldest_files = None
    print(oldest_files)
    # Push the list of file paths to XCom
    if oldest_files:
        kwargs['ti'].xcom_push(key='s3_paths', value=oldest_files)
    else:
        kwargs['ti'].xcom_push(key='s3_paths', value=None)

