from datetime import datetime, timezone
import boto3

def get_latest_s3_file_path_with_2_prefix(bucket_name, prefix, prefix2, **kwargs):
    s3 = boto3.client('s3')
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    response2 = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix2)

    # Define the time range for the current date
    now = datetime.now(timezone.utc)
    start_time = datetime(now.year, now.month, now.day, 0, 0, tzinfo=timezone.utc)
    end_time = datetime(now.year, now.month, now.day, 7, 0, tzinfo=timezone.utc)

    # Get the most recent file based on LastModified timestamp
    latest_file = None
    latest_timestamp = datetime(1970, 1, 1, tzinfo=timezone.utc)  # Initialize with UTC timezone

    for obj in response.get('Contents', []):
        obj_last_modified = obj['LastModified'].replace(tzinfo=timezone.utc)  # Make the timestamp timezone-aware
        if not obj['Key'].endswith('/') and (latest_file is None or obj_last_modified > latest_timestamp):
#            if start_time <= obj_last_modified <= end_time and (latest_file is None or obj_last_modified > latest_timestamp):
            if obj['Key'].endswith('.csv'): 
                latest_file = obj['Key']
                latest_timestamp = obj_last_modified

    latest_file2 = None
    latest_timestamp2 = datetime(1970, 1, 1, tzinfo=timezone.utc)  # Initialize with UTC timezone

    for obj in response2.get('Contents', []):
        obj_last_modified2 = obj['LastModified'].replace(tzinfo=timezone.utc)  # Make the timestamp timezone-aware
        if not obj['Key'].endswith('/') and (latest_file is None or obj_last_modified2 > latest_timestamp2) :
#            if start_time <= obj_last_modified2 <= end_time and (latest_file2 is None or obj_last_modified2 > latest_timestamp2):
            if obj['Key'].endswith('.csv'):
                latest_file2 = obj['Key']
                latest_timestamp2 = obj_last_modified2

    if latest_file:
        s3_path = f's3a://{bucket_name}/{latest_file}'
        # Push the s3_path to XCom
        kwargs['ti'].xcom_push(key='s3_path', value=s3_path)
    else:
        kwargs['ti'].xcom_push(key='s3_path', value="None")

    if latest_file2:
        s3_path2 = f's3a://{bucket_name}/{latest_file2}'
        kwargs['ti'].xcom_push(key='s3_path2', value=s3_path2)
    else:
        kwargs['ti'].xcom_push(key='s3_path2', value="None")
