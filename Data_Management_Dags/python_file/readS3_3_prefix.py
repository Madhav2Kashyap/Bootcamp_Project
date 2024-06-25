from datetime import datetime, timezone
import boto3


def get_latest_s3_file_path_with_3_prefix(bucket_name, prefix, prefix2, prefix3, **kwargs):
    s3 = boto3.client('s3')
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    response2 = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix2)
    response3 = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix3)


    # Get the most recent file based on LastModified timestamp
    latest_file = None
    latest_timestamp = datetime(1970, 1, 1, tzinfo=timezone.utc)  # Initialize with UTC timezone
    for obj in response.get('Contents', []):
        obj_last_modified = obj['LastModified'].replace(tzinfo=timezone.utc)  # Make the timestamp timezone-aware
        if not obj['Key'].endswith('/') and (latest_file is None or obj_last_modified > latest_timestamp):
            latest_file = obj['Key']
            latest_timestamp = obj_last_modified

    latest_file2 = None
    latest_timestamp2 = datetime(1970, 1, 1, tzinfo=timezone.utc)  # Initialize with UTC timezone
    for obj in response2.get('Contents', []):
        obj_last_modified2 = obj['LastModified'].replace(tzinfo=timezone.utc)  # Make the timestamp timezone-aware
        if not obj['Key'].endswith('/') and (latest_file2 is None or obj_last_modified2 > latest_timestamp2):
            latest_file2 = obj['Key']
            latest_timestamp2 = obj_last_modified2

    latest_file3 = None
    latest_timestamp3 = datetime(1970, 1, 1, tzinfo=timezone.utc)  # Initialize with UTC timezone
    for obj in response3.get('Contents', []):
        obj_last_modified3 = obj['LastModified'].replace(tzinfo=timezone.utc)  # Make the timestamp timezone-aware
        if not obj['Key'].endswith('/') and (latest_file3 is None or obj_last_modified3 > latest_timestamp3):
            latest_file3 = obj['Key']
            latest_timestamp3 = obj_last_modified3

    if latest_file:
        s3_path = f's3a://{bucket_name}/{latest_file}'
        # Push the s3_path to XCom
        kwargs['ti'].xcom_push(key='s3_path', value=s3_path)
    else:
        raise ValueError("No files found in the specified S3 bucket/prefix.")

    if latest_file2:
        s3_path2 = f's3a://{bucket_name}/{latest_file2}'
        kwargs['ti'].xcom_push(key='s3_path2', value=s3_path2)
    else:
        raise ValueError("No files found in the specified S3 bucket/prefix.")

    if latest_file3:
        s3_path3 = f's3a://{bucket_name}/{latest_file3}'
        kwargs['ti'].xcom_push(key='s3_path3', value=s3_path3)
    else:
        raise ValueError("No files found in the specified S3 bucket/prefix.")
