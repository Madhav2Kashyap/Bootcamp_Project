import boto3
from datetime import datetime, timezone
from collections import defaultdict

def task1_move_files(bucket_name, prefix, destination_prefix, **kwargs):
    # Initialize the S3 client
    s3 = boto3.client('s3')

    # List objects within the source prefix
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    # Initialize variables to track the oldest files
    oldest_files = []
    oldest_timestamp = datetime.now(timezone.utc)  # Initialize with the current time

    # Dictionary to hold files grouped by date
    date_files = defaultdict(list)

    # Iterate through the files and group them by date
    for obj in response.get('Contents', []):
        obj_last_modified = obj['LastModified'].replace(tzinfo=timezone.utc)  # Ensure timestamp is timezone-aware

        # Check if the object is a file (not a directory) and is a .csv file
        if not obj['Key'].endswith('/') and obj['Key'].endswith('.csv'):
            date_key = obj_last_modified.date()
            date_files[date_key].append(f's3://{bucket_name}/{obj["Key"]}')

            if obj_last_modified < oldest_timestamp:
                oldest_timestamp = obj_last_modified

    # Get the list of oldest files for the oldest date
    if oldest_timestamp.date() in date_files:
        oldest_files = date_files[oldest_timestamp.date()]

    # Move each of the oldest files to the destination folder
    for file_path in oldest_files:
        file_key = file_path.split(f's3://{bucket_name}/')[-1]

        # Construct the destination key with the timestamp while preserving the file extension
        file_name = file_key.split('/')[-1]
        base_name, extension = file_name.rsplit('.', 1)
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
        destination_key = f"{destination_prefix}/{base_name}_{timestamp}.{extension}"

        # Copy the file to the destination folder with the new name
        copy_source = {'Bucket': bucket_name, 'Key': file_key}
        s3.copy_object(CopySource=copy_source, Bucket=bucket_name, Key=destination_key)

        # Delete the original file from the source folder
        s3.delete_object(Bucket=bucket_name, Key=file_key)

