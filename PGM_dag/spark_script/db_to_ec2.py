from pyspark.sql import SparkSession
import json
import sys

def fetch_and_save_to_s3():
    # Step 1: Create Spark Session
    spark = SparkSession.builder \
        .appName("FetchAndSaveToS3") \
        .config("spark.jars", "/usr/lib/spark/jars/postgresql-42.6.2.jar") \
        .getOrCreate()

    jdbc_url = sys.argv[sys.argv.index('--jdbc_url') + 1]
    connection_properties = sys.argv[sys.argv.index('--cp') + 1]
    table_name = sys.argv[sys.argv.index('--table_name') + 1]
    bucket_name = sys.argv[sys.argv.index('--bucket_name') + 1]
    prefix = sys.argv[sys.argv.index('--prefix') + 1]
    s3_path = f"s3://{bucket_name}/{prefix}/{table_name}.csv"

    # Parse the JSON string back into a Python dictionary
    connection_properties = json.loads(connection_properties)

    # Step 2: Read DataFrame from PostgreSQL table
    df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=connection_properties)

    # Step 3: Write DataFrame to S3 without partitioning
    df.coalesce(1).write \
        .format("csv") \
        .option("header", "true") \
        .mode("overwrite") \
        .save(s3_path)

    # Stop the Spark session
    spark.stop()

fetch_and_save_to_s3()