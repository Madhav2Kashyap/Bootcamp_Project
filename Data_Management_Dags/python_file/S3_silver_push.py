from pyspark.sql import SparkSession
from jdbc import jdbc_url,connection_properties

# Function to save DataFrame to S3 after loading from PostgreSQL
def Save_to_S3(bucket_name, prefix, table_name, file_format ):

    spark = SparkSession.builder \
        .appName("Save to S3") \
        .getOrCreate()

    # Load data from PostgreSQL into a DataFrame
    df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=connection_properties)

    # Determine the file extension based on the file_format parameter
    if file_format == "csv":
        file_extension = "csv"
    elif file_format == "text":
        file_extension = "txt"

    # Construct the full S3 output path
    s3_output_path = f"s3a://{bucket_name}/{prefix}/{table_name}.{file_extension}"

    if file_format == "csv":
        df.write.csv(s3_output_path, header=True, mode="overwrite")
    elif file_format == "text":
        df.write.text(s3_output_path, mode="overwrite")