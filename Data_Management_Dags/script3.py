from pyspark.sql import SparkSession

# Initialize Spark session with required JARs for S3 and PostgreSQL
spark = SparkSession.builder \
    .appName("Read Latest S3 File and Write to PostgreSQL") \
    .config("spark.jars", "/usr/lib/spark/jars/postgresql-42.6.2.jar") \
    .getOrCreate()

# Define the S3 bucket and folder path
s3_bucket = "ttn-de-bootcamp-2024-bronze-us-east-1"
s3_folder = "priyanshu.rana/emp-data/"

# # Set AWS credentials if required (use IAM roles for better security)
# hadoop_conf = spark._jsc.hadoopConfiguration()
# hadoop_conf.set("fs.s3a.access.key", "your-access-key")
# hadoop_conf.set("fs.s3a.secret.key", "your-secret-key")
# hadoop_conf.set("fs.s3a.endpoint", "s3.amazonaws.com")

# List all files in the specified S3 folder
file_list = spark._jvm.org.apache.hadoop.fs.FileSystem \
    .get(spark._jsc.hadoopConfiguration()) \
    .listStatus(spark._jvm.org.apache.hadoop.fs.Path(s3_bucket + "/" + s3_folder))

# Extract file paths and modification times
files_with_time = [(f.getPath().toString(), f.getModificationTime()) for f in file_list]

# Find the latest file based on modification time
latest_file = max(files_with_time, key=lambda x: x[1])[0]

# Read the latest file into a DataFrame
df = spark.read.option("header", "true").csv(latest_file)
df.show()
'''
# PostgreSQL database properties
postgres_url = "jdbc:postgresql://35.170.206.20:5432/advance"
connection_properties = {
    "user": "priyanshu",
    "password": "1234",
    "driver": "org.postgresql.Driver"
}

# Write the DataFrame to the PostgreSQL table
df.write \
    .jdbc(url=postgres_url,
          table="your_table_name",
          mode="append",  # or "overwrite" depending on your requirement
          properties=connection_properties)
'''
# Stop the Spark session
spark.stop()