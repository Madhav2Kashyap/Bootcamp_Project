# from datetime import datetime, timezone
from pyspark.sql import SparkSession
import sys


def read_data():
    spark = SparkSession.builder\
        .appName("task1") \
        .config("spark.jars", "/usr/lib/spark/jars/postgresql-42.6.2.jar") \
        .getOrCreate()
    s3_path = sys.argv[1]
    table = 'employee_data'

    df = spark.read.csv(s3_path, header=True, inferSchema=True)
    df.show()

    jdbc_url = "jdbc:postgresql://35.170.206.20:5432/advance"
    connection_properties = {
        "user": "priyanshu",
        "password": "1234",
        "driver": "org.postgresql.Driver",
        "batchsize": "50000"  # Set the batch size to 50000 records
    }

    # Write Spark DataFrame to PostgreSQL table
    df.write\
        .jdbc(url=jdbc_url,
              table=table,
              mode="append",  # or "overwrite" depending on your requirement
              properties=connection_properties)


read_data()