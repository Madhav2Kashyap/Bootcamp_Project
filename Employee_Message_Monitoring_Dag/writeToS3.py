from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import row_number, col, count, date_format
import sys
import json
# import boto3

def write():

    spark = (SparkSession.builder \
                      .appName("Connect") \
                      .config("spark.jars", "/home/priyanshu/Documents/postgresql-42.6.2.jar") \
                      .getOrCreate())

    jdbc_url = "jdbc:postgresql://35.173.205.182:5432/advance"

    connection_properties = {
    "user": "priyanshu",
    "password": "1234",
    "driver": "org.postgresql.Driver"
    }

    # s3 = boto3.client('s3')

    tbl_df = spark.read.jdbc(url=jdbc_url, table='employee_messages_dim', properties=connection_properties)

    tbl_df = tbl_df.withColumn("date", date_format(col("time_stamp"), "yyyy-MM-dd"))

    tbl_df.printSchema()

    s3_path = "s3a://ttn-de-bootcamp-2024-bronze-us-east-1/priyanshu.rana/msg/"

    tbl_df.write \
        .mode("append") \
        .partitionBy("date") \
        .parquet(s3_path)
    spark.stop()

write()
