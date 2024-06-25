from pyspark.sql import SparkSession
import sys
import json
import ast

def transform_timestamp_data():
    spark = SparkSession.builder\
        .appName("task2") \
        .config("spark.jars", "/usr/lib/spark/jars/postgresql-42.6.2.jar") \
        .getOrCreate()

    s3_paths_str = sys.argv[1]
    jdbc_url = sys.argv[sys.argv.index('--jdbc_url') + 1]
    connection_properties = sys.argv[sys.argv.index('--cp') + 1]
    table_name = sys.argv[sys.argv.index('--table_name') + 1]


    # Parse the JSON string back into a Python dictionary
    connection_properties = json.loads(connection_properties)

    s3_paths = ast.literal_eval(s3_paths_str)

    spark_df = spark.read.csv(s3_paths, header=True, inferSchema=True)


    spark_df.write \
        .jdbc(url=jdbc_url,
              table=table_name,
              mode="append",  # or "overwrite" depending on your requirement
              properties=connection_properties)

transform_timestamp_data()