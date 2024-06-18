from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import row_number, col
import sys
import json
import ast

def task_1():
    spark = SparkSession.builder \
        .appName("task1") \
        .config("spark.jars", "/usr/lib/spark/jars/postgresql-42.6.2.jar") \
        .getOrCreate()

    s3_paths_str = sys.argv[1]
    jdbc_url = sys.argv[sys.argv.index('--jdbc_url') + 1]
    connection_properties = sys.argv[sys.argv.index('--cp') + 1]
    table_name = sys.argv[sys.argv.index('--table_name') + 1]

    # Parse the JSON string back into a Python dictionary
    connection_properties = json.loads(connection_properties)

    # Parse the input string into a list of paths
    s3_paths = ast.literal_eval(s3_paths_str)

    # Read multiple CSV files into a single DataFrame
    spark_df = spark.read.csv(s3_paths, header=True, inferSchema=True)

    window_spec = Window.partitionBy("emp_id").orderBy("emp_id")
    spark_df = spark_df.withColumn("row_num", row_number().over(window_spec)) \
        .filter(col("row_num") == 1) \
        .drop("row_num")

    # Write Spark DataFrame to PostgreSQL table
    spark_df.write \
        .jdbc(url=jdbc_url,
              table=table_name,
              mode="overwrite",  # or "overwrite" depending on your requirement
              properties=connection_properties)

    spark.stop()

task_1()