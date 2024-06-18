from pyspark.sql import SparkSession
import sys
import json
import ast

def task_4_1_transformation():
    spark = SparkSession.builder\
        .appName("task4_1") \
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

    # Filter the DataFrame for active employees
    spark_df = spark_df.filter(spark_df.status == "ACTIVE")

    # Group by designation and count
    active_counts_df = spark_df.groupBy("designation").count().withColumnRenamed("count", "active_count")

    # Join the count DataFrame with the original DataFrame on 'designation'

    active_counts_df.write \
        .option("truncate", "true") \
        .mode("overwrite") \
        .jdbc(url=jdbc_url, table=table_name, properties=connection_properties)

task_4_1_transformation()