from pyspark.sql import SparkSession, Window
import sys
import json
import ast
from pyspark.sql.functions import row_number, col, desc, monotonically_increasing_id, to_date


def task_3_2():
    spark = SparkSession.builder \
        .appName("task_3_2") \
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

    # Adding new column ser_num so to pick latest row in partition of emp_id
    spark_df = spark_df.withColumn("SerialNum", monotonically_increasing_id())

    # Define window specification
    windowSpec = Window.partitionBy("emp_id", "date").orderBy(col("serialNum").desc())

    # Add row numbers to the dataframe, partitioned by employee_id and leave_date
    leave_data_with_row_number = spark_df.withColumn("row_number", row_number().over(windowSpec))

    # Filter out the rows where row_number is 1 (i.e., keep only the first row in each group)
    duplicated_leave_data_df = leave_data_with_row_number.filter(col("row_number") == 1)

    # Show the resulting dataframe
    duplicated_leave_data_df = duplicated_leave_data_df.drop("row_number").drop("SerialNum")

    duplicated_leave_data_df = duplicated_leave_data_df.withColumn("date", to_date(col('date'), 'yyyy-MM-dd'))

    # Write Spark DataFrame to PostgreSQL table
    duplicated_leave_data_df.write\
        .option("truncate", "true") \
        .jdbc(url=jdbc_url,
              table=table_name,
              mode="overwrite",  # or "overwrite" depending on your requirement
              properties=connection_properties)

    spark.stop()

task_3_2()