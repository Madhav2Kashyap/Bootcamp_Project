from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.functions import min, from_unixtime, desc, lag, to_timestamp
from pyspark.sql.window import Window
import sys
import json
from pyspark import StorageLevel
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

    # reading new file to spark dataframe
    df_new = spark.read.csv(s3_paths, header=True, inferSchema=True)

    # persisting the dataframe
    df_new.persist(StorageLevel.MEMORY_AND_DISK)

    # Convert timestamps to dates
    df_new = (df_new.withColumn("start_date", from_unixtime("start_date").cast("date"))
              .withColumn("end_date", from_unixtime("end_date").cast("date")))

    # Setting status ACTIVE AND INACTIVE
    df_new = df_new.withColumn("status",when(col("end_date").isNull(), "ACTIVE").otherwise("INACTIVE"))

    # Checking if there are more than 1 Nulls for a emp_id on basis of end_date
    windowSpec = Window.partitionBy("emp_id", "end_date").orderBy(desc("salary"))
    df_new1 = df_new.select('*', row_number().over(windowSpec).alias('row_number')).where(df_new['end_date'].isNull())
    df_new1 = df_new1.select(df_new.emp_id, df_new.designation, df_new.start_date, df_new.end_date, df_new.salary,
                             df_new.status).where(df_new1.row_number > 1)

    # Removing multiple nulls and keeping only 1 null for each emp_id on basis of end_date
    df_new = df_new.exceptAll(df_new1)

    # Finding min(start_date) for each emp_id , so that we can set it for each emp_id in the old table where the status is ACTIVE
    df_new_2 = df_new.select('*').groupBy(df_new.emp_id).agg(min(df_new.start_date)) \
        .withColumn("demo_status", lit("INACTIVE"))

    # Renaming column in new data
    df_new_2 = df_new_2.withColumnRenamed("emp_id", "e_id")

    # Reading old table/data
    tbl_df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=connection_properties)

    # cndition for join
    condition = ((tbl_df["emp_id"] == df_new_2["e_id"]) & ((tbl_df.end_date).isNull()))

    # Setting min(start_date) for each emp_id , so that we can set it for each emp_id in the old table where the status is ACTIVE and also setting previous ACTIVE to INACTIVE
    target_df = tbl_df.alias("s").join(df_new_2.alias("t"), condition, "left") \
        .withColumn("end_date", coalesce("end_date", "min(start_date)")) \
        .withColumn("status", coalesce("demo_status", "status")) \
        .drop("e_id", "min(start_date)", "demo_status")

    # Combining both the table/data
    target_df = target_df.union(df_new)

    # Again checking if for each emp_id,start_date,end_date there exists multiple records then pick the record with maximum salary
    windowSpec = Window.partitionBy("emp_id", "start_date", "end_date").orderBy(desc("salary"))
    target_df = target_df.withColumn("row_number", row_number().over(windowSpec)) \
        .filter(col("row_number") == 1) \
        .drop("row_number")

    # Ordering the final data
    target_df = target_df.orderBy("emp_id","start_date", "end_date")

    # Writing data to Postgres staging_employee_ts_table
    target_df.write \
        .mode("overwrite") \
        .jdbc(url=jdbc_url, table='staging_employee_ts_table', properties=connection_properties)

transform_timestamp_data()