from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys
import json

def task_4_3_transformation():

    spark = (SparkSession.builder\
        .appName("task4_3") \
        .config("spark.jars", "/usr/lib/spark/jars/postgresql-42.6.2.jar") \
        .getOrCreate())

    s3_path_1 = sys.argv[1]
    s3_path_2 = sys.argv[2]
    jdbc_url = sys.argv[sys.argv.index('--jdbc_url') + 1]
    connection_properties = sys.argv[sys.argv.index('--cp') + 1]

    # Parse the JSON string back into a Python dictionary
    connection_properties = json.loads(connection_properties)


    emp_leave_quota_data = spark.read.csv(s3_path_1, header=True, inferSchema=True)
    employee_leave_application_data = spark.read.csv(s3_path_2, header=True, inferSchema=True)

    current_year = datetime.now().year

    # Filtering the data for current_year
    emp_leave_quota_data = emp_leave_quota_data.filter(col("year") == current_year)
    employee_leave_application_data = employee_leave_application_data.filter(year("date") == current_year)


    # Filter active leaves
    active_leaves = employee_leave_application_data.filter(col("status") == "ACTIVE")

    # Calculate total leaves taken by each employee and year
    total_leaves_taken = active_leaves.groupBy("emp_id", year("date").alias("year")).agg(
        count("*").alias("total_leaves_taken"))
    # print("total_leaves_taken")
    # total_leaves_taken.orderBy("emp_id").show()

    # Join total_leave_quota and total_leaves_taken
    leave_data = emp_leave_quota_data.join(total_leaves_taken, ["emp_id", "year"], "left")

    # Calculate percentage of leave quota used
    leave_data = leave_data.withColumn("percentage_used", expr("total_leaves_taken / leave_quota * 100")).filter(
        col("total_leaves_taken").isNotNull())
    # print("leave_data")
    # leave_data.orderBy("emp_id").show()

    # Identify employees whose availed leave quota exceeds 80% for each year
    employees_to_notify = leave_data.filter(col("percentage_used") > 80)
    # print("employees_to_notify")

    # # Write the final DataFrame to the database table
    employees_to_notify.write \
        .option("truncate", "true") \
        .mode("overwrite") \
        .jdbc(url=jdbc_url, table="employee_leaves_spend_table_dim", properties=connection_properties)


task_4_3_transformation()
