from pyspark import StorageLevel
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import *
from pyspark.sql.types import LongType, DateType
from datetime import datetime#, timezone
import sys
import json

def task_4_2_transformation():

    spark = SparkSession.builder\
             .appName("task4_2") \
             .config("spark.jars", "/usr/lib/spark/jars/postgresql-42.6.2.jar") \
             .getOrCreate()

    s3_path_1 = sys.argv[1]
    s3_path_2 = sys.argv[2]
    jdbc_url = sys.argv[sys.argv.index('--jdbc_url') + 1]
    connection_properties = sys.argv[sys.argv.index('--cp') + 1]
    table_name = sys.argv[sys.argv.index('--table_name') + 1]

    # Parse the JSON string back into a Python dictionary
    connection_properties = json.loads(connection_properties)

    # Read data
    calendar_data = spark.read.csv(s3_path_1, header=True, inferSchema=True)
    leave_data = spark.read.csv(s3_path_2, header=True, inferSchema=True)


    # Get current year and current date
    current_year = datetime.now().year
    current_date = datetime.now().date()

    #Filtering the data for current_year
    calendar_data = calendar_data.filter(year("date")==current_year)
    leave_data = leave_data.filter(year("date")==current_year)

    # Convert date columns to DateType
    to_date = lambda col: col.cast(DateType())
    leave_data = leave_data.withColumn("leave_date", to_date(col("date"))).drop("date")
    calendar_data = calendar_data.withColumn("calendar_date", to_date(col("date"))).drop("date")

    # Filter leave data of leaves which are about to come
    leave_data_filtered = leave_data.filter(col("leave_date") > current_date)
    leave_data_filtered.persist(StorageLevel.MEMORY_AND_DISK)

    # Filter the DataFrame to include only dates after the current date
    holidays_upcoming = calendar_data.filter(col("calendar_date") > current_date)
    holidays_upcoming = holidays_upcoming.filter(dayofweek(col("calendar_date")).isin([2, 3, 4, 5, 6]))

    # Count the rows in the filtered DataFrame
    holiday_days_count = holidays_upcoming.count()

    # Collect calendar_date values into a list
    calendar_dates = [row.calendar_date for row in holidays_upcoming.select("calendar_date").collect()]

    # Define expression for weekends 1 is Sunday and 7 is Saturday (This is just a condition which is used afterwards)
    weekend_expr = dayofweek(col("leave_date")).isin([1, 7])

    # Filter out weekends and holidays
    leave_data_filtered = leave_data_filtered.filter(~(weekend_expr | col("leave_date").isin(calendar_dates)))
    leave_data_filtered = leave_data_filtered.filter(col("status") != "CANCELLED")

    # Remove duplicates
    leave_data_filtered = leave_data_filtered.dropDuplicates(["emp_id", "leave_date"])

    # Calculate total leaves taken by each employee
    leave_count = leave_data_filtered.groupBy("emp_id").agg(count("*").alias("upcoming_leaves"))

    # Calculate end_date for the current year
    end_date = "{}-12-31".format(current_year)

    # Calculate the number of days between current date and end date
    days_diff = spark.sql(f"SELECT datediff(to_date('{end_date}'), current_date()) + 1 as days_diff").collect()[0]['days_diff']

    # Create date range
    date_range = spark.range(1, days_diff).select(expr("date_add(current_date(), cast(id as int))").alias("date"))

    # Filter out weekends (Saturday and Sunday)
    working_days = date_range.filter(dayofweek("date").isin([2, 3, 4, 5, 6])).count()
    total_working_days = working_days - holiday_days_count


    # Calculate potential leaves for the current year
    potential_leaves_year = leave_count.withColumn("potential_leaves_percentage", (col("upcoming_leaves") / total_working_days) * 100)
    upcoming_leaves = potential_leaves_year.select("emp_id","upcoming_leaves").filter(col("potential_leaves_percentage") > 8)


    upcoming_leaves.write \
        .option("truncate", "true") \
        .mode("overwrite") \
        .jdbc(url=jdbc_url, table=table_name , properties=connection_properties)
    #upcoming_leaves.orderBy("emp_id").show()


task_4_2_transformation()
