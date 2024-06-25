from datetime import date

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import row_number, col, count, date_format, current_timestamp, lit, round, pow, date_sub, \
    max as max_, when, to_date
import sys
import json
# import boto3

def checkStrike():

    spark = SparkSession.builder \
                      .appName("Connect") \
                      .config("spark.jars", "/usr/lib/spark/jars/postgresql-42.6.2.jar") \
                      .getOrCreate()


    jdbc_url = sys.argv[sys.argv.index('--jdbc_url') + 1]
    connection_properties = sys.argv[sys.argv.index('--cp') + 1]
    table_name1 = sys.argv[sys.argv.index('--table_name1') + 1]
    table_name2 = sys.argv[sys.argv.index('--table_name2') + 1]

    # Parse the JSON string back into a Python dictionary
    connection_properties = json.loads(connection_properties)


    flagged_df = spark.read.jdbc(url=jdbc_url, table=table_name1, properties=connection_properties)

    new_strikes_df = flagged_df.groupBy("sender_id") \
        .agg(count("*").alias("strike_count"), max_("time_stamp").alias("last_strike_date"))
    new_strikes_df = new_strikes_df.withColumn("last_strike_date",date_format("last_strike_date", "yyyy-MM-dd"))
    new_strikes_df = new_strikes_df.withColumn("last_strike_date", to_date("last_strike_date", "yyyy-MM-dd"))
    new_strikes_df = new_strikes_df.withColumnRenamed("sender_id", "employee_id")
    new_strikes_df.show()

    timeframe_df = spark.read.jdbc(url=jdbc_url, table=table_name2, properties=connection_properties)
    timeframe_df = timeframe_df.filter(col("status") == "ACTIVE")


    new_strikes_df = new_strikes_df.join(timeframe_df,new_strikes_df["employee_id"]==timeframe_df["emp_id"],"left")
    new_strikes_df = new_strikes_df.drop("emp_id","designation","start_date","end_date","status")


    new_strikes_df = new_strikes_df.groupby("employee_id","strike_count","last_strike_date").agg(max_("salary").alias("salary"))
    new_strikes_df.show()

    new_strikes_df = (new_strikes_df
                      .withColumnRenamed("strike_count", "strk_count")
                      .withColumnRenamed("last_strike_date", "lst_strike_date"))

    new_strikes_df.write.mode("overwrite").jdbc(url=jdbc_url, table="staging_strike_table",
                                                properties=connection_properties)



checkStrike()