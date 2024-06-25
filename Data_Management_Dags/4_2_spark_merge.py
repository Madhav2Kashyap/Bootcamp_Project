





from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("MergeExample") \
    .getOrCreate()

# Load the target DataFrame
ac_employee_ts_table = spark.read.format("csv").option("header", "true").load("/path/to/ac_employee_ts_table.csv")

# Load the staging DataFrame
staging_desig_count = spark.read.format("csv").option("header", "true").load("/path/to/staging_desig_count.csv")

join_condition = ac_employee_ts_table["desig"] == staging_desig_count["desig"]
matched_df = ac_employee_ts_table.join(staging_desig_count, join_condition, "left_outer")

matched_rows = matched_df.filter(col("staging_desig_count.desig").isNotNull())
unmatched_rows = matched_df.filter(col("staging_desig_count.desig").isNull())

updated_rows = matched_rows.withColumn("count", col("staging_desig_count.count")) \
                           .select(ac_employee_ts_table.columns)

new_rows = staging_desig_count.join(ac_employee_ts_table, join_condition, "left_anti")

final_df = updated_rows.union(unmatched_rows.select(ac_employee_ts_table.columns)).union(new_rows)

final_df.write.format("csv").option("header", "true").mode("overwrite").save("/path/to/ac_employee_ts_table")

