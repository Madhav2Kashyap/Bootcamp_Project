from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, expr, udf, coalesce, lit, count, when, window
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, TimestampType, LongType
import json
import boto3
from pyspark.sql.utils import *

bootstrap_servers = 'localhost:9092'

# Initialize Spark session
spark = SparkSession \
    .builder \
    .appName("Streaming from Kafka") \
    .config("spark.streaming.stopGracefullyOnShutdown", True) \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.2,org.postgresql:postgresql:42.2.23") \
    .config("spark.sql.shuffle.partitions", 8) \
    .getOrCreate()


# PostgreSQL connection parameters
jdbc_url = "jdbc:postgresql://35.173.205.182:5432/advance"
jdbc_properties = {
    "user": "priyanshu",
    "password": "1234",
    "driver": "org.postgresql.Driver"
}

s3_bucket = 'ttn-de-bootcamp-2024-bronze-us-east-1'
marked_word_key = '/home/priyanshu/Documents/bootcamp-project/data/marked_word.json'
vocab_key = '/home/priyanshu/Documents/bootcamp-project/data/vocab.json'


def read_json_from_local(file_path):
    try:
        with open(file_path, 'r') as file:
            data = json.load(file)
        return data
    except FileNotFoundError:
        print(f"The file {file_path} does not exist.")

def load_words():
    marked_words = set(read_json_from_local(marked_word_key))
    vocab_words = set(read_json_from_local(vocab_key))
    return marked_words.intersection(vocab_words)

flagged_words = list(load_words())


# Define UDF to flag messages
def is_flagged_udf(message):
    if message is None:
        return False
    words = set(message.split())
    return any(word in words for word in flagged_words)


is_flagged = udf(is_flagged_udf, BooleanType())


def main():
    # Define schema for incoming JSON messages
    schema = StructType([
        StructField("sender", StringType(), True),
        StructField("receiver", StringType(), True),
        StructField("message", StringType(), True)
    ])

    # Read from Kafka
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("subscribe", "test-topic") \
        .option("failOnDataLoss", "false") \
        .option("startingOffsets", "earliest") \
        .load()

    # Parse the JSON messages
    json_df = kafka_df.selectExpr("CAST(value AS STRING) as json", "timestamp") \
        .select(from_json(col("json"), schema).alias("data"), col("timestamp").alias("kafka_timestamp")) \
        .select("data.*", "kafka_timestamp")
    # json_df.printSchema()

    # Process the DataFrame to match the required schema
    df = json_df.withColumn("sender_id", col("sender").cast(LongType())) \
        .withColumn("receiver_id", col("receiver").cast(LongType())) \
        .withColumn("msg_text", col("message").cast(StringType())) \
        .withColumn("time_stamp", col("kafka_timestamp").cast(TimestampType())) \
        .withColumn("is_flagged", is_flagged(col("msg_text")))
    df.printSchema()
    df = df.drop("sender", "receiver", "message", "kafka_timestamp")

    # Set up the streaming query
    query = df.writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", "checkpoint_dir") \
        .outputMode("append")\
        .trigger(processingTime='10 seconds') \
        .start()
    query.awaitTermination()


def process_batch(batch_df, batch_id):
    # batch_df.cache()
    print(batch_id)
    batch_df.show()

    # Filter flagged messages
    flagged_df = batch_df.filter(col("is_flagged") == True)
    flagged_df.show()


    if flagged_df.count() > 0:
        # Calculate new strikes
        flagged_df = flagged_df.filter(col('sender_id') != col('receiver_id'))
        flagged_df = flagged_df.drop("is_flagged")

        flagged_df.write.jdbc(url=jdbc_url, table="public.employee_messages", mode="append", properties=jdbc_properties)


if __name__ == "__main__":
    main()