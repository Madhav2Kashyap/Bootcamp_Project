from pyspark.sql import SparkSession
# from smart_open import open
import tempfile
import sys
import json

def read_Data():
    # spark = SparkSession.builder \
    #     .config("spark.jars", "/usr/lib/spark/jars/postgresql-42.6.2.jar")\
    #     .appName("Connect") \
    #     .getOrCreate()
    # .config("spark.jars", "/usr/lib/spark/jars/postgresql-42.6.2.jar") \
    #     .config("spark.yarn.submit.waitAppCompletion", "true") \


    # FOR LOCAL:
    # spark = (SparkSession.builder \
    #          .appName("Connect") \
    #          .config("spark.hadoop.fs.s3a.access.key", "AKIATIH3FJYGDFPYKHGB") \
    #          .config("spark.hadoop.fs.s3a.secret.key", "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") \
    #          .config("spark.jars", "/home/priyanshu/Documents/postgresql-42.6.2.jar,"
    #                                "/home/priyanshu/Downloads/hadoop-aws-3.3.1.jar,"
    #                                "/home/priyanshu/Downloads/aws-java-sdk-bundle-1.11.901.jar") \
    #          .getOrCreate())

    spark = (SparkSession.builder \
                      .appName("Connect") \
                      .config("spark.jars", "/usr/lib/spark/jars/postgresql-42.6.2.jar") \
                      .getOrCreate())

    #s3_path = 's3a://ttn-de-bootcamp-2024-bronze-us-east-1/priyanshu.rana/files/games_data.csv'
    s3_path = sys.argv[1]
    jdbc_url = sys.argv[sys.argv.index('--jdbc_url')+1]
    connection_properties = sys.argv[sys.argv.index('--cp')+1]

    connection_properties = json.loads(connection_properties)

    # jdbc_user = sys.argv[sys.argv.index('--jdbc_user')+1]
    # jdbc_password = sys.argv[sys.argv.index('--jdbc_pass')+1]
    # driver_class = sys.argv[sys.argv.index('--driver_class')+1]
    #
    # connection_properties = {
    #     "user": jdbc_user,
    #     "password": jdbc_password,
    #     "driver": driver_class,
    #     "batchsize": "50000"  # Set the batch size to 50000 records
    # }
    print("connection_properties", connection_properties)


    # jdbc_url = sys.argv[sys.argv.index('--jdbc_url')+1]
    # print("JDBC URL:", jdbc_url)
    #
    # connection_properties = sys.argv[sys.argv.index('--cp')+1]
    # print("Connection Properties String:", connection_properties)
    #
    # connection_properties = json.loads(connection_properties)
    # print("Connection Properties Dictionary:", connection_properties)

    df = spark.read.csv(s3_path, header=True, inferSchema=True)
    df.show()
    # Define the JDBC URL and properties


    # jdbc_url = "jdbc:postgresql://35.170.206.20:5432/advance"
    # connection_properties = {
    #     "user": "priyanshu",
    #     "password": "1234",
    #     "driver": "org.postgresql.Driver"
    # }

    df.write \
        .mode("append") \
        .jdbc(url=jdbc_url, table='games_played', properties=connection_properties)

#     # spark.stop()
#
read_Data()
