s3_path = 's3://ttn-de-bootcamp-2024-bronze-us-east-1/priyanshu.rana/files/games_data.csv'
with open(s3_path, 'rb') as f:
    data = f.read()
    data_str = data.decode('utf-8')
with tempfile.NamedTemporaryFile(mode='w', delete=False) as tmp_file:
    tmp_file.write(data_str)
    tmp_file_path = tmp_file.name
    df = spark.read.csv(tmp_file_path, header=True, inferSchema=True)

# Define the JDBC URL and properties
jdbc_url = "jdbc:postgresql://35.170.206.20:5432/advance"
connection_properties = {
    "user": "priyanshu",
    "password": "1234",
    "driver": "org.postgresql.Driver"
}

df.write \
    .mode("append") \
    .jdbc(url=jdbc_url, table='games_played', properties=connection_properties)
