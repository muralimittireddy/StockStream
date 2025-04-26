
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Kafka_Spark_Consumer") \
    .getOrCreate()

# Set log level to WARN to reduce unnecessary logs
spark.sparkContext.setLogLevel("WARN")

# Define Kafka topics and Kafka broker
kafka_broker = "broker:29092"
kafka_topics = ["stocks", "crypto_currency", "indices", "etfs", "currencies"]

# Define schema for the incoming data
schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("open", FloatType(), True),
    StructField("high", FloatType(), True),
    StructField("low", FloatType(), True),
    StructField("close", FloatType(), True),
    StructField("volume", FloatType(), True),
])
# Create a streaming DataFrame by reading from Kafka topics
kafka_stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_broker) \
    .option("subscribe", ",".join(kafka_topics)) \
    .load()

# The Kafka message is in the 'value' field, which is in binary format.
# Deserialize the Kafka message from binary to string and apply the schema
# Extract the 'value' field and cast it to string
df = kafka_stream_df.selectExpr("CAST(value AS STRING) as json_value")

# Parse the JSON string using from_json
df_json = df.select(from_json(col("json_value"), schema).alias("data")).select("data.*")

# For demo purposes, let's display the data
# You can apply transformations, filtering, and processing as required
processed_df = df_json.select("symbol", "timestamp", "open", "high", "low", "close", "volume")

# Write the processed data to the console (you can also write it to other sinks like files, databases, etc.)
query = processed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Await termination of the query
query.awaitTermination()

# Stop Spark session once done
spark.stop()
