
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType

from clickhouse_client import create_ohlcv_table, insert_ohlcv_batch

packages = [
    "com.clickhouse.spark:clickhouse-spark-runtime-3.3_2.12:0.8.0",
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.3",
    "com.clickhouse:clickhouse-jdbc:0.6.3"]

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Kafka_Spark_Consumer") \
    .config("spark.jars.packages", ",".join(packages)) \
    .getOrCreate()

# Set log level to WARN to reduce unnecessary logs
spark.sparkContext.setLogLevel("WARN")

# Define Kafka topics and Kafka broker
kafka_broker = "broker:29092"
kafka_topics = ["stocks", "crypto_currency", "indices", "etfs", "currencies"]

# Create table before starting
create_ohlcv_table()

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

# Kafka message is in the 'value' field, which is in binary format.
# Deserialize the Kafka message from binary to string and apply the schema
df = kafka_stream_df.selectExpr("CAST(value AS STRING) as json_value","topic")

# Parse the JSON string using from_json
df_json = df.select(from_json(col("json_value"), schema).alias("data"),
    col("topic"))

# Flatten data
df_json_flattened = df_json.select(
    "data.*",
    "topic"
)

# Add 'category' column, which is derived from the Kafka topic
df_json_with_category = df_json.withColumn("category", col("topic"))

# Now select the necessary columns, including the new 'category' field
processed_df = df_json_with_category.select(
    "data.timestamp",
    "data.symbol",
    "data.open",
    "data.high",
    "data.low",
    "data.close",
    "data.volume",
    "category"
)

def write_to_clickhouse(batch_df, batch_id):
    print("=== BATCH ===")
    print(f"Writing batch: {batch_id} - Count: {batch_df.count()}")
    batch_df.printSchema()
    batch_df.show(truncate=False)

    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:clickhouse://clickhouse:8123/stock") \
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
        .option("dbtable", "ohlcv_data") \
        .option("user", "user") \
        .option("password", "user_password") \
        .mode("append") \
        .save()

# For demo purposes, let's display the data
# You can apply transformations, filtering, and processing as required
query = processed_df.writeStream \
            .foreachBatch(write_to_clickhouse) \
            .outputMode("append") \
            .start()


# Await termination of the query
query.awaitTermination()

# Stop Spark session once done
spark.stop()
