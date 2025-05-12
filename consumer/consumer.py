from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType

from clickhouse_client import create_ohlcv_table

# Required external packages
packages = [
    "com.clickhouse.spark:clickhouse-spark-runtime-3.3_2.12:0.8.0",
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.3",
    "com.clickhouse:clickhouse-jdbc:0.6.3"
]

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Kafka_Spark_Consumer") \
    .config("spark.jars.packages", ",".join(packages)) \
    .getOrCreate()

# Reduce log verbosity
spark.sparkContext.setLogLevel("WARN")

# Define Kafka broker and topics
kafka_broker = "broker:29092"
kafka_topics = ["stocks", "crypto_currency", "indices", "etfs", "currencies"]

# Create ClickHouse table (if not exists)
create_ohlcv_table()

# Define the schema matching producer JSON
schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("open", FloatType(), True),
    StructField("high", FloatType(), True),
    StructField("low", FloatType(), True),
    StructField("close", FloatType(), True),
    StructField("volume", FloatType(), True),
])

# Read Kafka messages as streaming DataFrame
kafka_stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_broker) \
    .option("subscribe", ",".join(kafka_topics)) \
    .load()

# Deserialize Kafka message value and apply schema
df = kafka_stream_df.selectExpr("CAST(value AS STRING) as json_value", "topic")

df_parsed = df.select(
    from_json(col("json_value"), schema).alias("data"),
    col("topic")
)

# Flatten the data and add category (topic) column
processed_df = df_parsed.select(
    col("data.timestamp").alias("timestamp"),
    col("data.symbol").alias("symbol"),
    col("data.open").alias("open"),
    col("data.high").alias("high"),
    col("data.low").alias("low"),
    col("data.close").alias("close"),
    col("data.volume").alias("volume"),
    col("topic").alias("category")
)

# Function to write each micro-batch to ClickHouse
def write_to_clickhouse(batch_df, batch_id):
    batch_df.show(truncate=False)  # For debugging — remove in prod
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:clickhouse://clickhouse:8123/stock") \
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
        .option("dbtable", "ohlcv_data") \
        .option("user", "user") \
        .option("password", "user_password") \
        .mode("append") \
        .save()

# Write to ClickHouse using foreachBatch
query = processed_df.writeStream \
    .foreachBatch(write_to_clickhouse) \
    .outputMode("append") \
    .start()

# Wait for termination
query.awaitTermination()
