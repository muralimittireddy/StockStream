import yfinance as yf
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
import json
import os
import pandas as pd 
from datetime import datetime, time
from pytz import timezone
import time as systime


def is_market_open():
    eastern = timezone('US/Eastern')
    now = datetime.now(eastern)
    return now.weekday() < 5 and time(9, 30) <= now.time() < time(16, 0)


print("Producer script started")
print(f"Connecting to Kafka at {os.getenv('BOOTSTRAP_SERVERS')}")


# Initialize Kafka Producer
try:
    producer = KafkaProducer(
        bootstrap_servers='broker:29092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print("Kafka producer created successfully")
except Exception as e:
    print(f"Failed to create Kafka producer: {e}")
    systime.sleep(10)
    exit(1)

# Kafka Admin
try:
    admin_client = KafkaAdminClient(
        bootstrap_servers='broker:29092'
    )
except Exception as e:
    print(f"Failed to create Kafka AdminClient: {e}")
    systime.sleep(10)
    exit(1)


# Define the stock symbols you want to fetch
# Symbol groups
topics_data = {
    "stocks": ["AAPL", "GOOG", "TSLA", "AMZN", "MSFT", "NFLX", "NVDA", "META", "BAC", "IBM"],
    "crypto_currency": ["BTC-USD", "ETH-USD", "XRP-USD", "DOGE-USD", "ADA-USD", "SOL-USD", "DOT-USD", "LTC-USD", "AVAX-USD", "SHIB-USD"],
    "indices": ["^GSPC", "^NDX", "^DJI", "^FTSE", "^N225", "^GDAXI", "^FCHI", "^HSI", "^AXJO", "^NSEI"],
    "etfs": ["SPY", "QQQ", "IWM", "VTI", "VOO", "EEM", "ARKK", "GLD", "VEU", "ACWX"],
    "currencies": ["EURUSD=X", "GBPUSD=X", "JPY=X", "EURGBP=X", "AUDUSD=X", "CAD=X", "CHF=X", "NZDUSD=X", "INR=X", "MXN=X"]
}

# Function to create Kafka topic with custom configurations if it doesn't exist
def create_topic_if_not_exists(topic_name):
    try:
        # Check if the topic already exists (this step is optional, just for safety)
        existing_topics = admin_client.list_topics()
        if topic_name not in existing_topics:
            print(f"Creating topic: {topic_name}")
            # Create the topic with custom configurations
            topic = NewTopic(
                name=topic_name, 
                num_partitions=10,  # Custom number of partitions
                replication_factor=1 # Custom replication factor
                # config={'retention.ms': '3600000'}  # Example custom configuration (1 hour retention)
            )
            admin_client.create_topics([topic])
            print(f"Topic {topic_name} created successfully.")
        else:
            print(f"Topic {topic_name} already exists.")
    except Exception as e:
        print(f"Error creating topic {topic_name}: {e}")

# Function to get stock data
def get_stock_data(symbol):
    print(f"Fetching data for: {symbol}")
    stock = yf.Ticker(symbol)
    data = stock.history(period="1d", interval="1m").tail(1)  # Only get the latest record
    if data.empty:
        print(f"No data for {symbol}")
        return pd.DataFrame()
    return data 

# Ingest data into Kafka every minute
while True:
    for topic, symbols in topics_data.items():
        # Skip non-crypto topics outside market hours
        if topic != "crypto_currency" and not is_market_open():
            print(f"Market closed. Skipping topic: {topic}")
            continue

        create_topic_if_not_exists(topic)
        for symbol in symbols:
            stock_data = get_stock_data(symbol)
            if not stock_data.empty:
                timestamp = stock_data.index[0]
                row = stock_data.iloc[0]
                payload = {
                    "symbol": symbol,
                    "timestamp": str(timestamp),
                    "open": row["Open"],
                    "high": row["High"],
                    "low": row["Low"],
                    "close": row["Close"],
                    "volume": row["Volume"]
                }
                print(f"Sending to topic: {topic} => {payload}")
                producer.send(topic, key=symbol.encode('utf-8'), value=payload)
            else:
                print(f"No data received for {symbol}")
    
    systime.sleep(60)
