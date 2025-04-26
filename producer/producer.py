import yfinance as yf
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
import json
import time

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='broker:29092',  # Kafka server
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Initialize Kafka AdminClient for topic creation
admin_client = KafkaAdminClient(
    bootstrap_servers='broker:29092'
)

# Define the stock symbols you want to fetch
# Symbol groups
topics_data = {
    "stocks": ["AAPL", "GOOG", "TSLA", "AMZN", "MSFT", "NFLX", "NVDA", "META", "BAC", "IBM"],
    "crypto_currency": ["BTC-USD", "ETH-USD", "XRP-USD", "DOGE-USD", "ADA-USD", "SOL-USD", "DOT-USD", "LTC-USD", "AVAX-USD", "SHIB-USD"],
    "indices": ["^GSPC", "^NDX", "^DJI", "^FTSE", "^N225", "^GDAXI", "^FCHI", "^HSI", "^AXJO", "^NSEI"],
    "etfs": ["SPY", "QQQ", "IWM", "VTI", "VOO", "EEM", "ARKK", "GLD", "VEU", "ACWX"],
    "currencies": ["EURUSD=X", "GBPUSD=X", "JPY=X", "EURGBP=X", "AUDUSD=X", "CAD=X", "CHF=X", "CNY=X", "INR=X", "MXN=X"]
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
        return []
    return data 

# Ingest data into Kafka every minute
while True:
    for topic, symbols in topics_data.items():
        # Ensure the topic exists or create it
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
                # Send data with symbol as key to ensure consistent partitioning
                producer.send(topic, key=symbol.encode('utf-8'), value=payload)
            else:
                print(f"No data received for {symbol}")
    time.sleep(60)  # Sleep before next round
