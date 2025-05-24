# clickhouse_client.py

from clickhouse_connect import get_client

# Connection setup
client = get_client(
    host='clickhouse',
    port=8123,  # HTTP port for ClickHouse
    username='user',
    password='user_password'
)

# Create table if not exists
def create_ohlcv_table():
    # Step 1: Create database if it doesn't exist
    client.command('CREATE DATABASE IF NOT EXISTS stock')
    print("database created")

    # Step 2: Switch to stock database
    client.command('USE stock')
    print("its in use")

     # Step 3: Create table if it doesn't exist
    client.command('''
    CREATE TABLE IF NOT EXISTS ohlcv_data (
        timestamp DateTime,
        symbol String,
        open Float64,
        high Float64,
        low Float64,
        close Float64,
        volume Float64,
        category String
    ) ENGINE = MergeTree()
    ORDER BY (symbol, timestamp)
    TTL timestamp + INTERVAL 1 DAY;
    ''')
    print("ClickHouse table is ready.")

# Insert batch into ClickHouse
def insert_ohlcv_batch(batch_data):
    rows = []
    for record in batch_data:
        rows.append((
            record['timestamp'],
            record['symbol'],
            record['open'],
            record['high'],
            record['low'],
            record['close'],
            record['volume'],
            record.get('category', '')
        ))
    
    client.insert(
        table='stock.ohlcv_data',
        data=rows,
        column_names=['timestamp', 'symbol', 'open', 'high', 'low', 'close', 'volume','category']
    )
    print(f"Inserted {len(rows)} records into ClickHouse.")
