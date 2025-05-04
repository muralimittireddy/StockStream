import pandas as pd
from clickhouse_connect import get_client
from google.cloud import bigquery
from datetime import datetime, timedelta, timezone
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def ingest_data():
    try:
        # Use UTC time for consistency with Airflow
        now = datetime.now(timezone.utc)

        # Calculate previous hour fixed window
        end_time = now.replace(minute=0, second=0, microsecond=0)
        start_time = end_time - timedelta(hours=1)

        # Format times for ClickHouse
        start_str = start_time.strftime('%Y-%m-%d %H:%M:%S')
        end_str = end_time.strftime('%Y-%m-%d %H:%M:%S')

        # ClickHouse connection
        clickhouse_client = get_client(host='clickhouse', port=8123, username='user', password='user_password')

        # Query only data from the exact hour
        query = f"""
            SELECT * FROM stock.ohlcv_data
            WHERE timestamp >= toDateTime('{start_str}')
              AND timestamp < toDateTime('{end_str}')
        """

        logger.info(f"Running query: {query}")
        result = clickhouse_client.query(query)

        if not result.result_rows:
            logger.warning(f"No data found for the time window {start_str} to {end_str}.")
            return

        df = pd.DataFrame(result.result_rows, columns=result.column_names)  # Option 1

        # BigQuery setup
        bq_client = bigquery.Client()

        # Ensure the destination table exists, or else handle accordingly
        table_id = 'solarcropsanalysis-454507.ohlcv_dataset.ohlcv_table'
        logger.info(f"Loading data to BigQuery table: {table_id}")

        job = bq_client.load_table_from_dataframe(df, table_id)
        job.result()  # Wait for the job to complete

        logger.info(f"Loaded OHLCV data from {start_str} to {end_str} into BigQuery.")

    except Exception as e:
        logger.error(f"Error occurred during ingestion: {str(e)}")
        raise  # Re-raise the exception for Airflow to capture failure

if __name__ == "__main__":
    ingest_data()
