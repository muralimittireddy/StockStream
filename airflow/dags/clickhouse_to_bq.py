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


        # ClickHouse connection
        clickhouse_client = get_client(host='clickhouse', port=8123, username='user', password='user_password')

        # Query only data from the exact hour
        today = datetime.now(timezone.utc).date()

        query = f"""
            SELECT * FROM stock.ohlcv_data
            WHERE toDate(timestamp) = toDate('{today}') and category IN ('stocks', 'etfs', 'currencies', 'indices')
        """

        logger.info(f"Running query: {query}")
        result = clickhouse_client.query(query)

        if not result.result_rows:
            logger.warning(f"No data found for the time window for {today} .")
            return

        df = pd.DataFrame(result.result_rows, columns=result.column_names)  # Option 1

        # BigQuery setup
        bq_client = bigquery.Client()

        # Ensure the destination table exists, or else handle accordingly
        table_id = 'solarcropsanalysis-454507.ohlcv_dataset.stocks_ohlcv_table'
        logger.info(f"Loading data to BigQuery table: {table_id}")

        job = bq_client.load_table_from_dataframe(df, table_id)
        job.result()  # Wait for the job to complete

        logger.info(f"Loaded OHLCV data of {today}  into BigQuery.")

    except Exception as e:
        logger.error(f"Error occurred during ingestion: {str(e)}")
        raise  # Re-raise the exception for Airflow to capture failure

if __name__ == "__main__":
    ingest_data()
