# StockStream

## 📝 Project Overview

This project implements a real-time data pipeline for processing **OHLCV (Open, High, Low, Close, Volume)** data for stocks, cryptocurrencies, ETFs, indices, and currencies. The pipeline streams, processes, transforms, and stores financial market data using modern data engineering tools.

---

## ⚙️ Architecture

1. **Kafka Producer**: Streams live OHLCV data.
2. **Kafka Broker**: Handles message delivery.
3. **Spark Structured Streaming**: Consumes Kafka topics and processes OHLCV data in real-time.
4. **ClickHouse**: Stores processed data for fast analytics.
5. **Airflow**: Orchestrates data ingestion from ClickHouse to BigQuery.
6. **BigQuery**: Data warehouse for advanced analytics and reporting.
7. **dbt**: Data modeling, transformation, and quality checks in BigQuery.
8. **Docker**: Containerizes all components for easy deployment.
9. **GCP VM**: Hosts and runs the pipeline infrastructure.

---

## 🧰 Technologies Used

- **Apache Kafka**: Real-time messaging
- **Apache Spark Structured Streaming**: Stream processing
- **ClickHouse**: Analytical database
- **Apache Airflow**: Workflow orchestration
- **Google BigQuery**: Cloud data warehouse
- **dbt (data build tool)**: Data transformation
- **Docker**: Containerization
- **Google Cloud Platform (GCP) VM**: Hosting/compute

---

## 🚀 Setup Instructions

1. **Clone the Repository**
    ```bash
    git clone <your-repo-url>
    cd <project-folder>
    ```

2. **Environment Variables**
    - Configure `.env` files or set environment variables for Kafka, ClickHouse, BigQuery credentials, etc.

3. **Build and Run with Docker**
    ```bash
    docker-compose up --build
    ```

4. **Initialize Airflow**
    - Set up Airflow by initializing the database and starting the webserver and scheduler.

5. **Start the Pipeline**
    - Start the Kafka producer (simulate or connect to a live data feed).
    - Spark will begin consuming data automatically.
    - Airflow DAGs will orchestrate ETL from ClickHouse to BigQuery and trigger dbt transformations.

---

## 📈 Usage

- Access **Airflow UI** at [http://localhost:8080](http://localhost:8080)
- Monitor **Kafka** and **Spark** logs for streaming status.
- Query **ClickHouse** for raw OHLCV data.
- Query **BigQuery** for transformed and analytical data.

---

## 📂 Directory Structure

    ├── airflow/
    │ └── dags/
    ├── consumer/
    ├── dbt/
    ├── docker/
    ├── producer/
    ├── .env
    ├── docker-compose.airflow.yml
    ├── docker-compose.yml
    ├── requirements.txt
    ├── README.md

---

## 🤝 Contributing & Contact

- Pull requests are welcome.  
- For major changes, please open an issue first.
