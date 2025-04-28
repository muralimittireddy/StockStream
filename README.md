# StockStream
# Stock and Crypto Streaming Platform 🚀

A real-time data processing platform built with **Apache Spark**, **Kafka**, **ClickHouse**, and **Docker**. The project is designed to consume, process, and store live stock and cryptocurrency market data, with plans to incorporate advanced analytics and alerting.

---

## 📚 Project Overview

This platform currently:
- Streams real-time financial market data using a Kafka **producer**.
- Processes streaming data using a Spark Structured Streaming **consumer**.
- Stores processed data into a **ClickHouse** database.
- Runs fully containerized with **Docker Compose**.

Upcoming features:
- Scraping latest **financial news**.
- Extracting **historical market data**.
- **DBT** (Data Build Tool) transformations for cleaned, modeled datasets.
- **Grafana** dashboards for data visualization.
- **Machine Learning** to detect price anomalies.
- **Alerting** systems via email or Slack for market events.

---

## ⚙️ Tech Stack

- **Apache Kafka** - Messaging system for streaming stock and crypto data.
- **Apache Spark** (Structured Streaming) - Real-time data processing.
- **ClickHouse** - High-performance OLAP database.
- **Docker & Docker Compose** - For containerization and orchestration.
- **Python** - Scripts for producer, consumer, and utilities.
- **DBT** (coming soon) - Data transformations and modeling.
- **Grafana** (coming soon) - Dashboard visualizations.
- **Scikit-learn / PyTorch** (planned) - Anomaly detection model.
- **Slack API / SMTP** (planned) - For real-time alerting.

---

## 🐳 Quick Start (Running Locally)

1.Clone the repository:

    git clone https://github.com/muralimittireddy/StockStream.git
    cd StockStream
    
2.Start services using Docker Compose:

    docker-compose up --build
    
3.Access:

  - Spark Master UI: http://localhost:8080
      
  - ClickHouse UI (if set up): http://localhost:8123
      
  - Kafka Broker: http://localhost:29092
