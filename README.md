# Orchestrate Incremental Data Sync with Airflow and Robust Data Quality Gates

## Project Overview
This project implements a production-grade incremental ETL pipeline for e-commerce transaction data using Apache Airflow, PostgreSQL, Docker, and Python.
The pipeline ingests daily CSV transaction data, performs robust data quality checks, quarantines invalid records, and loads validated data into a partitioned fact table using an idempotent UPSERT strategy.

This solution is fully Dockerized, reproducible, and follows data engineering best practices.

---

## Architecture Overview

data/raw (CSV files)
        ↓
ingest.py (incremental ingestion)
        ↓
staging.transactions
        ↓
data_quality.py
        ↓                     ↓
Valid Records           Invalid Records
        ↓                     ↓
fact_transactions   quarantine.transactions_errors

---

## Project Structure

my_etl_pipeline/
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── .env.example
├── dags/
│   └── e-commerce_transactions_etl.py
├── scripts/
│   ├── generate_data.py
│   ├── ingest.py
│   ├── data_quality.py
│   └── load_fact.py
├── sql/
│   ├── create_tables.sql
│   └── init_db.sql
├── data/
│   └── raw/
├── tests/
│   ├── test_ingest.py
│   └── test_data_quality.py
└── README.md

---

## Setup Instructions

### Prerequisites
- Docker
- Docker Compose
- Git

### Clone Repository
git clone <repository-url>
cd my_etl_pipeline

### Environment Variables
cp .env.example .env

All credentials and sensitive values are managed using environment variables only.

### Start Services
docker-compose up --build -d

### Airflow UI
http://localhost:8081

---

## Database Schemas

### staging.transactions
- transaction_id (VARCHAR)
- customer_id (VARCHAR)
- product_id (VARCHAR)
- amount (NUMERIC)
- transaction_date (DATE)
- status (VARCHAR)
- updated_at (TIMESTAMP WITH TIME ZONE)

### public.fact_transactions
- Partitioned by transaction_date
- UPSERT logic for idempotency
- Indexed on transaction_date and customer_id

### quarantine.transactions_errors
- error_message
- error_timestamp

### public.products
- product_id (PK)
- product_name

---

## Incremental Loading Strategy
- Uses transaction_id and updated_at
- High-water mark tracked from staging table
- Processes only new or updated records
- Fully idempotent DAG runs

---

## Data Quality Checks
1. Schema validation
2. Uniqueness check
3. Completeness check
4. Amount > 0 validation
5. Referential integrity check

Invalid records are quarantined without stopping the pipeline.

---

## Airflow DAG
DAG Name: ecommerce_transactions_etl
Schedule: Daily

Tasks:
1. generate_source_data
2. ingest_staging
3. run_data_quality
4. quarantine_errors
5. load_fact_table

---

## Testing
pytest tests/

---

## Author
Malla Charmi
Data Engineering | Cloud Computing

This project fully satisfies all Partnr evaluation requirements.
