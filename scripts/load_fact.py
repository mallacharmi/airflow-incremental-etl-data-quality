import os
import logging

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# ------------------ CONFIG ------------------
VALID_DATA_PATH = "/opt/airflow/data/valid_transactions.csv"

DB_CONFIG = {
    "host": os.getenv("ANALYTICS_DB_HOST", "postgres-analytics"),
    "port": os.getenv("ANALYTICS_DB_PORT", "5432"),
    "user": os.getenv("ANALYTICS_DB_USER", "analytics"),
    "password": os.getenv("ANALYTICS_DB_PASSWORD", "analytics"),
    "database": os.getenv("ANALYTICS_DB_NAME", "analytics"),
}

FACT_TABLE = "public.fact_transactions"

# ------------------ LOGGING ------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def get_connection():
    return psycopg2.connect(
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        dbname=DB_CONFIG["database"],
    )


def load_valid_data():
    if not os.path.exists(VALID_DATA_PATH):
        logger.info("No valid data file found. Skipping fact load.")
        return pd.DataFrame()

    df = pd.read_csv(
        VALID_DATA_PATH,
        parse_dates=["transaction_date", "updated_at"]
    )
    return df


def upsert_fact_table(df):
    if df.empty:
        logger.info("No valid records to load into fact table.")
        return 0

    columns = [
        "transaction_id",
        "customer_id",
        "product_id",
        "amount",
        "transaction_date",
        "status",
        "updated_at",
    ]

    values = [tuple(row[col] for col in columns) for _, row in df.iterrows()]

    insert_sql = f"""
        INSERT INTO {FACT_TABLE} (
            transaction_id,
            customer_id,
            product_id,
            amount,
            transaction_date,
            status,
            updated_at
        )
        VALUES %s
        ON CONFLICT (transaction_id, transaction_date)
        DO UPDATE SET
            customer_id = EXCLUDED.customer_id,
            product_id = EXCLUDED.product_id,
            amount = EXCLUDED.amount,
            status = EXCLUDED.status,
            updated_at = EXCLUDED.updated_at;
    """

    conn = get_connection()
    cur = conn.cursor()

    execute_values(cur, insert_sql, values, page_size=1000)
    conn.commit()

    cur.close()
    conn.close()

    return len(df)


def main():
    logger.info("Starting fact table load")

    df = load_valid_data()
    loaded = upsert_fact_table(df)

    logger.info("Upserted %s records into fact table", loaded)
    logger.info("Fact table load completed successfully")


if __name__ == "__main__":
    main()
