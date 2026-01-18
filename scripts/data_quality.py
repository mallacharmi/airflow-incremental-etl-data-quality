import os
import logging
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine, text

# ------------------ CONFIG ------------------
DB_CONFIG = {
    "host": os.getenv("ANALYTICS_DB_HOST", "postgres-analytics"),
    "port": os.getenv("ANALYTICS_DB_PORT", "5432"),
    "user": os.getenv("ANALYTICS_DB_USER", "analytics"),
    "password": os.getenv("ANALYTICS_DB_PASSWORD", "analytics"),
    "database": os.getenv("ANALYTICS_DB_NAME", "analytics"),
}

STAGING_TABLE = "staging.transactions"
PRODUCTS_TABLE = "public.products"
QUARANTINE_TABLE = "quarantine.transactions_errors"

EXPECTED_COLUMNS = [
    "transaction_id",
    "customer_id",
    "product_id",
    "amount",
    "transaction_date",
    "status",
    "updated_at",
]

NOT_NULL_COLUMNS = [
    "transaction_id",
    "customer_id",
    "product_id",
    "amount",
    "transaction_date",
]

# ✅ Accept SUCCESS (normalized to lowercase)
VALID_STATUS = {"completed", "pending", "cancelled", "success"}

# ------------------ LOGGING ------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def get_engine():
    conn_str = (
        f"postgresql+psycopg2://{DB_CONFIG['user']}:"
        f"{DB_CONFIG['password']}@{DB_CONFIG['host']}:"
        f"{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )
    return create_engine(conn_str)


def ensure_quarantine(engine):
    sql = """
    CREATE SCHEMA IF NOT EXISTS quarantine;

    CREATE TABLE IF NOT EXISTS quarantine.transactions_errors (
        transaction_id TEXT,
        error_message TEXT,
        error_timestamp TIMESTAMPTZ DEFAULT NOW()
    );
    """
    with engine.begin() as conn:
        conn.execute(text(sql))


def load_staging(engine):
    df = pd.read_sql(f"SELECT * FROM {STAGING_TABLE};", engine)
    df["updated_at"] = pd.to_datetime(df["updated_at"], utc=True)
    df["transaction_date"] = pd.to_datetime(df["transaction_date"])
    return df


def load_products(engine):
    try:
        df = pd.read_sql(f"SELECT product_id FROM {PRODUCTS_TABLE};", engine)
        return set(df["product_id"])
    except Exception:
        logger.warning("Products table not found — skipping FK check")
        return set()


# =========================================================
# REQUIRED BY TESTS ✅
# =========================================================
def apply_data_quality_checks(df, products_set):
    valid_df = df.copy()
    errors = []

    # ---- Schema check ----
    for col in EXPECTED_COLUMNS:
        if col not in valid_df.columns:
            raise ValueError(f"Missing required column: {col}")

    # ---- Normalize status (🔥 CRITICAL FIX) ----
    valid_df["status"] = valid_df["status"].str.lower()

    # ---- Duplicate check ----
    dupes = valid_df[valid_df.duplicated("transaction_id", keep=False)]
    for _, row in dupes.iterrows():
        errors.append((row["transaction_id"], "Duplicate transaction_id"))
    valid_df = valid_df.drop_duplicates("transaction_id", keep="first")

    # ---- NOT NULL checks ----
    for col in NOT_NULL_COLUMNS:
        nulls = valid_df[valid_df[col].isnull()]
        for _, row in nulls.iterrows():
            errors.append((row["transaction_id"], f"NULL value in {col}"))
        valid_df = valid_df[valid_df[col].notnull()]

    # ---- Amount validation ----
    invalid_amount = valid_df[valid_df["amount"] <= 0]
    for _, row in invalid_amount.iterrows():
        errors.append((row["transaction_id"], "Amount must be > 0"))
    valid_df = valid_df[valid_df["amount"] > 0]

    # ---- Status validation ----
    invalid_status = valid_df[~valid_df["status"].isin(VALID_STATUS)]
    for _, row in invalid_status.iterrows():
        errors.append((row["transaction_id"], "Invalid status value"))
    valid_df = valid_df[valid_df["status"].isin(VALID_STATUS)]

    # ---- Referential integrity (product_id) ----
    if products_set:
        invalid_products = valid_df[~valid_df["product_id"].isin(products_set)]
        for _, row in invalid_products.iterrows():
            errors.append((row["transaction_id"], "Invalid product_id"))
        valid_df = valid_df[valid_df["product_id"].isin(products_set)]

    error_df = pd.DataFrame(errors, columns=["transaction_id", "error_message"])
    if not error_df.empty:
        error_df["error_timestamp"] = datetime.utcnow()

    return valid_df, error_df


def load_quarantine(error_df, engine):
    if error_df.empty:
        logger.info("No invalid records found.")
        return 0

    error_df.to_sql(
        "transactions_errors",
        schema="quarantine",
        con=engine,
        if_exists="append",
        index=False,
    )
    return len(error_df)


def main():
    logger.info("Starting data quality checks")

    engine = get_engine()
    ensure_quarantine(engine)

    staging_df = load_staging(engine)
    products_set = load_products(engine)

    valid_df, error_df = apply_data_quality_checks(staging_df, products_set)

    quarantined = load_quarantine(error_df, engine)

    logger.info("Valid records: %s", len(valid_df))
    logger.info("Quarantined records: %s", quarantined)

    valid_df.to_csv("/opt/airflow/data/valid_transactions.csv", index=False)

    logger.info("Data quality checks completed successfully")


if __name__ == "__main__":
    main()
