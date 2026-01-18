import os
import logging
import pandas as pd
from sqlalchemy import create_engine, text

# ------------------ CONFIG ------------------
DATA_DIR = "/opt/airflow/data/raw"

DB_CONFIG = {
    "host": os.getenv("ANALYTICS_DB_HOST", "postgres-analytics"),
    "port": os.getenv("ANALYTICS_DB_PORT", "5432"),
    "user": os.getenv("ANALYTICS_DB_USER", "analytics"),
    "password": os.getenv("ANALYTICS_DB_PASSWORD", "analytics"),
    "database": os.getenv("ANALYTICS_DB_NAME", "analytics"),
}

STAGING_SCHEMA = "staging"
STAGING_TABLE = "transactions"

# ------------------ LOGGING ------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# ------------------ DB ENGINE ------------------
def get_engine():
    conn_str = (
        f"postgresql+psycopg2://{DB_CONFIG['user']}:"
        f"{DB_CONFIG['password']}@{DB_CONFIG['host']}:"
        f"{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )
    return create_engine(conn_str)


# ------------------ SCHEMA SAFETY ------------------
def ensure_staging_schema(engine):
    sql = f"CREATE SCHEMA IF NOT EXISTS {STAGING_SCHEMA};"
    with engine.begin() as conn:
        conn.execute(text(sql))
    logger.info("Schema '%s' ensured", STAGING_SCHEMA)


# ------------------ INCREMENTAL CHECK ------------------
def get_max_updated_at(engine):
    check_sql = """
    SELECT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'staging'
          AND table_name = 'transactions'
    );
    """

    with engine.connect() as conn:
        exists = conn.execute(text(check_sql)).scalar()

        if not exists:
            logger.info("Staging table does not exist yet → first load")
            return None

        return conn.execute(
            text("SELECT MAX(updated_at) FROM staging.transactions")
        ).scalar()


# ------------------ LOAD SOURCE FILES ------------------
def load_csv_files():
    files = sorted(
        os.path.join(DATA_DIR, f)
        for f in os.listdir(DATA_DIR)
        if f.endswith(".csv")
    )

    if not files:
        logger.info("No CSV files found")
        return pd.DataFrame()

    df = pd.concat(
        [pd.read_csv(f, parse_dates=["updated_at", "transaction_date"]) for f in files],
        ignore_index=True,
    )

    logger.info("Loaded %s total records from CSV files", len(df))
    return df


# ------------------ INCREMENTAL FILTER ------------------
def filter_incremental(df, max_updated_at):
    if df.empty:
        return df

    df["updated_at"] = pd.to_datetime(df["updated_at"], utc=True)

    if max_updated_at is None:
        logger.info("Full load (staging empty)")
        return df.drop_duplicates(subset=["transaction_id"])

    max_updated_at = pd.to_datetime(max_updated_at, utc=True)

    filtered = df[df["updated_at"] > max_updated_at]

    logger.info(
        "Incremental filter | Before: %s | After: %s",
        len(df),
        len(filtered),
    )

    return filtered.drop_duplicates(subset=["transaction_id"])


# ------------------ LOAD TO STAGING ------------------
def load_to_staging(df, engine):
    if df.empty:
        logger.info("No new records to insert")
        return 0

    df.to_sql(
        name=STAGING_TABLE,
        schema=STAGING_SCHEMA,
        con=engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000,
    )

    return len(df)


# ------------------ MAIN ------------------
def main():
    logger.info("Starting ingestion process")

    engine = get_engine()

    # ✅ CRITICAL FIX
    ensure_staging_schema(engine)

    max_updated_at = get_max_updated_at(engine)
    logger.info("Max updated_at in staging: %s", max_updated_at)

    source_df = load_csv_files()
    incremental_df = filter_incremental(source_df, max_updated_at)

    inserted = load_to_staging(incremental_df, engine)
    logger.info("Inserted %s records into staging", inserted)

    logger.info("Ingestion completed successfully")


if __name__ == "__main__":
    main()
