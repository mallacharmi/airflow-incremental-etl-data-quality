import os
import random
import uuid
from datetime import datetime, timedelta

import pandas as pd

DATA_DIR = "/opt/airflow/data/raw"
PRODUCT_IDS = [f"P{i}" for i in range(1, 11)]
STATUSES = ["SUCCESS", "FAILED", "PENDING"]

os.makedirs(DATA_DIR, exist_ok=True)


def get_existing_files():
    return sorted(
        [
            f for f in os.listdir(DATA_DIR)
            if f.startswith("daily_transactions_") and f.endswith(".csv")
        ]
    )


def load_previous_data():
    files = get_existing_files()
    if not files:
        return pd.DataFrame()
    return pd.read_csv(os.path.join(DATA_DIR, files[-1]))


def generate_new_transactions(n, transaction_date):
    records = []
    for _ in range(n):
        records.append({
            "transaction_id": str(uuid.uuid4()),
            "customer_id": f"C{random.randint(1, 100)}",
            "product_id": random.choice(PRODUCT_IDS),
            "amount": round(random.uniform(100, 5000), 2),
            "transaction_date": transaction_date,
            "status": random.choice(STATUSES),
            "updated_at": datetime.utcnow().isoformat()
        })
    return pd.DataFrame(records)


def update_existing_transactions(df, update_ratio=0.2):
    if df.empty:
        return df

    df = df.copy()
    update_count = max(1, int(len(df) * update_ratio))
    indices = random.sample(list(df.index), update_count)

    for idx in indices:
        df.loc[idx, "amount"] = round(random.uniform(100, 5000), 2)
        df.loc[idx, "status"] = random.choice(STATUSES)
        df.loc[idx, "updated_at"] = datetime.utcnow().isoformat()

    return df.loc[indices]


def main():
    now = datetime.utcnow()
    run_ts = now.strftime("%Y-%m-%d_%H-%M-%S")

    file_name = f"daily_transactions_{run_ts}.csv"
    file_path = os.path.join(DATA_DIR, file_name)

    previous_df = load_previous_data()

    new_df = generate_new_transactions(
        n=20,
        transaction_date=now.date()
    )

    updated_df = update_existing_transactions(previous_df)

    final_df = pd.concat([new_df, updated_df], ignore_index=True)

    final_df.to_csv(file_path, index=False)
    print(f"Generated {len(final_df)} records → {file_path}")



if __name__ == "__main__":
    main()
