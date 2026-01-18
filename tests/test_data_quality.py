import sys
import os
import pandas as pd
from datetime import datetime, timezone

sys.path.append("/opt/airflow")

from scripts.data_quality import apply_data_quality_checks


def test_valid_record_passes():
    df = pd.DataFrame([
        {
            "transaction_id": "t1",
            "customer_id": "c1",
            "product_id": "p1",
            "amount": 100.0,
            "transaction_date": "2026-01-01",
            "status": "SUCCESS",
            "updated_at": datetime.now(timezone.utc),
        }
    ])

    products = {"p1"}
    valid, errors = apply_data_quality_checks(df, products)

    assert len(valid) == 1
    assert errors.empty


def test_invalid_amount_fails():
    df = pd.DataFrame([
        {
            "transaction_id": "t2",
            "customer_id": "c2",
            "product_id": "p1",
            "amount": -10.0,
            "transaction_date": "2026-01-01",
            "status": "FAILED",
            "updated_at": datetime.now(timezone.utc),
        }
    ])

    products = {"p1"}
    valid, errors = apply_data_quality_checks(df, products)

    assert valid.empty
    assert not errors.empty
