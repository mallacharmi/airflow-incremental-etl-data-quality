import sys
import os
import pandas as pd
from datetime import datetime, timezone

sys.path.append("/opt/airflow")

from scripts.ingest import filter_incremental


def test_filter_incremental_empty():
    df = pd.DataFrame()
    result = filter_incremental(df, None)
    assert result.empty


def test_filter_incremental_new_records():
    data = {
        "transaction_id": ["t1", "t2"],
        "updated_at": [
            datetime(2026, 1, 1, tzinfo=timezone.utc),
            datetime(2026, 1, 2, tzinfo=timezone.utc),
        ],
    }
    df = pd.DataFrame(data)

    max_updated_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    result = filter_incremental(df, max_updated_at)

    assert len(result) == 1
    assert result.iloc[0]["transaction_id"] == "t2"
