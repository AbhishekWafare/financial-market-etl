
import logging
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
from airflow.sdk import task # type: ignore


@task
def backup_csv(data):
    """Save validated financial market data as a timestamped CSV backup."""

    logging.info("Financial market CSV backup started")

    if not isinstance(data, dict) or not data:
        raise ValueError("Backup data must be a non-empty dictionary.")

    backup_dir = Path(__file__).resolve().parents[1] / "backup"
    backup_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(
        ZoneInfo("Asia/Kolkata")
    ).strftime("%Y_%m_%d_%H_%M_%S")

    backup_file = backup_dir / f"market_data_{timestamp}.csv"

    try:
        df = pd.DataFrame([data])
        df.to_csv(backup_file, index=False)

        logging.info(
            "CSV backup completed successfully: %s",
            backup_file
        )

        return str(backup_file)

    except Exception:
        logging.exception("Financial market CSV backup failed")
        raise
