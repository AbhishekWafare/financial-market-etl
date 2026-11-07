# Inspired by Medallion Architecture (Bronze/Silver/Gold)

"""
Financial Market ETL Pipeline.

Orchestrates market data extraction, transformation, validation,
PostgreSQL loading, backups, and analytical processing.
"""

import os
from datetime import datetime, timedelta

from airflow.sdk import dag # type: ignore

from tasks.extract import extract
from tasks.transform import transform
from tasks.validate import validate
from tasks.load import load
from tasks.backup import backup_csv
from tasks.analytics import calculate_moving_averages
from tasks.correlation import correlation_analysis
from tasks.plot import plot
from tasks.failure_alert import failure_alert


default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

alert_email = os.getenv("ALERT_EMAIL")

if alert_email:
    default_args.update({
        "email": [alert_email],
        "email_on_failure": True,
        "email_on_retry": True,
    })


@dag(
    dag_id="new_tf_etl_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    on_failure_callback=failure_alert,
)
def financial_market_etl():

    raw_data = extract()

    transformed_data = transform(raw_data)

    validated_data = validate(transformed_data)

    load_task = load(validated_data)

    backup_task = backup_csv(validated_data)

    moving_avg_task = calculate_moving_averages()

    correlation_task = correlation_analysis()

    plot_task = plot()

    load_task >> [
        backup_task,
        moving_avg_task,
        correlation_task,
    ]

    moving_avg_task >> plot_task


financial_market_etl()
