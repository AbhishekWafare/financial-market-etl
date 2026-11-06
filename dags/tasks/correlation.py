
import logging
import math
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
from airflow.sdk import task #type: ignore

from tasks.db_connection import get_connection


@task
def correlation_analysis():
    """Calculate Bitcoin-Gold price correlation and store the result."""

    logging.info("Bitcoin-Gold correlation analysis started")

    conn = None

    try:
        conn = get_connection()

        query = """
            SELECT bitcoin_price, gold_price
            FROM tf_silver_market_data
            ORDER BY time;
        """

        df = pd.read_sql_query(query, conn)

        if df.empty:
            raise ValueError(
                "No market data available for correlation analysis."
            )

        # Ensure both price columns contain valid numeric values.
        for column in ["bitcoin_price", "gold_price"]:
            df[column] = pd.to_numeric(
                df[column], errors="coerce"
            )

        df = df.replace([float("inf"), float("-inf")], float("nan"))
        df = df.dropna(
            subset=["bitcoin_price", "gold_price"]
        )

        if len(df) < 2:
            raise ValueError(
                "At least two valid paired observations are required."
            )

        correlation = df["bitcoin_price"].corr(
            df["gold_price"]
        )

        if not math.isfinite(correlation):
            raise ValueError(
                "Correlation could not be calculated. "
                "Check whether price values have sufficient variation."
            )

        correlation = round(float(correlation), 4)

        # Store the local execution timestamp as a naive timestamp.
        run_time = datetime.now(
            ZoneInfo("Asia/Kolkata")
        ).replace(tzinfo=None)

        create_table_query = """
            CREATE TABLE IF NOT EXISTS tf_market_correlation (
                run_time TIMESTAMP,
                correlation FLOAT
            );
        """

        insert_query = """
            INSERT INTO tf_market_correlation (
                run_time, correlation
            )
            VALUES (%s, %s);
        """

        with conn:
            with conn.cursor() as cursor:
                cursor.execute(create_table_query)

                cursor.execute(
                    insert_query,
                    (run_time, correlation)
                )

        logging.info(
            "Bitcoin-Gold correlation calculated successfully: %s",
            correlation
        )

    except Exception:
        logging.exception("Correlation analysis failed")
        raise

    finally:
        if conn is not None:
            conn.close()
