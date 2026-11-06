
import logging

import pandas as pd
from airflow.sdk import task # type: ignore
from psycopg2.extras import execute_values

from tasks.db_connection import get_connection


SOURCE_COLUMNS = [
    "time",
    "bitcoin_price",
    "gold_price",
    "ethereum_price",
    "silver_price",
    "oil_price",
    "sp500_price",
]

MOVING_AVERAGE_COLUMNS = {
    "bitcoin_price": ("btc_ma_7", "btc_ma_30"),
    "gold_price": ("gold_ma_7", "gold_ma_30"),
    "ethereum_price": ("ethe_ma_7", "ethe_ma_30"),
    "silver_price": ("silver_ma_7", "silver_ma_30"),
    "oil_price": ("oil_ma_7", "oil_ma_30"),
    "sp500_price": ("sp_ma_7", "sp_ma_30"),
}

OUTPUT_COLUMNS = SOURCE_COLUMNS + [
    column
    for pair in MOVING_AVERAGE_COLUMNS.values()
    for column in pair
]


@task
def calculate_moving_averages():
    """Calculate moving averages and load analytical data into PostgreSQL."""

    logging.info("Moving average calculation started")

    conn = None

    try:
        conn = get_connection()

        query = """
            SELECT
                time,
                bitcoin_price,
                gold_price,
                ethereum_price,
                silver_price,
                oil_price,
                sp500_price
            FROM tf_silver_market_data
            ORDER BY time;
        """

        df = pd.read_sql_query(query, conn)

        if df.empty:
            raise ValueError(
                "No market data available in tf_silver_market_data."
            )

        # Calculate 7-record and 30-record moving averages.
        for price_column, (ma_7, ma_30) in MOVING_AVERAGE_COLUMNS.items():
            df[ma_7] = (
                df[price_column]
                .rolling(window=7)
                .mean()
                .round(2)
            )

            df[ma_30] = (
                df[price_column]
                .rolling(window=30)
                .mean()
                .round(2)
            )

        df = df[OUTPUT_COLUMNS]

        # Convert Pandas / NumPy values into PostgreSQL-compatible values.
        records = []

        for row in df.itertuples(index=False, name=None):
            values = []

            for value in row:
                if pd.isna(value):
                    values.append(None)
                elif isinstance(value, pd.Timestamp):
                    values.append(value.to_pydatetime())
                elif hasattr(value, "item"):
                    values.append(value.item())
                else:
                    values.append(value)

            records.append(tuple(values))

        create_table_query = """
            CREATE TABLE IF NOT EXISTS tf_gold_market_analytics (
                time TIMESTAMP,
                bitcoin_price FLOAT,
                gold_price FLOAT,
                ethereum_price FLOAT,
                silver_price FLOAT,
                oil_price FLOAT,
                sp500_price FLOAT,
                btc_ma_7 FLOAT,
                btc_ma_30 FLOAT,
                gold_ma_7 FLOAT,
                gold_ma_30 FLOAT,
                ethe_ma_7 FLOAT,
                ethe_ma_30 FLOAT,
                silver_ma_7 FLOAT,
                silver_ma_30 FLOAT,
                oil_ma_7 FLOAT,
                oil_ma_30 FLOAT,
                sp_ma_7 FLOAT,
                sp_ma_30 FLOAT
            );
        """

        insert_query = """
            INSERT INTO tf_gold_market_analytics (
                time,
                bitcoin_price,
                gold_price,
                ethereum_price,
                silver_price,
                oil_price,
                sp500_price,
                btc_ma_7,
                btc_ma_30,
                gold_ma_7,
                gold_ma_30,
                ethe_ma_7,
                ethe_ma_30,
                silver_ma_7,
                silver_ma_30,
                oil_ma_7,
                oil_ma_30,
                sp_ma_7,
                sp_ma_30
            )
            VALUES %s;
        """

        # Replace the existing analytical snapshot atomically.
        with conn:
            with conn.cursor() as cursor:
                cursor.execute(create_table_query)

                cursor.execute(
                    "TRUNCATE TABLE tf_gold_market_analytics;"
                )

                execute_values(
                    cursor,
                    insert_query,
                    records,
                    page_size=1000
                )

        logging.info(
            "Moving averages calculated successfully. "
            "Records processed: %s",
            len(records)
        )

    except Exception:
        logging.exception("Moving average calculation failed")
        raise

    finally:
        if conn is not None:
            conn.close()
