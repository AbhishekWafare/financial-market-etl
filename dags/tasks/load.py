
import json
import logging
from datetime import datetime

from airflow.sdk import task # type: ignore

from tasks.db_connection import get_connection


def _record_failure():
    """Record a failed ETL load in the PostgreSQL logging table."""

    conn = get_connection()

    try:
        with conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS tf_etl_logs (
                        run_time TIMESTAMP,
                        status VARCHAR(20),
                        records_loaded INT
                    );
                """)

                cursor.execute("""
                    INSERT INTO tf_etl_logs
                    (run_time, status, records_loaded)
                    VALUES (%s, %s, %s);
                """, (datetime.now(), "FAILED", 0))

    finally:
        conn.close()


@task
def load(data):
    """Load financial market data into Bronze, Silver, and reporting tables."""

    logging.info("PostgreSQL load started")

    conn = None

    try:
        conn = get_connection()

        with conn:
            with conn.cursor() as cursor:

                # Bitcoin price table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS tf_bitcoin_price (
                        time TIMESTAMP PRIMARY KEY,
                        price_usd FLOAT,
                        price_gold FLOAT,
                        price_ethereum FLOAT,
                        price_silver FLOAT,
                        price_oil FLOAT,
                        price_sp500 FLOAT
                    );
                """)

                cursor.execute("""
                    INSERT INTO tf_bitcoin_price (
                        time, price_usd, price_gold,
                        price_ethereum, price_silver,
                        price_oil, price_sp500
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (time) DO NOTHING;
                """, (
                    data["time"],
                    data["price_usd"],
                    data["price_gold"],
                    data["price_ethereum"],
                    data["price_silver"],
                    data["price_oil"],
                    data["price_sp500"]
                ))

                # Bronze layer: raw ingestion
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS tf_bronze_market_data (
                        id SERIAL PRIMARY KEY,
                        ingestion_time TIMESTAMP,
                        raw_json JSONB
                    );
                """)

                cursor.execute("""
                    INSERT INTO tf_bronze_market_data (
                        ingestion_time, raw_json
                    )
                    VALUES (%s, %s);
                """, (
                    datetime.now(),
                    json.dumps(data)
                ))

                # Silver layer: cleaned market data
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS tf_silver_market_data (
                        time TIMESTAMP PRIMARY KEY,
                        bitcoin_price FLOAT,
                        gold_price FLOAT,
                        ethereum_price FLOAT,
                        silver_price FLOAT,
                        oil_price FLOAT,
                        sp500_price FLOAT
                    );
                """)

                cursor.execute("""
                    INSERT INTO tf_silver_market_data (
                        time, bitcoin_price, gold_price,
                        ethereum_price, silver_price,
                        oil_price, sp500_price
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (time) DO NOTHING;
                """, (
                    data["time"],
                    data["price_usd"],
                    data["price_gold"],
                    data["price_ethereum"],
                    data["price_silver"],
                    data["price_oil"],
                    data["price_sp500"]
                ))

                # ETL execution log
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS tf_etl_logs (
                        run_time TIMESTAMP,
                        status VARCHAR(20),
                        records_loaded INT
                    );
                """)

                cursor.execute("""
                    INSERT INTO tf_etl_logs (
                        run_time, status, records_loaded
                    )
                    VALUES (%s, %s, %s);
                """, (datetime.now(), "SUCCESS", 1))

        logging.info("Financial market data loaded successfully")

    except Exception:
        logging.exception("PostgreSQL load failed")

        try:
            _record_failure()
        except Exception:
            logging.exception("Could not record ETL failure in PostgreSQL")

        raise

    finally:
        if conn is not None:
            conn.close()
