
import logging
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt

from airflow.sdk import task # type: ignore
from tasks.db_connection import get_connection


PRICE_COLUMNS = {
    "price_usd": "Bitcoin",
    "price_gold": "Gold",
    "price_ethereum": "Ethereum",
    "price_silver": "Silver",
    "price_oil": "Crude Oil",
    "price_sp500": "S&P 500",
}


@task
def plot():
    """Generate a normalized financial market price comparison chart."""

    logging.info("Financial market visualization started")

    conn = None

    try:
        conn = get_connection()

        query = """
            SELECT
                time,
                price_usd,
                price_gold,
                price_ethereum,
                price_silver,
                price_oil,
                price_sp500
            FROM tf_bitcoin_price
            ORDER BY time;
        """

        df = pd.read_sql_query(query, conn)

    except Exception:
        logging.exception("Failed to retrieve market data for plotting")
        raise

    finally:
        if conn is not None:
            conn.close()

    if df.empty:
        raise ValueError(
            "No market data available for visualization."
        )

    df["time"] = pd.to_datetime(
        df["time"], errors="coerce"
    )

    df = df.dropna(subset=["time"]).sort_values("time")

    if df.empty:
        raise ValueError("No valid timestamps available for plotting.")

    output_dir = Path(__file__).resolve().parents[1] / "plots"
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / "gold_vs_bitcoin_etl.png"

    fig, ax = plt.subplots(figsize=(13, 7))

    plotted_assets = 0

    try:
        for column, label in PRICE_COLUMNS.items():

            prices = pd.to_numeric(
                df[column], errors="coerce"
            )

            prices = prices.replace(
                [float("inf"), float("-inf")], float("nan")
            )

            valid_mask = prices.notna() & (prices > 0)

            asset_prices = prices[valid_mask]
            asset_times = df.loc[valid_mask, "time"]

            if asset_prices.empty:
                logging.warning(
                    "No valid price records available for %s",
                    label
                )
                continue

            # Normalize each asset against its first valid observation.
            normalized_prices = (
                asset_prices / asset_prices.iloc[0]
            ) * 100

            ax.plot(
                asset_times,
                normalized_prices,
                label=label,
                linewidth=2
            )

            plotted_assets += 1

        if plotted_assets == 0:
            raise ValueError(
                "No valid financial market prices available for plotting."
            )

        ax.set_xlabel("Date")
        ax.set_ylabel("Normalized Price Index (First Value = 100)")
        ax.set_title("Financial Market Performance Comparison")

        ax.legend()
        ax.grid(True, alpha=0.3)

        fig.autofmt_xdate()
        fig.tight_layout()

        fig.savefig(
            output_file,
            dpi=150,
            bbox_inches="tight"
        )

        logging.info(
            "Financial market visualization saved: %s",
            output_file
        )

    finally:
        plt.close(fig)
