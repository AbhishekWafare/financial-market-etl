
import logging
import os
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import psycopg2
import streamlit as st
from dotenv import load_dotenv # type: ignore


# --------------------------------------------------
# CONFIGURATION
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[1]

load_dotenv(PROJECT_ROOT / ".env")

st.set_page_config(
    page_title="Financial Market Dashboard",
    page_icon="📈",
    layout="wide"
)

PRICE_COLUMNS = {
    "price_usd": "Bitcoin",
    "price_gold": "Gold",
    "price_ethereum": "Ethereum",
    "price_silver": "Silver",
    "price_oil": "Crude Oil",
    "price_sp500": "S&P 500",
}


# --------------------------------------------------
# DATABASE CONNECTION
# --------------------------------------------------

def get_connection():
    """Create a PostgreSQL connection using environment variables."""

    password = os.getenv("POSTGRES_PASSWORD")

    if not password:
        raise ValueError(
            "POSTGRES_PASSWORD is not configured."
        )

    return psycopg2.connect(
        host=os.getenv("POSTGRES_EXTERNAL_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5433")),
        database=os.getenv("POSTGRES_DB", "airflow"),
        user=os.getenv("POSTGRES_USER", "airflow"),
        password=password,
        connect_timeout=10
    )


# --------------------------------------------------
# LOAD MARKET DATA
# --------------------------------------------------

@st.cache_data(ttl=60)
def load_market_data():
    """Fetch the latest financial market records from PostgreSQL."""

    conn = None

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
        ORDER BY time DESC
        LIMIT 30;
    """

    try:
        conn = get_connection()

        with conn.cursor() as cursor:
            cursor.execute(query)

            records = cursor.fetchall()

            columns = [
                description[0]
                for description in cursor.description
            ]

        df = pd.DataFrame(
            records,
            columns=columns
        )

        return df

    except Exception:
        logging.exception("Failed to load financial market data")
        raise

    finally:
        if conn is not None:
            conn.close()


# --------------------------------------------------
# DASHBOARD HEADER
# --------------------------------------------------

st.title("📈 Financial Market Dashboard")

st.caption(
    "Financial market analysis powered by "
    "Apache Airflow, PostgreSQL, Python, and Streamlit."
)

try:
    df = load_market_data()

except Exception:
    st.error(
        "Unable to connect to PostgreSQL or retrieve market data. "
        "Check your database configuration and running containers."
    )
    st.stop()


if df.empty:
    st.warning(
        "No financial market records are currently available."
    )
    st.stop()


# --------------------------------------------------
# DATA PREPARATION
# --------------------------------------------------

df["time"] = pd.to_datetime(
    df["time"],
    errors="coerce"
)

df = df.dropna(
    subset=["time"]
).sort_values("time")

for column in PRICE_COLUMNS:
    df[column] = pd.to_numeric(
        df[column],
        errors="coerce"
    )

if df.empty:
    st.warning("No valid market records are available.")
    st.stop()


latest_record = df.iloc[-1]


# --------------------------------------------------
# MARKET OVERVIEW
# --------------------------------------------------

st.subheader("Latest Market Prices")

asset_names = list(PRICE_COLUMNS.values())

for start in range(0, len(asset_names), 3):

    columns = st.columns(3)

    for column, asset in zip(
        columns,
        asset_names[start:start + 3]
    ):

        price_column = next(
            key for key, value in PRICE_COLUMNS.items()
            if value == asset
        )

        value = latest_record[price_column]

        with column:
            if pd.notna(value):
                st.metric(
                    label=asset,
                    value=f"{value:,.2f}"
                )
            else:
                st.metric(
                    label=asset,
                    value="N/A"
                )


st.caption(
    f"Latest available record: "
    f"{latest_record['time']}"
)


# --------------------------------------------------
# NORMALIZED PRICE PERFORMANCE
# --------------------------------------------------

st.subheader("Financial Market Performance")

st.caption(
    "All assets are normalized to an initial index value of 100. "
    "This compares relative price movements rather than "
    "absolute market prices."
)

normalized_df = pd.DataFrame({
    "time": df["time"]
})

for column, asset in PRICE_COLUMNS.items():

    prices = df[column]

    valid_prices = (
        prices.notna()
        & (prices > 0)
        & prices.map(
            lambda value: pd.notna(value)
            and abs(value) != float("inf")
        )
    )

    if not valid_prices.any():
        continue

    first_price = prices[valid_prices].iloc[0]

    normalized_df[asset] = (
        prices / first_price
    ) * 100


fig, ax = plt.subplots(figsize=(13, 6))

plotted_assets = 0

for asset in PRICE_COLUMNS.values():

    if asset not in normalized_df.columns:
        continue

    if normalized_df[asset].notna().any():

        ax.plot(
            normalized_df["time"],
            normalized_df[asset],
            label=asset,
            linewidth=2
        )

        plotted_assets += 1


if plotted_assets > 0:

    ax.set_xlabel("Date")
    ax.set_ylabel("Normalized Price Index")
    ax.set_title("Relative Financial Market Performance")

    ax.axhline(
        y=100,
        linestyle="--",
        linewidth=1,
        alpha=0.6
    )

    ax.legend()
    ax.grid(True, alpha=0.3)

    fig.autofmt_xdate()
    fig.tight_layout()

    st.pyplot(fig)
    plt.close(fig)

else:
    plt.close(fig)
    st.warning("No valid prices available for visualization.")


# --------------------------------------------------
# MARKET DATA TABLE
# --------------------------------------------------

st.subheader("Latest Market Records")

st.dataframe(
    df.sort_values(
        "time",
        ascending=False
    ).round(2),
    use_container_width=True,
    hide_index=True
)


# --------------------------------------------------
# FOOTER
# --------------------------------------------------

st.caption(
    "Data source: Financial Market ETL Pipeline | "
    "Orchestration: Apache Airflow | "
    "Storage: PostgreSQL"
)
