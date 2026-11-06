
import logging
import math

import requests
from airflow.sdk import task # type: ignore


def _get_json(url: str) -> dict:
    """Fetch JSON data from an API and validate the response."""

    try:
        response = requests.get(url, timeout=(5, 20))
        response.raise_for_status()
        data = response.json()

    except requests.RequestException:
        logging.exception("API request failed: %s", url)
        raise

    except ValueError:
        logging.exception("API returned invalid JSON: %s", url)
        raise

    if not isinstance(data, dict):
        raise ValueError(f"Expected a JSON object from {url}")

    return data


def _validate_price(value, asset: str) -> float:
    """Ensure the extracted market price is a valid positive number."""

    try:
        price = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid price received for {asset}") from exc

    if not math.isfinite(price) or price <= 0:
        raise ValueError(f"{asset} price must be a finite positive number")

    return price


def _get_yfinance_price(yf, symbol: str, asset: str) -> float:
    """Fetch the latest available closing price from Yahoo Finance."""

    history = yf.Ticker(symbol).history(period="1d")

    if history.empty or "Close" not in history.columns:
        raise ValueError(f"No closing price available for {asset}")

    closing_prices = history["Close"].dropna()

    if closing_prices.empty:
        raise ValueError(f"No valid closing price available for {asset}")

    return _validate_price(closing_prices.iloc[-1], asset)


@task
def extract():
    """Extract financial market prices from external APIs."""

    logging.info("Financial market extraction started")

    # Import inside the task to avoid loading yfinance during DAG parsing.
    import yfinance as yf

    try:
        bitcoin_data = _get_json(
            "https://api.coingecko.com/api/v3/simple/price"
            "?ids=bitcoin&vs_currencies=usd"
        )

        ethereum_data = _get_json(
            "https://api.coingecko.com/api/v3/simple/price"
            "?ids=ethereum&vs_currencies=usd"
        )

        gold_data = _get_json(
            "https://api.gold-api.com/price/XAU"
        )

        silver_data = _get_json(
            "https://api.gold-api.com/price/XAG"
        )

        bitcoin_price = _validate_price(
            bitcoin_data.get("bitcoin", {}).get("usd"),
            "Bitcoin"
        )

        ethereum_price = _validate_price(
            ethereum_data.get("ethereum", {}).get("usd"),
            "Ethereum"
        )

        gold_price = _validate_price(
            gold_data.get("price"),
            "Gold"
        )

        silver_price = _validate_price(
            silver_data.get("price"),
            "Silver"
        )

        oil_price = _get_yfinance_price(
            yf, "CL=F", "Crude Oil"
        )

        sp500_price = _get_yfinance_price(
            yf, "^GSPC", "S&P 500"
        )

        result = {
            "bitcoin_price": bitcoin_price,
            "gold_price": gold_price,
            "ethereum_price": ethereum_price,
            "silver_price": silver_price,
            "oil_price": oil_price,
            "sp500_price": sp500_price
        }

        logging.info("Financial market extraction completed successfully")

        return result

    except Exception:
        logging.exception("Financial market extraction failed")
        raise
