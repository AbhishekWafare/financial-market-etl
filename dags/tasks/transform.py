
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from airflow.sdk import task # type: ignore


PRICE_MAPPING = {
    "bitcoin_price": "price_usd",
    "gold_price": "price_gold",
    "ethereum_price": "price_ethereum",
    "silver_price": "price_silver",
    "oil_price": "price_oil",
    "sp500_price": "price_sp500",
}


@task
def transform(data):
    """Transform extracted market prices into the warehouse format."""

    logging.info("Financial market transformation started")

    if not isinstance(data, dict):
        raise TypeError("Extracted data must be a dictionary.")

    transformed_data = {}

    for source_column, target_column in PRICE_MAPPING.items():

        if source_column not in data:
            raise KeyError(
                f"Missing extracted field: {source_column}"
            )

        try:
            transformed_data[target_column] = float(
                data[source_column]
            )
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"Invalid value for {source_column}"
            ) from exc

    transformed_data["time"] = datetime.now(
        ZoneInfo("Asia/Kolkata")
    ).strftime("%Y-%m-%d %H:%M:%S")

    logging.info("Financial market transformation completed")

    return transformed_data
