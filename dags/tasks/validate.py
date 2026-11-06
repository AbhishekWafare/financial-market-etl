
import logging
import math
from datetime import datetime

from airflow.sdk import task # type: ignore


REQUIRED_PRICE_FIELDS = [
    "price_usd",
    "price_gold",
    "price_ethereum",
    "price_silver",
    "price_oil",
    "price_sp500",
]


@task
def validate(data):
    """Validate transformed financial market data."""

    logging.info("Financial market data validation started")

    if not isinstance(data, dict):
        raise TypeError("Data must be a dictionary.")

    if "time" not in data or not data["time"]:
        raise ValueError("Market data timestamp is missing.")

    try:
        datetime.fromisoformat(str(data["time"]))
    except (TypeError, ValueError) as exc:
        raise ValueError("Invalid market data timestamp.") from exc

    validated_data = data.copy()

    for field in REQUIRED_PRICE_FIELDS:

        if field not in validated_data:
            raise ValueError(f"Missing required field: {field}")

        value = validated_data[field]

        if value is None:
            raise ValueError(f"{field} cannot be NULL.")

        try:
            price = float(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"{field} must contain a valid numeric value."
            ) from exc

        if not math.isfinite(price):
            raise ValueError(
                f"{field} must contain a finite numeric value."
            )

        if price <= 0:
            raise ValueError(
                f"{field} must be greater than zero."
            )

        validated_data[field] = price

    logging.info("Financial market data validation passed")

    return validated_data
