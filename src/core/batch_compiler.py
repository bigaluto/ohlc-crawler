from typing import Any

from src.constants import TICKER_BATCH_SIZE


def create_ticker_batches(ticker_list: list[Any]):
    batches_list = [
        ticker_list[i : i + TICKER_BATCH_SIZE]
        for i in range(0, len(ticker_list), TICKER_BATCH_SIZE)
    ]
    return batches_list
