import asyncio

from src.core.ohlc_fetcher import execute_fetcher, sort_ticker_collection
from src.sql.ohlc_db_connection import ohlc_database

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    ticker_collection_task = sort_ticker_collection()
    ticker_collection = loop.run_until_complete(ticker_collection_task)
    execute_fetcher(ticker_collection)
    loop.run_until_complete(ohlc_database.dispose())
    loop.close()
