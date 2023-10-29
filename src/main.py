from .utils.ohlc_fetcher import execute_fetcher, sort_ticker_collection

if __name__ == "__main__":
    ticker_collection = sort_ticker_collection()
    execute_fetcher(ticker_collection)
