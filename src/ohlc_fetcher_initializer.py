import datetime as dt
import json
import time

import pandas as pd
import requests
from sqlalchemy import create_engine, desc, select

engine = create_engine(
    "postgresql+psycopg2://Jayc:admin@127.0.0.1:5432/bigaluto_stockdb"
)

from sqlalchemy.orm import Session

from .sql.schema import Data, Ticker, Timeframe

with open("identifiers.json", "r") as f:
    identifiers = json.load(f)

headers = {
    "Accept": "*/*",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
}

all_errors = []

timeframe_win = "D"
for index, identifier in enumerate(identifiers):
    try:
        # query = f'https://api.nasdaq.com/api/quote/{identifier}/historical'
        unixts_1 = 946684800
        unixts_2 = 1698263931

        query = f"https://query2.finance.yahoo.com/v8/finance/chart/NVR?formatted=true&crumb=0VkTr5j7Fso&lang=en-GB&region=GB&includeAdjustedClose=true&interval=1d&period1={unixts_1}&period2={unixts_2}&events=capitalGain%7Cdiv%7Csplit&useYfid=true&corsDomain=uk.finance.yahoo.com"
        r = requests.get(query, headers=headers)
        content = r.json()

        timestamp_list = content["chart"]["result"][0]["timestamp"]
        events_content = content["chart"]["result"][0]["indicators"]
        ohlc_content = events_content["quote"][0]
        adj_close = events_content["adjclose"][0]

        df = pd.DataFrame(
            {
                "date": timestamp_list,
                "open": ohlc_content["open"],
                "high": ohlc_content["high"],
                "low": ohlc_content["low"],
                "close": ohlc_content["close"],
                "adj_close": adj_close["adjclose"],
                "volume": ohlc_content["volume"],
            }
        )
        df["date"] = pd.to_datetime(df["date"], unit="s").dt.date

        # df = df[["date", "open", "high", "low", "close", "adj_close", "volume"]]

        with Session(engine) as session:
            try:
                # ticker = Ticker(name=identifier)
                # timeframe = Timeframe(name=timeframe_win)

                ticker = session.scalar(select(Ticker).filter_by(name=identifier))
                timeframe = session.scalar(
                    select(Timeframe).filter_by(name=timeframe_win)
                )

                if not ticker:
                    ticker = Ticker(name=identifier)
                    session.add(ticker)

                if not timeframe:
                    timeframe = Timeframe(name=timeframe_win)
                    session.add(timeframe)

                session.flush()

                # data_to_insert = session.query(
                #         Data.date, Data.ticker_id, Data.time_frame_id
                #     ).filter_by(ticker=ticker_id).order_by(desc('date')).first()

                data_list = []
                for row in df.itertuples():
                    data = Data(
                        date=row.date,
                        open=row.open,
                        high=row.high,
                        low=row.low,
                        close=row.close,
                        adj_close=row.adj_close,
                        volume=row.volume,
                        ticker=ticker,
                        timeframe=timeframe,
                    )
                    data_list.append(data)
                session.add_all(data_list)

                # session.execute(db.update(Data), data_list)
                session.commit()
                print(f"Populated: {identifier} from {index}")
            except Exception as e:
                print(e)
                print(f"Error on: {identifier}")

        time.sleep(0.1)

    except Exception as e:
        print(e)
        all_errors.append(identifier)
        print(f"Data issues for identifier: {identifier}")

print(all_errors)
