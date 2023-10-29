import calendar
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

from ..sql.schema import Data, Ticker, Timeframe


def execute_fetcher(
    ticker_collection: dict[str, list[tuple[int, str]]],
):
    headers = {
        "Accept": "*/*",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
    }

    all_errors = []
    timeframe_win = "D"

    for date, ticker_list in ticker_collection.items():
        start_date = dt.datetime.strptime(date, "%Y-%m-%d").date()
        for ticker_id, _ in ticker_list:
            try:
                unixts_1 = int(calendar.timegm(start_date.timetuple()))
                unixts_2 = int(calendar.timegm(dt.datetime.utcnow().timetuple()))

                query = f"https://query2.finance.yahoo.com/v8/finance/chart/NVR?formatted=true&crumb=0VkTr5j7Fso&lang=en-GB&region=US&includeAdjustedClose=true&interval=1d&period1={unixts_1}&period2={unixts_2}&events=capitalGain%7Cdiv%7Csplit&useYfid=true&corsDomain=uk.finance.yahoo.com"
                r = requests.get(query, headers=headers)
                content = r.json()

                content_data = content["chart"]["result"][0]

                if "timestamp" not in content_data:
                    continue

                timestamp_list = content_data["timestamp"]
                events_content = content_data["indicators"]
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

                with Session(engine) as session:
                    try:
                        ticker = session.scalar(select(Ticker).filter_by(id=ticker_id))
                        timeframe = session.scalar(
                            select(Timeframe).filter_by(name=timeframe_win)
                        )

                        if not ticker:
                            ticker = Ticker(name=ticker)
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
                        session.commit()
                        print(f"Populated: {ticker_id}")
                    except Exception as e:
                        print(e)
                        print(f"Error on: {ticker_id}")

                time.sleep(0.1)

            except Exception as e:
                print(e)
                all_errors.append(ticker_id)
                print(f"Data issues for identifier: {ticker_id}")

    print(all_errors)


def sort_ticker_collection() -> dict[str, list[tuple[int, str]]]:
    t1 = dt.datetime.now()
    with open("identifiers.json", "r") as f:
        identifiers = json.load(f)

    with Session(engine) as session:
        ticker_objs = session.scalars(
            select(Ticker).filter(Ticker.name.in_(identifiers))
        ).all()
        ticker_collection = {}

        for ticker_obj in ticker_objs:
            latest_date = session.scalars(
                select(Data.date)
                .filter_by(ticker_id=ticker_obj.id)
                .order_by(desc(Data.date))
            ).first()

            latest_date = (
                str(latest_date + dt.timedelta(days=2))  # type: ignore (time margin to deal with dates issue)
                if latest_date
                else str(dt.datetime(2000, 1, 1))
            )

            if latest_date in ticker_collection:
                ticker_collection[latest_date].append((ticker_obj.id, ticker_obj.name))
            else:
                ticker_collection[latest_date] = [(ticker_obj.id, ticker_obj.name)]

            break

    print(ticker_collection)
    print(f"Time taken: {dt.datetime.now() - t1}")
    return ticker_collection
