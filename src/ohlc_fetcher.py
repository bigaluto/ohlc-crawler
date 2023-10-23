import pandas as pd
import datetime as dt
import json
import requests
import time
from sqlalchemy import create_engine, select, desc
engine = create_engine('postgresql+psycopg2://Jayc:admin@127.0.0.1:5432/bigaluto_stockdb')

from sqlalchemy.orm import Session
from .sql.schema import Data, Ticker, Timeframe

with open('identifiers.json', 'r') as f:
    identifiers = json.load(f)

headers = {
    'Accept': '*/*',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'
}

all_errors = []

timeframe_win = 'D'
for index, identifier in enumerate(identifiers):
    try:
        query = f'https://api.nasdaq.com/api/quote/{identifier}/historical'
        r = requests.get(query, headers=headers, params={
            'assetclass': 'stocks',
            'fromdate': '2013-08-14',
            'limit': 999999,
            'todate': '2023-08-14'
        })

        df = pd.json_normalize(r.json()['data']['tradesTable']['rows'])

        df['date'] = pd.to_datetime(df['date'])

        df = df[['date', 'open', 'high', 'low', 'close', 'volume']]
        df[['open', 'high', 'low', 'close']] = df[['open', 'high', 'low', 'close']].applymap(lambda x: x.replace('$', ''))
        df[['open', 'high', 'low', 'close', 'volume']] = df[['open', 'high', 'low', 'close', 'volume']].applymap(lambda x: x.replace(',', ''))

        df['volume'] = df['volume'].astype(int)
        df[['open', 'high', 'low', 'close']] = df[['open', 'high', 'low', 'close']].astype(float)

        with Session(engine) as session:
            try:
                # ticker = Ticker(name=identifier)
                # timeframe = Timeframe(name=timeframe_win)

                ticker = session.scalar(select(Ticker).filter_by(name=identifier))
                timeframe = session.scalar(select(Timeframe).filter_by(name=timeframe_win))

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
                    data = Data(date=row.date, open=row.open, high=row.high, low=row.low, close=row.close,
                            volume=row.volume, ticker=ticker, timeframe=timeframe)
                    data_list.append(data)
                session.add_all(data_list)

                # session.execute(db.update(Data), data_list)
                session.commit()
                print(f'Populated: {identifier} from {index}')
            except Exception as e:
                print(e)
                print(f'Error on: {identifier}')
            
        time.sleep(0.1)
    
    except Exception as e:
        all_errors.append(identifier)
        print(f'Data issues for identifier: {identifier}')

print(all_errors)

