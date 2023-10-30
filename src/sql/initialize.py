import os

import sqlalchemy as db

from src.sql.schema import Base

ohlc_postgres_url = os.environ.get("ohlc_postgres_url", "")
engine = db.create_engine(ohlc_postgres_url)
Base.metadata.create_all(engine)
