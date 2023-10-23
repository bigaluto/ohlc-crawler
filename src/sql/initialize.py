import sqlalchemy as db
from .schema import Base

engine = db.create_engine('postgresql+psycopg2://Jayc:admin@127.0.0.1:5432/bigaluto_stockdb')
Base.metadata.create_all(engine)
