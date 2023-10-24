from typing import List

from sqlalchemy import DATE, BigInteger, Column, Float, ForeignKey, Integer, String
from sqlalchemy.orm import Mapped, declarative_base, mapped_column, relationship

Base = declarative_base()

class Ticker(Base):
    __tablename__  = "ticker"
    id = mapped_column(BigInteger(), primary_key=True, autoincrement=True)
    name = mapped_column(String, unique=True, nullable=False)
    datas: Mapped[List["Data"]] = relationship(back_populates="ticker")

class Timeframe(Base):
    __tablename__  = "timeframe"
    id = mapped_column(BigInteger(), primary_key=True, autoincrement=True)
    name = mapped_column(String, unique=True, nullable=False)
    datas: Mapped[List["Data"]] = relationship(back_populates="timeframe")

class Data(Base):
    __tablename__ = "data"

    date: Mapped[DATE] = mapped_column(DATE, primary_key=True)
    ticker_id = mapped_column(Integer(), ForeignKey("ticker.id"), primary_key=True)
    timeframe_id = mapped_column(Integer(), ForeignKey("timeframe.id"), primary_key=True)
    open: Mapped[Float] = mapped_column(Float)
    high: Mapped[Float] = mapped_column(Float)
    low: Mapped[Float] = mapped_column(Float)
    close: Mapped[Float] = mapped_column(Float)
    volume: Mapped[Integer] = mapped_column(Integer)
    ticker: Mapped["Ticker"] = relationship(back_populates="datas")
    timeframe: Mapped["Timeframe"] = relationship(back_populates="datas")
