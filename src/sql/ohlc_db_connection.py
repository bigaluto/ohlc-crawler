import os
from typing import Any, Union

from sqlalchemy import create_engine, select
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import Session


class OHLCDatabase:
    def __init__(self) -> None:
        self.engine = create_engine(os.environ.get("ohlc_postgres_url", ""))
        self.async_engine = create_async_engine(
            os.environ.get("async_ohlc_postgres_url", "")
        )

    def insert_data(self, session: Session, data: Any):
        session.add(data)

    def insert_bulk_data(self, session: Session, data_list: list[Any]):
        session.add_all(data_list)

    def select(
        self,
        session: Session,
        table_name: Any,
        filter_param: dict[str, Any],
        order_param: list[Any] = [],
    ) -> Any:
        return session.scalars(
            select(table_name).filter_by(**filter_param).order_by(*order_param)
        )

    async def async_select(
        self,
        session: AsyncSession,
        table_name: Any,
        filter_param: dict[str, Any],
        order_param: list[Any] = [],
    ) -> Any:
        return await session.scalars(
            select(table_name).filter_by(**filter_param).order_by(*order_param)
        )

    async def dispose(self):
        await self.async_engine.dispose()


ohlc_database = OHLCDatabase()
