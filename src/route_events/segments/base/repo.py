from sqlalchemy import Engine
import polars as pl
from .model import RouteSegmentEvents


class RouteSegmentEventsRepo(object):
    def __init__(self, sql_engine: Engine, table_name: str):
        self._table = table_name
        self._engine = sql_engine

    @property
    def table(self):
        return self._table

    def get_by_linkid(self, linkid: str) -> RouteSegmentEvents:
        """
        Get route segment events based on linkid query.
        """
        query = f"select * from {self.table} where linkid = '{linkid}'"
        df = pl.read_database(
            query, 
            connection=self._engine,
            infer_schema_length=None
        )

        return RouteSegmentEvents(
            df.to_arrow(),
            route=linkid
        )
