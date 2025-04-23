from sqlalchemy import Engine, inspect
from .model import RouteRoughness
import polars as pl
from typing import Literal


class RouteRoughnessRepo(object):
    def __init__(self, sql_engine: Engine):
        self._table = 'roughness'
        self._engine = sql_engine
        self._inspect = inspect(sql_engine)

    @property
    def table(self):
        return self._table

    def get_by_linkid(
            self,
            linkid: str,
            year: int,
            semester: Literal[1,2],
            raise_if_table_does_not_exists: bool = False
    ) -> RouteRoughness:
        """
        Get Roughness data from database and load it into RouteRoughness object.
        """
        table = "{0}_{1}_{2}"
        query = "select * from {0} where linkid = '{1}'"

        if self._inspect.has_table(table.format(self.table, semester, year)):
            pass
        elif raise_if_table_does_not_exists:
            raise Exception(f"Table {table.format(self.table, semester, year)} does not exists.")
        else:
            None
        
        df = pl.read_database(
                query.format(
                    table.format(self.table, semester, year),
                    linkid
                ),
                connection=self._engine,
                infer_schema_length=None
            )

        obj = RouteRoughness(
            df.to_arrow(),
            route=linkid,
            data_year=year,
            data_semester=semester
        )

        obj.sta_unit = 'km'

        return obj
