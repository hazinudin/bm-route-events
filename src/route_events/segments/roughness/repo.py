from sqlalchemy import Engine, inspect, text
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

    def put(self, events: RouteRoughness, year: int, semester: int):
        """
        Put RNI data into RNI geodatabase table.
        """
        with self._engine.connect() as conn, conn.execution_options(isolation_level='READ COMMITTED'):
            try:
                self._delete(events, conn=conn, commit=False, year=year, semester=semester)
                self._insert(events, conn=conn, commit=False, year=year, semester=semester)
            except Exception as e:
                conn.rollback()
                raise e

            conn.commit()

        return

    def _delete(self, events: RouteRoughness, year: int, semester: int, conn, commit: bool = True):
        """
        Delete RNI data into RNI geodatabase table.
        """
        # Delete statement
        _where = f" where {events._linkid_col} = '{events.route_id}'"
        _del_stt = f"delete from {self.table}_{semester}_{year}" + _where

        try:
            conn.execute(text(_del_stt))
        except Exception as e:
            conn.rollback()
            raise e

        if commit:
            conn.commit()
        
        return
    
    def _insert(self, events: RouteRoughness, year: int, semester: int, conn, commit: bool = True):
        """
        Insert RNI data into RNI geodatabase table.
        """
        try:
            events.pl_df.write_database(
                f"{self._table}_{semester}_{year}",
                connection=conn,
                if_table_exists='append'
            )
        
        except Exception as e:
            conn.rollback()
            raise e
        
        if commit:
            conn.commit()

