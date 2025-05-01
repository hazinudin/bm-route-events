from sqlalchemy import Engine, inspect, text
from .model import RouteRNI
import polars as pl
from typing import List, Union


class RouteRNIRepo(object):
    def __init__(self, sql_engine: Engine):
        self._table = 'rni'
        self._engine = sql_engine
        self._inspect = inspect(sql_engine)
    
    @property
    def table(self):
        return self._table
    
    def get_by_linkid(
            self, 
            linkid: str, 
            year: int,
            columns: Union[str | List[str]] = '*',
            raise_if_table_does_not_exists: bool = False
    ) -> RouteRNI:
        """
        Get RNI data from database and load it into RouteRNI object.
        """
        if type(year) is not int:
            raise TypeError("'year' is not integer.")
        
        if type(columns) == str:
            pass
        elif type(columns) == list:
            columns = ", ".join(columns)
        else:
            raise TypeError("'columns' only accepts string or list type.")
        
        table = "{0}_{1}_{2}"
        query = "select {0} from {1} where linkid = '{2}'"

        if self._inspect.has_table(table.format(self.table, 2, year)):
            semester = 2
        elif self._inspect.has_table(table.format(self.table, 1, year)):
            semester = 1
        elif raise_if_table_does_not_exists:
            raise Exception(f"Table {table.format(self.table, semester, year)} does not exists.")
        else:
            None
        
        df = pl.read_database(
                query.format(
                    columns,
                    table.format(
                        self.table, 
                        semester, 
                        year
                    ),
                    linkid
                ),
                connection=self._engine,
                infer_schema_length=None
            )

        return RouteRNI(
            artable=df.to_arrow(),
            route=linkid,
            data_year=year
        )

    def put(self, events: RouteRNI, year: int, semester: int):
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

    def _delete(self, events: RouteRNI, year: int, semester: int, conn, commit: bool = True):
        """
        Delete RNI data into RNI geodatabase table.
        """
        # Delete statement
        _where = f"where {events._linkid_col} = '{events.route_id}'"
        _del_stt = f"delete from {self.table}_{semester}_{year}" + _where

        try:
            conn.execute(text(_del_stt))
        except Exception as e:
            conn.rollback()
            raise e

        if commit:
            conn.commit()
        
        return
    
    def _insert(self, events: RouteRNI, year: int, semester: int, conn, commit: bool = True):
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
