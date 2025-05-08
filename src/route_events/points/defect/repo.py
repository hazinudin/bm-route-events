from sqlalchemy import Engine, inspect, text
from .model import RouteDefects
import polars as pl
from pyarrow import Table


class RouteDefectsRepo(object):
    def __init__(self, sql_engine: Engine):
        self._table = 'rdd'
        self._engine = sql_engine
        self._inspect = inspect(sql_engine)

    @property
    def table(self):
        return self._table
    
    def get_by_linkid(
            self,
            linkid: str,
            year: int,
            raise_if_table_does_not_exists: bool = False
    ) -> RouteDefects:
        """
        Get Route Defects data from database and it into RouteDefects object.
        """
        if type(year) is not int:
            raise TypeError("'year' is not integer.")
        
        query = f"select * from {self.table}_{year} where linkid = '{linkid}'"

        if not self._inspect.has_table(f"{self.table}_{year}") and raise_if_table_does_not_exists:
            raise Exception(f"Table {self.table}_{year} does not exists")
        
        df = pl.read_database(
            query,
            connection=self._engine,
            infer_schema_length=None
        )
            
        obj = RouteDefects(
            df.to_arrow(),
            route=linkid,
            data_year=year
        )

        return obj
    
    def put(self, events: RouteDefects, year: int):
        """
        Put Route Defect data into Defect geodatabase table.
        """
        with self._engine.connect() as conn, conn.execution_options(isolation_level='READ COMMITTED'):
            try:
                self._delete(events, conn=conn, commit=False, year=year)
                self._insert(events, conn=conn, commit=False, year=year)
            except Exception as e:
                conn.rollback()
                raise e
            
        return
    
    def _delete(self, events: RouteDefects, year: int, conn, commit: bool = True):
        """
        Delete Defect data from Defect geodatabase table.
        """
        # Delete statement
        _where = f" where {events._linkid_col} = '{events.route_id}'"
        _del_stt = f"delete from {self.table}_{year}" + _where

        try:
            conn.execute(text(_del_stt))
        except Exception as e:
            conn.rollback()
            raise e
        
        if commit:
            conn.commit()
   
    def _insert(self, events: RouteDefects, year: int, conn, commit: bool = True):
        """
        Insert Defect data into Defect geodatabase table.
        """
        try:
            if not self._inspect.has_table(f"{self.table}_{year}"):
                events.pl_df.write_database(
                    f"{self.table}_{year}",
                    connection=conn
                )
            else:
                events.pl_df.write_database(
                    f"{self.table}_{year}",
                    connection=conn,
                    if_table_exists='append'
                )
        
        except Exception as e:
            conn.rollback()
            raise e
        
        if commit:
            conn.commit()
