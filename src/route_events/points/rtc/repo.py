from sqlalchemy import Engine, inspect, text
from sqlalchemy.exc import NoSuchTableError
from .model import RouteRTC
from ...utils import ora_pl_dtype
from ...utils.oid import has_objectid, generate_objectid
import polars as pl
from datetime import datetime


class RouteRTCRepo(object):
    def __init__(self, sql_engine: Engine):
        self._table = 'rtc'
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
    ) -> RouteRTC:
        """
        Get Route RTC data from database and it into RouteRTC object.
        """
        if type(year) is not int:
            raise TypeError("'year' is not integer.")
        
        query = f"select * from {self.table}_{year} where linkid = '{linkid}'"

        if not self._inspect.has_table(f"{self.table}_{year}") and raise_if_table_does_not_exists:
            raise NoSuchTableError(f"Table {self.table}_{year} does not exists")
        
        df = pl.read_database(
            query,
            connection=self._engine,
            infer_schema_length=None
        )
            
        obj = RouteRTC(
            df.to_arrow(),
            route=linkid,
            data_year=year
        )

        return obj
    
    def put(self, events: RouteRTC, year: int):
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
            
            conn.commit()
            
        return
    
    def _delete(self, events: RouteRTC, year: int, conn, commit: bool = True):
        """
        Delete Defect data from Defect geodatabase table.
        """
        # Delete statement
        _where = f" where {events._linkid_col} = '{events.route_id}'"
        _del_stt = f"delete from {self.table}_{year}" + _where

        if self._inspect.has_table(f"{self.table}_{year}"):
            try:
                conn.execute(text(_del_stt))
            except Exception as e:
                conn.rollback()
                raise e
            
            if commit:
                conn.commit()
   
    def _insert(self, events: RouteRTC, year: int, conn, commit: bool = True):
        """
        Insert Defect data into Defect geodatabase table.
        """
        try:
            if self._inspect.has_table(f"{self.table}_{year}"):
                if has_objectid(f"{self._table}_{year}", self._engine):
                    oids = generate_objectid(
                        schema='smd',
                        table=f"{self.table}_{year}",
                        sql_engine=self._engine,
                        oid_count=events.pl_df.select(pl.len()).rows()[0][0]
                    )

                    args = [pl.Series('OBJECTID', oids)]

                else:
                    args = []

                events.pl_df.with_columns(
                    pl.lit(datetime.now()).dt.datetime().alias('UPDATE_DATE'),
                    pl.lit(0).alias('COPIED'),
                    *args
                ).write_database(
                    f"{self.table}_{year}",
                    connection=conn,
                    if_table_exists='append',
                    engine_options={
                        'dtype': ora_pl_dtype(
                            events.pl_df,
                            date_cols_keywords='DATE'
                        )
                    }
                )
                
            else:
                events.pl_df.with_columns(
                    pl.lit(datetime.now()).dt.datetime().alias('UPDATE_DATE'),
                    pl.lit(0).alias('COPIED')
                ).write_database(
                    f"{self.table}_{year}",
                    connection=conn,
                    if_table_exists='append',
                    engine_options={
                        'dtype': ora_pl_dtype(
                            events.pl_df,
                            date_cols_keyword='DATE'
                        )
                    }
                )
        
        except Exception as e:
            conn.rollback()
            raise e
        
        if commit:
            conn.commit()

