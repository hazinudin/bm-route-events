from sqlalchemy import Engine, inspect, text
from .model import RoutePCI
import polars as pl
from typing import Literal


class RoutePCIRepo(object):
    def __init__(self, sql_engine: Engine):
        self._table = 'pci'
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
    ) -> RoutePCI:
        """
        Get PCI data from database and load it into RoutePCI object.
        """
        if type(year) is not int:
            raise TypeError("'year' is not integer.")
        
        table = "{0}_{1}_{2}"
        query = "select * from {0} where linkid = '{1}'"

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
                table.format(self.table, semester, year),
                linkid
            ),
            connection=self._engine,
            infer_schema_length=None
        )

        pci = RoutePCI(
            artable=df.to_arrow(),
            route=linkid,
            data_year=year
        )

        pci.sta_unit = 'km'  # Default is kilometers

        if year <= 2024:  # From 2024 and before that, default segment is 100m
            pci.segment_length = 0.1
        else:  # Starting from 2025, default segment is 50m
            pci.segment_length = 0.05

        return pci

    def put(self, events: RoutePCI, year: int, semester: int):
        """
        Put PCI data into PCI geodatabase table.
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
    
    def _delete(self, events: RoutePCI, year: int, semester: int, conn, commit: bool = True):
        """
        Delete PCI data into PCI geodatabase table.
        """
        # Delete statement
        _where = f"where {events._linkid_col} = '{events._route_id}'"
        _del_stt = f"delete from {self.table}_{semester}_{year} " + _where

        if self._inspect.has_table(f"{self.table}_{semester}_{year}"):
            try:
                conn.execute(text(_del_stt))
            except Exception as e:
                conn.rollback()
                raise e
            
            if commit:
                conn.commit()
        
        return
    
    def _insert(self, events: RoutePCI, year: int, semester: int, conn, commit: bool = True):
        """
        Insert PCI data into PCI geodatabase table.
        """
        try:
            if self._inspect.has_table(f"{self.table}_{semester}_{year}"):
                if_table_exists='append'
            else:
                if_table_exists='replace'

            events.pl_df.write_database(
                f"{self.table}_{semester}_{year}",
                connection=conn,
                if_table_exists=if_table_exists
            )
            
        except Exception as e:
            conn.rollback()
            raise e
        
        if commit:
            conn.commit()
