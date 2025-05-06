from sqlalchemy import Engine
import polars as pl
from .model import RoutePOK
from typing import List


class RoutePOKRepo(object):
    def __init__(self, sql_engine: Engine):
        """
        Object to handle POK data in the database.
        
        :param sql_engine: SQLAlchemy engine
        """
        self._engine = sql_engine
        self._table = 'pok.pok_jalan_raw'
        self._comp_name_col = 'COMP_NAME'
        self._budget_year_col = 'BUDGET_YEAR'
        self._routeid_col = 'LINKID'
        self._from_sta_col = 'START_IND'
        self._to_sta_col = 'END_IND'

        self._column_selection = [
            self._routeid_col,
            self._comp_name_col,
            self._budget_year_col,
            self._from_sta_col,
            self._to_sta_col
        ]

    @property
    def table(self):
        """
        Return the table name.
        
        :return: Table name
        """
        return self._table
    
    def get_by_comp_name(
            self,
            linkid: str, 
            budget_year: int, 
            comp_name_keywords: List[str]
        ) -> RoutePOK:
        """
        Get POK data from database by budget year and component name, and load it into RoutePOK object.
        
        :param linkid: Route id 
        :param budget_year: Budget year
        :param comp_name: Component name
        :return: RoutePOK
        """
        comp_name_keywords = [
            # Convert all keywords into uppercase
            f"UPPER({self._comp_name_col}) LIKE '%{_.upper()}%'" for _ in comp_name_keywords
        ]  

        query = f"""
        SELECT {', '.join(self._column_selection)} 
        FROM {self.table} 
        WHERE 
        {self._routeid_col} = '{linkid}' AND
        {self._budget_year_col} = {budget_year} AND 
        ({' OR '.join(comp_name_keywords)})
        """
        
        df = pl.read_database(
            query, 
            connection=self._engine, 
            infer_schema_length=None
        )

        obj = RoutePOK(
            df.to_arrow(),
            route=linkid,
            data_year=budget_year
        )
        
        return obj