import polars as pl
import oracledb
from sqlalchemy import Engine, inspect, text
from sqlalchemy.dialects.oracle import NUMBER, NVARCHAR2, TIMESTAMP
from datetime import datetime
from .profile.model import BridgeInventory
from .structure import Superstructure, Substructure
from .structure.element import StructureElement
from ...utils.oid import has_objectid, generate_objectid
from ...utils import ora_pl_dtype


class BridgeInventoryRepo(object):
    def __init__(self, sql_engine: Engine):
        # SQLAlchemy engine
        # Use the oracledb engine instead of 'oracle' which means cxoracle
        # self._engine = create_engine(self._ora_cstr.replace('oracle', 'oracle+oracledb'))
        self._engine = sql_engine
        self._inspect = inspect(sql_engine)
        self._db_schema = 'MISC'

        self.sups_table_name = 'NAT_BRIDGE_SPAN'
        self.subs_table_name = 'NAT_BRIDGE_ABT'
        self.inv_table_name = 'NAT_BRIDGE_PROFILE'

        self.sups_el_table_name = 'NAT_BRIDGE_SPAN_L3L4'
        self.subs_el_table_name = 'NAT_BRIDGE_ABT_L3L4'

        self.bridge_id_col = 'BRIDGE_ID'
        self.inv_year_col = 'INV_YEAR'

    @property
    def _ora_cstr(self):
        """
        Oracle connection string.
        """
        return f"oracle://{self.user}:{self.pwd}@{self.host}:{self.port}/{self.service_name}"
    
    @property
    def _tables(self):
        """
        Return list of all inventory tables.
        """
        return [
            self.sups_table_name,
            self.sups_el_table_name,
            self.subs_table_name,
            self.subs_el_table_name,
            self.inv_table_name
        ]
    
    def get_by_bridge_id(self, bridge_id: str, inv_year: int)->BridgeInventory:
        """
        Load BridgeInventory from database table.
        """
        _where = f"where {self.bridge_id_col} = '{bridge_id}' and {self.inv_year_col} = {inv_year}"
        bridge_id_query = "select * from {0} " + _where

        # Download data from database
        df_inv = pl.read_database(bridge_id_query.format(self.inv_table_name), connection=self._engine)
        df_sups = pl.read_database(bridge_id_query.format(self.sups_table_name), connection=self._engine)
        df_subs = pl.read_database(bridge_id_query.format(self.subs_table_name), connection=self._engine)
        df_sups_el = pl.read_database(bridge_id_query.format(self.sups_el_table_name), connection=self._engine)
        df_subs_el = pl.read_database(bridge_id_query.format(self.subs_el_table_name), connection=self._engine)
        
        # Load into object
        inv = BridgeInventory(df_inv.to_arrow())

        if not inv.is_empty:
            sups = Superstructure(df_sups.to_arrow(), validate=True)
            subs = Substructure(df_subs.to_arrow(), validate=True)
            sups_el = StructureElement(df_sups_el.to_arrow())
            subs_el = StructureElement(df_subs_el.to_arrow())
            
            # Populate the BridgeInventory object
            inv.add_superstructure(sups)
            inv.add_substructure(subs)

            # Add elements to Superstructure and Substructure
            inv.sups.add_l3l4_elements(sups_el)
            inv.subs.add_l3_l4_elements(subs_el)

            return inv
        else:
            return None
    
    def get_available_years(self, bridge_id: str)->list:
        """
        Get available year of bridge inventory data.
        """
        query = f"select {self.inv_year_col} from {self.inv_table_name} where {self.bridge_id_col} = '{bridge_id}'"
        results = pl.read_database(query, connection=self._engine)

        return results[self.inv_year_col].to_list()
    
    def _insert(self, obj: BridgeInventory, conn, commit=True):
        """
        Insert BridgeInventory to database table.
        """
        if obj.inventory_state == 'DETAIL':
            inv_df = obj.pl_df
            sups_df = obj.sups.pl_df
            subs_df = obj.subs.pl_df
            sups_el_df = obj.sups.elements.pl_df
            subs_el_df = obj.subs.elements.pl_df

            # Convert string INV_DATE from string to datetime
            inv_df = inv_df.with_columns(
                INV_DATE=pl.col('INV_DATE').dt.strftime("%d/%b/%Y, 12:00:00%p")
            )

            table_mapping = {
                self.inv_table_name: inv_df,
                self.sups_table_name: sups_df,
                self.subs_table_name: subs_df,
                self.sups_el_table_name: sups_el_df,
                self.subs_el_table_name: subs_el_df 
            }
        else:
            inv_df = obj.pl_df
            sups_df = obj.sups.pl_df

            # Convert string INV_DATE from string to datetime
            inv_df = inv_df.with_columns(
                INV_DATE=pl.col('INV_DATE').dt.strftime("%d/%b/%Y, 12:00:00%p")
            )

            table_mapping = {
                self.inv_table_name: inv_df,
                self.sups_table_name: sups_df,
            }

        for table, df in zip(table_mapping, table_mapping.values()):
            args = []
            
            if self._table_exists(table):
                if has_objectid(table, self._engine):
                    oids = generate_objectid(
                        schema=self._db_schema,
                        table=table,
                        sql_engine=self._engine,
                        oid_count=df.select(pl.len()).rows()[0][0]
                    )

                    args = [pl.Series('OBJECTID', oids)]

            # Add update date and ESRI ObjectID (if exists)
            df_ = df.with_columns(
                pl.lit(datetime.now()).dt.datetime().alias('UPDATE_DATE'),
                *args
            )

            try:
                if self._table_exists(table):
                    df_.write_database(
                        table,
                        connection=conn,
                        if_table_exists='append'  # Append to existing
                    )
                else:
                    df_.write_database(
                        table,
                        connection=conn,
                        if_table_exists='replace',  # Create new table
                        engine_options = {
                            'dtype': ora_pl_dtype(
                                df,
                                date_cols_keywords='DATE'
                            )
                        }
                    )

            except Exception as e:
                conn.rollback()  # Rollback if there is an error
                raise e
        
        if commit:
            conn.commit()

        return
    
    def _delete(self, obj: BridgeInventory, conn, commit=True):
        """
        Delete BridgeInventory in database table.
        """
        # Delete statement
        # Delete based on bridge_id and inventory year.
        _where = f"where {self.bridge_id_col} = '{obj.id}' and {self.inv_year_col} = {obj.inv_year}"
        del_stt = "DELETE FROM {0} " + _where

        # Delete inventory items in all inventory tables.
        for table in self._tables:
            # Check if table exists
            # If the table does not exist, then skip
            if not self._table_exists(table):
                continue
            
            try:
                conn.execute(text(del_stt.format(table)))
            except Exception as e:
                conn.rollback()  # Rollback if there is an error
                raise e
        
        if commit:
            conn.commit()

        return

    def put(self, obj: BridgeInventory):
        """
        Replace/Insert BridgeInventory data in database table.
        """
        with self._engine.connect() as conn, conn.execution_options(isolation_level="READ COMMITTED"):
            try:
                self._delete(obj, conn=conn, commit=False)
                self._insert(obj, conn=conn, commit=False)
            except Exception as e:
                conn.rollback()  # Rollback if there is an error
                raise e

            conn.commit()

        return        
        
    def _table_exists(self, table)->bool:
        """
        Check if table exist.
        """
        return self._inspect.has_table(table)
    
    def _ora_dtype(self, df: pl.DataFrame)->dict:
        """
        Return Oracle dtype for table creation.
        """
        out_dict = dict()
        for col in df.schema.items():
            col_name = col[0]
            dtype = col[1]

            if 'DATE' in col_name:
                dtype = pl.Datetime

            if dtype == pl.String:
                out_dict[col_name] = NVARCHAR2(255)
            elif dtype == pl.Float64:
                out_dict[col_name] = NUMBER(38, 8)
            elif dtype == pl.Int64:
                out_dict[col_name] = NUMBER(38)
            elif dtype == pl.Datetime:
                out_dict[col_name] = TIMESTAMP(timezone=True)

        return out_dict