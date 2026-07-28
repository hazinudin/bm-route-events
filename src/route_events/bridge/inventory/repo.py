import polars as pl
import oracledb
import json
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
        self._db_schema = "MISC"

        self.sups_table_name = "NAT_BRIDGE_SPAN"
        self.subs_table_name = "NAT_BRIDGE_ABT"
        self.inv_table_name = "NAT_BRIDGE_PROFILE"

        self.sups_el_table_name = "NAT_BRIDGE_SPAN_L3L4"
        self.subs_el_table_name = "NAT_BRIDGE_ABT_L3L4"

        # sups_only (popup) tables
        self.sups_popup_profile_table_name = "NAT_BRIDGE_PROFILE_POPUP"
        self.sups_popup_span_table_name = "NAT_BRIDGE_SPAN_POPUP"

        self.bridge_id_col = "BRIDGE_ID"
        self.inv_year_col = "INV_YEAR"
        self.latitude_col = "LATITUDE"
        self.longitude_col = "LONGITUDE"

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
            self.inv_table_name,
        ]

    def get_by_bridge_id(self, bridge_id: str, inv_year: int) -> BridgeInventory | None:
        """
        Load BridgeInventory from database table.
        """
        _where = f"where {self.bridge_id_col} = '{bridge_id}' and {self.inv_year_col} = {inv_year}"
        bridge_id_query = "select * from {0} " + _where

        # Download data from database
        # Add status filter
        df_inv = pl.read_database(
            bridge_id_query.format(self.inv_table_name) + f" and {self.latitude_col} is not null and {self.longitude_col} is not null", 
            connection=self._engine
        )
        df_sups = pl.read_database(
            bridge_id_query.format(self.sups_table_name), connection=self._engine
        )
        df_subs = pl.read_database(
            bridge_id_query.format(self.subs_table_name), connection=self._engine
        )
        df_sups_el = pl.read_database(
            bridge_id_query.format(self.sups_el_table_name), connection=self._engine
        )
        df_subs_el = pl.read_database(
            bridge_id_query.format(self.subs_el_table_name), connection=self._engine
        )

        # Load into object
        inv = BridgeInventory(df_inv.to_arrow())

        if not inv.is_empty:
            # Disable the validation, should be temporary
            sups = Superstructure(df_sups.to_arrow(), validate=False)
            subs = Substructure(df_subs.to_arrow(), validate=False)
            sups_el = StructureElement(df_sups_el.to_arrow())
            subs_el = StructureElement(df_subs_el.to_arrow())

            # Populate the BridgeInventory object
            if not df_sups.is_empty():
                inv.add_superstructure(sups)
                inv.sups.add_l3_l4_elements(sups_el)

            if not df_subs.is_empty():
                inv.add_substructure(subs)
                inv.subs.add_l3_l4_elements(subs_el)

            return inv
        else:
            return None

    def get_available_years(self, bridge_id: str) -> list:
        """
        Get available year of bridge inventory data.
        """
        # Add update_date is not null and inventory_state is not null to skip manually inserted data.
        query = f"select {self.inv_year_col} from {self.inv_table_name} where {self.bridge_id_col} = '{bridge_id}' and update_date is not null and inventory_state is not null"
        query = query + f" and {self.latitude_col} is not null and {self.longitude_col} is not null"
        results = pl.read_database(query, connection=self._engine)

        return results[self.inv_year_col].to_list()

    def _insert(self, obj: BridgeInventory, conn, commit=True):
        """
        Insert BridgeInventory to database table.
        """
        if obj.inventory_state == "DETAIL":
            inv_df = obj.pl_df
            sups_df = obj.sups.pl_df
            subs_df = obj.subs.pl_df
            sups_el_df = obj.sups.elements.pl_df
            subs_el_df = obj.subs.elements.pl_df

            # Convert string INV_DATE from string to datetime
            inv_df = inv_df.with_columns(
                INV_DATE=pl.col("INV_DATE").dt.strftime("%d/%b/%Y, 12:00:00%p")
            )

            table_mapping = {
                self.inv_table_name: inv_df,
                self.sups_table_name: sups_df,
                self.subs_table_name: subs_df,
                self.sups_el_table_name: sups_el_df,
                self.subs_el_table_name: subs_el_df,
            }
        else:
            inv_df = obj.pl_df
            sups_df = obj.sups.pl_df

            # Convert string INV_DATE from string to datetime
            inv_df = inv_df.with_columns(
                INV_DATE=pl.col("INV_DATE").dt.strftime("%d/%b/%Y, 12:00:00%p")
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
                        oid_count=df.select(pl.len()).rows()[0][0],
                    )

                    args = [pl.Series("OBJECTID", oids)]

            # Add update date and ESRI ObjectID (if exists)
            df_ = df.with_columns(
                pl.lit(datetime.now()).dt.datetime().alias("UPDATE_DATE"), *args
            )

            try:
                if self._table_exists(table):
                    df_.write_database(
                        table,
                        connection=conn,
                        if_table_exists="append",  # Append to existing
                    )
                else:
                    df_.write_database(
                        table,
                        connection=conn,
                        if_table_exists="replace",  # Create new table
                        engine_options={
                            "dtype": ora_pl_dtype(df, date_cols_keywords="DATE")
                        },
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
        with (
            self._engine.connect() as conn,
            conn.execution_options(isolation_level="READ COMMITTED"),
        ):
            try:
                self._delete(obj, conn=conn, commit=False)
                self._insert(obj, conn=conn, commit=False)
            except Exception as e:
                conn.rollback()
                raise e

            conn.commit()

        return

    def put_sups(self, obj: BridgeInventory, val_note: str = None):
        """
        Replace superstructure (sups_only) data for an existing bridge inventory.

        Writes only the two popup tables:
        - NAT_BRIDGE_PROFILE_POPUP: one row per bridge (superstructure summary).
        - NAT_BRIDGE_SPAN_POPUP: one row per span.

        `val_note` is stored in NAT_BRIDGE_PROFILE_POPUP.VAL_NOTE (validation result).
        """
        with (
            self._engine.connect() as conn,
            conn.execution_options(isolation_level="READ COMMITTED"),
        ):
            try:
                # Delete existing popup rows
                # The profile popup table has no INV_YEAR, so key on BRIDGE_ID only.
                _where_profile = f"where {self.bridge_id_col} = '{obj.id}'"
                _where_span = f"where {self.bridge_id_col} = '{obj.id}' and {self.inv_year_col} = {obj.inv_year}"

                if self._table_exists(self.sups_popup_profile_table_name):
                    conn.execute(
                        text(
                            f"DELETE FROM {self.sups_popup_profile_table_name} {_where_profile}"
                        )
                    )
                if self._table_exists(self.sups_popup_span_table_name):
                    conn.execute(
                        text(
                            f"DELETE FROM {self.sups_popup_span_table_name} {_where_span}"
                        )
                    )

                # Re-insert superstructure summary and span detail
                self._insert_sups_popup_profile(
                    obj, conn=conn, commit=False, val_note=val_note
                )
                self._insert_sups_popup_span(obj, conn=conn, commit=False)
            except Exception as e:
                conn.rollback()
                raise e

            conn.commit()

        return

    def _resolve_inv_date(self, obj) -> datetime:
        """
        Resolve the inventory/survey date. Use the profile INV_DATE if present,
        otherwise fall back to January 1st of the inventory year.
        """
        profile_df = obj.pl_df
        if "INV_DATE" in profile_df.columns:
            val = profile_df["INV_DATE"][0]
            if val is not None:
                return val

        return datetime(obj.inv_year, 1, 1)

    def _insert_sups_popup_profile(
        self, obj: BridgeInventory, conn, commit=True, val_note=None
    ):
        """Insert the one-row-per-bridge superstructure summary into NAT_BRIDGE_PROFILE_POPUP."""
        inv_date = self._resolve_inv_date(obj)

        # VAL_NOTE may be passed as a list of validation messages; serialize so it
        # can be written to a single column. Note: NVARCHAR2(2) is too small to hold
        # a message list -- widen the column (e.g. CLOB / large NVARCHAR2) to persist it.
        if isinstance(val_note, list):
            val_note = json.dumps(val_note, ensure_ascii=False)

        df = pl.DataFrame(
            {
                "BRIDGE_ID": [obj.id],
                "BRIDGE_LENGTH": [obj.length],
                "SUPERSTR_TYPE": [obj.span_type],
                "SURV_DATE": [inv_date],
                "UPDATE_DATE": [datetime.now()],
                "VAL_NOTE": [val_note],
                "SOURCE": ["VV"],
            }
        )

        self._write_popup_table(
            self.sups_popup_profile_table_name, df, conn, commit
        )
        return

    def _insert_sups_popup_span(
            self, 
            obj: BridgeInventory, 
            conn, 
            commit=True,
            source: Literal['VV', 'SURVEY'] = 'VV'
        ):
        """Insert the per-span detail into NAT_BRIDGE_SPAN_POPUP."""
        if obj.sups is None:
            return

        span_df = obj.sups.pl_df.select(
            [
                "BRIDGE_ID",
                "INV_YEAR",
                "SUPERSTRUCTURE",
                "SPAN_LENGTH",
                "SPAN_NUMBER",
                "SPAN_SEQ",
                "SPAN_TYPE",
            ]
        )

        inv_date = self._resolve_inv_date(obj)

        span_df = span_df.with_columns(
            INV_DATE=pl.lit(inv_date),
            UPDATE_DATE=pl.lit(datetime.now()),
        )

        self._write_popup_table(
            self.sups_popup_span_table_name, span_df, conn, commit
        )
        return

    def _write_popup_table(self, table: str, df: pl.DataFrame, conn, commit=True):
        """
        Append a DataFrame to a popup table, assigning an ESRI OBJECTID when the
        table has one. Creates the table (with Oracle dtypes) if it does not exist.
        """
        args = []
        if self._table_exists(table):
            if has_objectid(table, self._engine):
                oids = generate_objectid(
                    schema=self._db_schema,
                    table=table,
                    sql_engine=self._engine,
                    oid_count=df.select(pl.len()).rows()[0][0],
                )
                args = [pl.Series("OBJECTID", oids)]

        df_ = df.with_columns(*args) if args else df

        try:
            if self._table_exists(table):
                df_.write_database(table, connection=conn, if_table_exists="append")
            else:
                df_.write_database(
                    table,
                    connection=conn,
                    if_table_exists="replace",
                    engine_options={
                        "dtype": ora_pl_dtype(df_, date_cols_keywords="DATE")
                    },
                )
        except Exception as e:
            conn.rollback()
            raise e

        if commit:
            conn.commit()
        return

    def _table_exists(self, table) -> bool:
        """
        Check if table exist.
        """
        return self._inspect.has_table(table)

    def _ora_dtype(self, df: pl.DataFrame) -> dict:
        """
        Return Oracle dtype for table creation.
        """
        out_dict = dict()
        for col in df.schema.items():
            col_name = col[0]
            dtype = col[1]

            if "DATE" in col_name:
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
