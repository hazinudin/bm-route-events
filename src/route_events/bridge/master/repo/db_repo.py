from ..model import BridgeMaster
from ..events import BridgeEvents
import oracledb
from polars import read_database, col, from_arrow, lit, format, DataFrame, String
from sqlalchemy import Engine, text
from datetime import datetime


class BridgeMasterRepoDB(object):
    def __init__(self, sql_engine: Engine):
        # SQLAlchemy engine
        self._engine = sql_engine

        # Default columns
        self._start_date_col = 'START_DATE'
        self._end_date_col = 'END_DATE'
        self._bridge_id_col = 'BRIDGE_ID'
        self._bridge_num_col = 'BRIDGE_NUM'

        # Table name
        self._table = 'MISC.NATIONAL_BRIDGE'
        self._event_table = 'MISC.NAT_BRIDGE_EVENT_STORE'

    @property
    def active_date_query(self):
        return f"({self._start_date_col} is NULL or {self._start_date_col} < CURRENT_TIMESTAMP) AND ({self._end_date_col} is NULL or {self._end_date_col} > CURRENT_TIMESTAMP)"
    
    def get_oid(self):
        """
        Generate new GeoDatabase Object ID.
        """
        with self._engine.connect() as cur:
            result = cur.execute(
                text(f"select sde.gdb_util.next_rowid('{self._table.split('.')[0]}', '{self._table.split('.')[1]}') from dual")
                )
            oid = result.fetchall()[0][0]
        
        return oid
    
    def get_by_bridge_number(self, bridge_num: str, return_count_only=False)->BridgeMaster:
        """
        Load BridgeMaster data from database with Bridge number query.
        """
        if return_count_only:
            query = f"SELECT COUNT(*) FROM {self._table} WHERE {self._bridge_num_col} = '{bridge_num}' AND {self.active_date_query}"

            with self._engine.connect() as cur:
                result = cur.execute(text(query))
                count = result.fetchall()[0][0]

                return count
        else:
            query = f"SELECT * FROM {self._table} WHERE {self._bridge_num_col} = '{bridge_num}' AND {self.active_date_query}"
            df = read_database(query, connection=self._engine)
            
            if df.is_empty():
                return None
            else:
                if df['LAST_INV_DATE'].is_null().any():
                    df = df.with_columns(LAST_INV_DATE=lit(datetime(1900, 1, 1)))
                # else:
                #     df = df.with_columns(LAST_INV_DATE=col('LAST_INV_DATE').dt.strftime("%d/%m/%Y"))
                
                return BridgeMaster(df.head(1).drop('SHAPE').to_arrow(), validate=False)

    def get_by_bridge_id(self, bridge_id: str, return_count_only=False)->BridgeMaster:
        """
        Load BridgeMaster data from database with Bridge ID query.
        """
        if return_count_only:
            query = f"SELECT COUNT(*) FROM {self._table} WHERE {self._bridge_id_col} = '{bridge_id}' AND {self.active_date_query}"

            with self._engine.connect() as cur:
                result = cur.execute(text(query))
                count = result.fetchall()[0][0]

                return count
        else:
            query = f"SELECT * FROM {self._table} WHERE {self._bridge_id_col} = '{bridge_id}' AND {self.active_date_query}"
            df = read_database(query, connection=self._engine)
            
            if df.is_empty():
                return None
            else:
                if df['LAST_INV_DATE'].is_null().any():
                    df = df.with_columns(LAST_INV_DATE=lit(datetime(1900, 1, 1)))
                # else:
                #     df = df.with_columns(LAST_INV_DATE=col('LAST_INV_DATE').dt.strftime("%d/%m/%Y"))
                
                return BridgeMaster(df.head(1).drop('SHAPE').to_arrow(), validate=False)
            
    def get_nearest(self, bridge: BridgeMaster, radius: int, return_count_only=False):
        """
        Load Bridge Master data from database with spatial radius (in meter) query.
        """
        query = f"""
        with bridge as (select * from {self._table} where {self.active_date_query})
        select *
        from bridge
        where SQRT(
        POWER({bridge._point_lambert.Y}-sde.ST_X(shape), 2) + 
        POWER({bridge._point_lambert.X}-sde.ST_Y(shape), 2)
        ) <= {radius}
        and
        {self._bridge_id_col} != {bridge.id}
        """

        df = read_database(query, connection=self._engine)
        
        if return_count_only:
            return df.shape[0]
        else:
            return df

    def insert(self, bridge: BridgeMaster):
        """
        Insert BridgeMaster to database
        """
        df = from_arrow(bridge.artable)
        oid = self.get_oid()
        
        df = df.with_columns(
            OBJECTID=lit(oid),
            LAST_INV_DATE=col("LAST_INV_DATE").dt.strftime("%d/%b/%Y"),
            START_DATE=lit(datetime.now().strftime("%d/%b/%Y, %I:%M:0%p"))
            )

        df.write_database(
            self._table, 
            connection=self._engine, 
            if_table_exists='append'
            )
        
        # Update the SHAPE rows using Object ID
        # 300005 is SRID for Lambert
        update_statement = f"UPDATE {self._table} SET SHAPE = sde.ST_GEOMETRY({bridge._point_lambert.Y}, {bridge._point_lambert.X}, 0, 0, 300005) WHERE OBJECTID = {oid}"

        with self._engine.connect() as cur:
            cur.execute(text(update_statement))
            cur.commit()

        return
    
    def append_events(self, bridge: BridgeMaster):
        """
        Append event to the event store table.
        """
        insert_statement = text(
            f"""insert into {self._event_table} (bridge_id, event_name, event, occurred_at) 
            values (:id, :event_name, :event, current_timestamp)"""
        )

        rows = [
            {
                "id": event.id, 
                "event_name": event.name, 
                "event": event.serialize()
            } for event in bridge.get_all_events()
        ]

        with self._engine.connect() as conn:
            for row in rows:
                conn.execute(insert_statement, row)
            conn.commit()

        return

    def update(self, bridge: BridgeMaster):
        """
        Update BridgeMaster based on Bridge ID.
        """
        df = from_arrow(bridge.artable)
        df = df.with_columns(
            LAST_INV_DATE=col("LAST_INV_DATE").dt.strftime("%d/%b/%Y")
            )
        
        # Create set and update statement.
        set_value = self._create_update_set(df)
        set_value = set_value + f", SHAPE = sde.ST_GEOMETRY({bridge._point_lambert.Y}, {bridge._point_lambert.X}, 0, 0, 300005)"
        update_stt = f"UPDATE {self._table} SET {set_value} WHERE {bridge._bridge_id_col} = '{bridge.id}' AND {self.active_date_query}"

        with self._engine.connect() as cur:
            cur.execute(text(update_stt))
            cur.commit()

        return

    def retire(self, bridge: BridgeMaster):
        """
        Retire BridgeMaster based on Bridge ID.
        """
        df = from_arrow(bridge.artable)

        # Set the end date value
        df = df.with_columns(
            LAST_INV_DATE=col("LAST_INV_DATE").dt.strftime("%d/%b/%Y"),
            END_DATE=lit(datetime.now().strftime("%d/%b/%Y, %I:%M:%S%p"))
        )

        # Create set and update statement.
        set_value = self._create_update_set(df)
        update_stt = f"UPDATE {self._table} SET {set_value} WHERE {bridge._bridge_id_col} = '{bridge.id}' AND {self.active_date_query}"

        with self._engine.connect() as cur:
            cur.execute(text(update_stt))
            cur.commit()

        return

    def delete(self, bridge: BridgeMaster):
        """
        Delete Bridge data based on its Bridge ID.
        """
        delete_statement = f"DELETE FROM {self._table} WHERE {bridge._bridge_id_col} = '{bridge.id}'"
        
        with self._engine.connect() as cur:
            cur.execute(text(delete_statement))
            cur.commit()

        return
    
    def _create_update_set(self, df: DataFrame):
        """
        Update using Polars DataFrame.
        """
        # Create set and update statement.
        set_statement = str(df.columns).strip('[]').replace("'", '"').replace(',', "='{}',") + "='{}'"

        # Escape single quote
        # Example case is BRIDGE_NAME = "IDANO BA'A" needs to be converted to "IDANO BA''A" for the update statement
        df = df.with_columns(col(String()).str.replace(r"'", "''"))

        set_value = df.select(format(*[set_statement] + df.columns))['literal'][0]
        
        return set_value