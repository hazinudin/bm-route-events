from sqlalchemy import Engine, inspect, text
from sqlalchemy.exc import NoSuchTableError
from .model import RouteFWD
from ...utils import ora_pl_dtype
from ...utils.oid import has_objectid, generate_objectid
import polars as pl
from datetime import datetime


class RouteFWDRepo(object):
    def __init__(self, sql_engine: Engine):
        self._table = "fwd"
        self._engine = sql_engine
        self._inspect = inspect(sql_engine)

    @property
    def table(self):
        return self._table

    def _full_table_name(
        self,
        year: int,
        semester: int,
    ):
        """
        The FWD table full name.
        """
        return f"{self.table}_{semester}_{year}"

    def get_by_linkid(
        self,
        linkid: str,
        year: int,
        semester: int = 2,
        raise_if_table_does_not_exists: bool = False,
    ) -> RouteFWD:
        """
        Get Route FWD data from database and load it into RouteFWD object.
        """
        if type(year) is not int:
            raise TypeError("'year' is not integer.")

        full_table_name = self._full_table_name(year, semester)

        query = f"select * from {full_table_name} where linkid = '{linkid}'"

        if (
            not self._inspect.has_table(f"{full_table_name}")
            and raise_if_table_does_not_exists
        ):
            raise NoSuchTableError(f"Table {full_table_name} does not exists")

        df = pl.read_database(query, connection=self._engine, infer_schema_length=None)

        obj = RouteFWD(df.to_arrow(), route=linkid, data_year=year, lane_data=True)

        return obj

    def put(self, events: RouteFWD, year: int, semester: int = 2):
        """
        Put Route FWD data into FWD geodatabase table.
        """
        with (
            self._engine.connect() as conn,
            conn.execution_options(isolation_level="READ COMMITTED"),
        ):
            try:
                self._delete(
                    events, conn=conn, commit=False, year=year, semester=semester
                )
                self._insert(
                    events, conn=conn, commit=False, year=year, semester=semester
                )
            except Exception as e:
                conn.rollback()
                raise e

            conn.commit()

        return

    def _delete(
        self, events: RouteFWD, year: int, semester: int, conn, commit: bool = True
    ):
        """
        Delete FWD data from FWD geodatabase table.
        """
        _where = f" where {events._linkid_col} = '{events.route_id}'"
        full_table_name = self._full_table_name(year, semester)
        _del_stt = f"delete from {full_table_name}" + _where

        if self._inspect.has_table(full_table_name):
            try:
                conn.execute(text(_del_stt))
            except Exception as e:
                conn.rollback()
                raise e

            if commit:
                conn.commit()

    def _insert(
        self, events: RouteFWD, year: int, semester: int, conn, commit: bool = True
    ):
        """
        Insert FWD data into FWD geodatabase table.
        """
        full_table_name = self._full_table_name(year, semester)
        try:
            if self._inspect.has_table(full_table_name):
                if has_objectid(full_table_name, self._engine):
                    oids = generate_objectid(
                        schema="smd",
                        table=full_table_name,
                        sql_engine=self._engine,
                        oid_count=events.pl_df.select(pl.len()).rows()[0][0],
                    )

                    args = [pl.Series("OBJECTID", oids)]

                else:
                    args = []

                events.pl_df.with_columns(
                    pl.lit(datetime.now()).dt.datetime().alias("UPDATE_DATE"),
                    pl.lit(0).alias("COPIED"),
                    *args,
                ).write_database(
                    full_table_name,
                    connection=conn,
                    if_table_exists="append",
                    engine_options={
                        "dtype": ora_pl_dtype(events.pl_df, date_cols_keywords="DATE")
                    },
                )

            else:
                events.pl_df.with_columns(
                    pl.lit(datetime.now()).dt.datetime().alias("UPDATE_DATE"),
                    pl.lit(0).alias("COPIED"),
                ).write_database(
                    full_table_name,
                    connection=conn,
                    if_table_exists="append",
                    engine_options={
                        "dtype": ora_pl_dtype(events.pl_df, date_cols_keywords="DATE")
                    },
                )

        except Exception as e:
            conn.rollback()
            raise e

        if commit:
            conn.commit()
