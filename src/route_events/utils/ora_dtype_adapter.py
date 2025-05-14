from sqlalchemy.dialects.oracle import NUMBER, VARCHAR2, TIMESTAMP
import polars as pl


def ora_pl_dtype(df: pl.DataFrame, date_cols_keywords:str ='DATE') -> dict:
    """
    Return Oracle dtype for table creation.
    """
    out_dict = dict()

    for col in df.schema.items():
        col_name = col[0]
        dtype = col[1]

        if date_cols_keywords in col_name:
            dtype = pl.Datetime

        if dtype == pl.String:
            out_dict[col_name] = VARCHAR2(255)
        elif dtype in (pl.Float64, pl.Float32):
            out_dict[col_name] = NUMBER(38, 8)
        elif dtype in (pl.Int64, pl.Int16, pl.Int32, pl.Int128, pl.Int8):
            out_dict[col_name] = NUMBER(38)
        elif dtype == pl.Datetime:
            out_dict[col_name] = TIMESTAMP(timezone=True)

    return out_dict
