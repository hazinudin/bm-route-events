import pyarrow as pa
import polars as pl
from .element_schema import ElementSchema


class StructureElement(object):
    @classmethod
    def from_invij(cls, data: dict, parent_keys: dict, l4_col: str="L4"):
        """
        Create Element object from INVIJ JSON format.
        """
        df = pl.DataFrame(data)

        if not df[l4_col].is_null().all():  # Means the L4 contains Elements (not null)
            df = df.explode(l4_col).rename({l4_col: "_L4_STRUCT"})
            
            ## Old format ##
            # df = df.with_columns(**{l4_col: pl.col("_L4_STRUCT").\
            #                         map_elements(
            #                             lambda s: pl.DataFrame(s).transpose(include_header=True, header_name='L4', column_names=['BHN']).drop_nulls().to_struct() if s is not None else s
            #                             ).explode()})

            df = df.with_columns(pl.col('_L4_STRUCT').struct.rename_fields([
                "L4", "MATERIAL"
            ]).struct.unnest())
            
            # df = df.with_columns(pl.col(l4_col).struct.unnest())  # Will update the BHN and L4
            
            df = df.select(pl.exclude('_L4_STRUCT'))  # Remove the _L4_STRUCT
            df = df.unique(subset=["L3", "L4"])  # Will remove any duplicate, especially when L4 is null.

        # Add parent attribute to Arrow Table
        df = df.with_columns(**{k.upper():pl.lit(v) for k,v in parent_keys.items()})

        return cls(df.to_arrow())
        
    def __init__(self, data: pa.Table):
        # Add parent keys to Arrow Table.
        self.artable = data
        
    @property
    def pl_df(self):
        return pl.from_arrow(self.artable)
