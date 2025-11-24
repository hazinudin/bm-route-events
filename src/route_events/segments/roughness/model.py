from ..base import RouteSegmentEvents
from ..base.schema import RouteSegmentEventSchema
from pydantic import TypeAdapter, BaseModel
from typing import Literal, List
import os
import polars as pl


class RouteRoughness(RouteSegmentEvents):
    """
    Route segment Roughness(IRI) model.
    """
    @classmethod
    def from_excel(
        cls, 
        excel_path: str, 
        linkid: str | list = 'ALL', 
        linkid_col: str = 'LINKID',
        ignore_review = False,
        data_year: int = None,
        data_semester: Literal[1,2] = None
    ):
        """
        Parse data from Excel file to Arrow format and load it into Roughness object.
        """
        config_path = os.path.dirname(__file__) + '/schema.json'
        segment_length = 0.1

        schema = RouteSegmentEventSchema(
            config_path=config_path, 
            ignore_review_err=ignore_review
        )

        df_str = pl.read_excel(
            excel_path, 
            engine='calamine',
            infer_schema_length=None
            ).rename(
                str.upper
            ).cast(
                pl.String  # Cast all values into string for Pydantic validation.
            )
        
        if linkid == 'ALL':
            pass
        elif (type(linkid) == str) and (linkid != 'ALL'):
            df_str = df_str.filter(pl.col(linkid_col) == linkid)
        elif type(linkid) == list:
            df_str = df_str.filter(pl.col(linkid_col).str.is_in(linkid))
        else:
            raise TypeError(f"LINKID argument with type {type(linkid)} is invalid type.")

        # Validate using Pydantic
        ta = TypeAdapter(List[schema.model])
        df = pl.DataFrame(
            ta.validate_python(df_str.to_dicts()),
            infer_schema_length=None
        )
        
        return cls(
            artable=df.to_arrow(),
            route=linkid,
            segment_length=segment_length,
            data_year=data_year,
            data_semester=data_semester
        )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._iri_col = 'IRI' 