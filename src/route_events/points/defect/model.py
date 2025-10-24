from ..base import RoutePointEvents
import os
from ...schema import RouteEventsSchema
from ...segments.rni import surface_types
import polars as pl
from pydantic import TypeAdapter
from typing import List


class RouteDefects(RoutePointEvents):
    """
    Model of route defect points.
    """
    @classmethod
    def from_excel(
        cls,
        excel_path: str,
        linkid: str | list = 'ALL',
        linkid_col: str = 'LINKID',
        ignore_review: bool = False,
        data_year: int = None,
        photo_url_col: str = 'URL_PHOTO'
    ):
        """
        Parse data from Excel file to Arrow format and load it into Route Defect object.
        """
        if type(linkid) is not str:
            raise TypeError(f"Only accept single route in str type, not {linkid}")
        
        if linkid == 'ALL':
            route_filter = pl.col(linkid_col).is_not_null()
        else:
            route_filter = pl.col(linkid_col).eq(linkid)
        
        config_path = os.path.dirname(__file__) + '/schema.json'

        schema = RouteEventsSchema(
            file_path=config_path,
            ignore_review_err=ignore_review
        )

        df_str = pl.read_excel(
            excel_path,
            engine='calamine',
            infer_schema_length=None
        ).rename(
            str.upper
        ).cast(
            pl.String
        ).with_columns(
            pl.col(pl.String).exclude(photo_url_col).str.to_uppercase()
        )

        ta = TypeAdapter(List[schema.model])
        df = pl.DataFrame(
            ta.validate_python(df_str.to_dicts()),
            infer_schema_length=None
        ).filter(
            route_filter
        )

        return cls(
            artable=df.to_arrow(),
            route=linkid,
            data_year=data_year,
            lane_data=True
        )
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        # Columns
        self._photo_url_cols = 'URL_PHOTO'
        self._surf_type_col = 'SURF_TYPE'
        self._defects_type_col = 'DEFECTS_TYPE'
        self._defects_dimension_col = 'DEFECTS_DIMENSION'
        self._defects_severity_col = 'DEFECTS_SEVERITY'

        # Defects type which can/should have null severity
        self._defect_no_severity = [
            "AS_POLISHED_AGGREGATE"
        ]

        self.lane_data = True

        # Surface types mapping for types in data
        self._surf_types_map = None

    def invalid_severity(self) -> pl.DataFrame:
        """
        Check the severity value, there are damages type that can and should have null damage severity.
        """
        error = self.pl_df.filter(
            pl.col(self._defects_type_col).is_in(self._defect_no_severity).not_() &
            pl.col(self._defects_severity_col).is_null()
        )

        return error

    @property
    def surface_type_mapping(self) -> pl.DataFrame:
        """
        Return surface types mapping and other properties for available surfaces in this model.
        """
        if self._surf_types_map is None:
            self._surf_types_map = pl.DataFrame(surface_types).cast({
                'iri_kemantapan': pl.Array(shape=(3,), inner=pl.Int16),
                'pci_kemantapan': pl.Array(shape=(3,), inner=pl.Int16),
                'iri_rating': pl.Array(shape=(4,), inner=pl.Int16),
                'pci_rating': pl.Array(shape=(4,), inner=pl.Int16)
            }).filter(
                pl.col('surf_type').is_in(
                    self.pl_df[self._surf_type_col].unique()
                )
            )
            return self._surf_types_map
        
        else:
            return self._surf_types_map

