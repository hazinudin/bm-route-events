from ..base import RouteSegmentEvents
from ..base.schema import RouteSegmentEventSchema
from pydantic import TypeAdapter
from typing import List
import os
import polars as pl


class RoutePCI(RouteSegmentEvents):
    """
    Route segment PCI model.
    """
    @classmethod
    def from_excel(
        cls,
        excel_path: str,
        linkid: str,
        linkid_col: str = 'LINKID',
        ignore_review: bool = False,
        data_year: int = None,
        segment_length: float = 0.05
    ):
        """
        Parse data from Excel file to Arrow format and load it into PCI object.
        """
        config_path = os.path.dirname(__file__) + '/schema.json'
        segment_length = segment_length

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

        # Pydantic validation
        ta = TypeAdapter(List[schema.model])
        df = pl.DataFrame(
            ta.validate_python(df_str.to_dicts()),
            infer_schema_length=None
        ).filter(
            pl.col(linkid_col) == linkid
        )

        return cls(
            artable=df.to_arrow(),
            route=linkid,
            segment_length=segment_length,
            data_year=data_year
        )
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Default columns
        self._pci_col = 'PCI'

        # Damage column prefix
        self._dvol = 'VOL_'  # Damage volume
        self._dsev = 'SEV_'  # Damage severity
        
        # PCI max and min value
        self._pci_max = 100
        self._pci_min = 0

        # Asphalt damages
        self._as_damages = [
            'AS_ALG_CRACK',
            'AS_EDGE_CRACK',
            'AS_LONG_CRACK',
            'AS_OTHER_CRACK',
            'AS_POTHOLE',
            'AS_PATCHING',
            'AS_RUTTING',
            'AS_CORRUGATION',
            'AS_DEPRESSION',
            'AS_RAVELING',
            'AS_BLEEDING',
            'AS_SH_DROPOFF',
        ]

        # Rigid damages
        self._rg_damages = [
            'RG_COBREAK',
            'RG_DIV_SLAB',
            'RG_FAULTING',
            'RG_LINE_CRACK',
            'RG_PUNCH_OUT',
            'RG_SHRINKAGE',
            'RG_PUMPING',
            'RG_COSPALLING',
            'RG_JSPALLING',
            'RG_JSEAL',
            'RG_PATCHING',
            'RG_SH_DROPOFF'
        ]
    
    @property
    def asphalt_damages(self) -> list:
        """
        Return the list of all asphalt damages.
        """
        return self._as_damages

    @property
    def rigid_damages(self) -> list:
        """
        Return the list of all rigid damages.
        """
        return self._rg_damages
    
    @property
    def all_damages(self) -> list:
        """
        Return the list of all damages.
        """
        return self._as_damages + self._rg_damages
    
    def invalid_pci_value(self) -> pl.DataFrame:
        """
        Segment with invalid PCI value if compared to its damage columns.
        """
        segments = self.pl_df.select(
            self._linkid_col,
            self._from_sta_col,
            self._to_sta_col,
            self._lane_code_col,
            self._pci_col,
            *[
                pl.col(f"{self._dvol}{dcol}" for dcol in self.all_damages).is_null()
            ]
        ).filter(
            pl.all_horizontal(
                *[f"{self._dvol}{dcol}" for dcol in self.all_damages]
            ).not_().and_(pl.col(self._pci_col).eq(self._pci_max)) 
            |
            pl.all_horizontal(
                *[f"{self._dvol}{dcol}" for dcol in self.all_damages]
            ).and_(pl.col(self._pci_col).lt(self._pci_max))
        )
        
        return segments
