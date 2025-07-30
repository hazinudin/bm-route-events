import pyarrow as pa
from ...inventory import (
    Superstructure,
    Substructure,
    SuperstructureSchema,
    SubstructureSchema,
    ElementSchema
)
from ....geometry import Point, LAMBERT_WKT
from .schema import InventoryProfileSchema
import polars as pl
from typing import List, Literal, Optional
from pydantic import Field, AliasChoices
import json
import duckdb


class BridgeInventory(object):
    @classmethod
    def from_invij_popup(cls, data:dict, sups_key='BANGUNAN_ATAS', ignore_review_err=False):
        """
        Load popup data INVIJ input to BridgeInventory object.
        """
        profile_schema = InventoryProfileSchema(ignore_review_err)
        sups_schema = SuperstructureSchema(ignore_review_err)

        profile_data = profile_schema.model.model_validate(data).model_dump(by_alias=True)

        # Pydantic validation models
        class SupsModel(sups_schema.model):
            BRIDGE_ID: str = str(profile_data['BRIDGE_ID']).upper()
            INV_YEAR: int = profile_data['INV_YEAR']

        class InvModel(profile_schema.model):
            BANGUNAN_ATAS: List[SupsModel] = Field(
                validation_alias=AliasChoices('BANGUNAN_ATAS', 'bangunan_atas')
            )
            INVENTORY_STATE: str = 'POPUP'  # The data state
        
        # Pydantic validation start
        invij_model = InvModel.model_validate(data)
        
        profile_data = invij_model.model_dump(
            exclude=[sups_key, 'MODE', 'VAL_HISTORY'], 
            by_alias=True
        )  # Inventory profile

        sups_data = invij_model.model_dump(
            include=sups_key, 
            by_alias=True
        )[sups_key]  # Superstructure data

        df = pl.from_dicts([profile_data])

        # Initiate BridgeInventory object
        inv = cls(df.to_arrow())

        # Initiate Superstructure object
        sups = Superstructure.from_invij_popup(
            inv.id, 
            inv.inv_year, 
            sups_data, 
            validate=False
            )
        inv.add_superstructure(sups)

        return inv

    @classmethod
    def from_invij(
        cls, 
        data: dict, 
        ignore_review_err=False
    ):
        """
        Load data from INVIJ input to BridgeInventory object.
        """
        profile_schema = InventoryProfileSchema(ignore_review_err)
        sups_schema = SuperstructureSchema(ignore_review_err)
        subs_schema = SubstructureSchema(ignore_review_err)
        element_schema = ElementSchema(ignore_review_err)

        profile_data = profile_schema.model.model_validate(data).model_dump(by_alias=True)
        
        # Pydantic validation models
        class ElementModel(element_schema.model):
            L4: Optional[List] = Field(
                validation_alias=AliasChoices('l4','L4')
            )

        class SubsModel(subs_schema.model):
            BRIDGE_ID: str = str(profile_data['BRIDGE_ID']).upper()
            INV_YEAR: int = profile_data['INV_YEAR']
            ELEMEN: List[ElementModel] = Field(
                validation_alias=AliasChoices('elemen', 'ELEMEN')
            )

        class SupsModel(sups_schema.model):
            BRIDGE_ID: str = str(profile_data['BRIDGE_ID']).upper()
            INV_YEAR: int = profile_data['INV_YEAR']
            ELEMEN: List[ElementModel] = Field(
                validation_alias=AliasChoices('elemen', 'ELEMEN')
            )

        class InvModel(profile_schema.model):
            BANGUNAN_ATAS: List[SupsModel] = Field(
                validation_alias=AliasChoices('BANGUNAN_ATAS', 'bangunan_atas')
            )
            BANGUNAN_BAWAH: List[SubsModel] = Field(
                validation_alias=AliasChoices('BANGUNAN_BAWAH', 'bangunan_bawah')
            )
            MODE: Literal["INSERT", "UPDATE", "RETIRE", 'insert', 'update', 'retire']
            INVENTORY_STATE: str = 'DETAIL' # The data state
            VAL_HISTORY: List

        data = json.loads(json.dumps(data).upper().replace("NULL", "null"))  # Upper case model

        # Pydantic validation start
        invij_model = InvModel.model_validate(data)  # Load as a model
        profile_data = invij_model.model_dump(exclude=['BANGUNAN_ATAS', 'BANGUNAN_BAWAH', 'MODE', 'VAL_HISTORY'], by_alias=True)  # Inventory profile
        sups_data = invij_model.model_dump(include='BANGUNAN_ATAS', by_alias=True)['BANGUNAN_ATAS']  # Superstructure data
        subs_data = invij_model.model_dump(include='BANGUNAN_BAWAH', by_alias=True)['BANGUNAN_BAWAH']
        
        df = pl.from_dicts([profile_data])

        # Initiate BridgeInventory object
        inv = cls(df.to_arrow())

        # Initiate Superstructure object
        sups = Superstructure.from_invij(inv.id, inv.inv_year, sups_data, validate=False)
        inv.add_superstructure(sups)

        # Initiate Substructure object
        subs = Substructure.from_invij(inv.id, inv.inv_year, subs_data, validate=False)
        inv.add_substructure(subs)

        return inv
    
    def __init__(self, inv_data: pa.Table):
        # Columns name
        self._bridge_id_col = 'BRIDGE_ID'
        self._inv_year_col = 'INV_YEAR'
        self._linkid_col = 'LINKID'
        self._length_col = 'BRIDGE_LENGTH'
        self._lat_col = 'LATITUDE'
        self._lon_col = 'LONGITUDE'

        self.artable = inv_data
        self._sups = None
        self._subs = None

        # DuckDB Session
        self.ddb = duckdb.connect()

        if len(self.artable) != 0:
            # Geometry
            self._point_4326 = Point(
                long=self.artable[self._lon_col][0],
                lat=self.artable[self._lat_col][0],
                wkt='EPSG:4326',
                ddb=self.ddb
                )
            
            self._point_lambert = self._point_4326.transform(LAMBERT_WKT, invert=True)
            self._empty = False
        else:
            self._point_4326 = None
            self._point_lambert = None
            self._empty = True

    @property
    def is_empty(self):
        return self._empty
    
    def add_superstructure(self, obj: Superstructure, replace=False):
        """
        Add Superstructure object to BridgeInventory object.
        """
        if type(obj) != Superstructure:
            raise TypeError(f"Could only set superstructure with Superstructure object, not with {type(obj)}.")
        
        if (self._sups is None) or replace:
            self._sups = obj
        else:
            raise AttributeError("Could not update superstructure. Try replace=True")

        return self
    
    def add_substructure(self, obj: Substructure, replace=False):
        """
        Add Substructure object to BridgeInventory object.
        """
        if type(obj) != Substructure:
            raise TypeError(f"Could only set substructure with Substructure object, not with {type(obj)}.")
    
        if (self._subs is None) or replace:
            self._subs = obj
        else:
            raise AttributeError("Could not update substructure. Try replace=True")
        
        return self
    
    @property
    def sups(self)->Superstructure:
        """
        Return Superstructure
        """
        return self._sups
    
    @property
    def subs(self)->Substructure:
        """
        Return Substructure
        """
        return self._subs
    
    @property
    def id(self)->str:
        """
        Return Bridge ID.
        """
        return self.artable[self._bridge_id_col][0].as_py()
    
    @property
    def linkid(self)->str:
        """
        Return LINKID of the bridge.
        """
        return self.artable[self._linkid_col][0].as_py()
    
    @property
    def inv_year(self)->int:
        """
        Return inventory year.
        """
        return self.artable[self._inv_year_col][0].as_py()
    
    @property
    def length(self)->float:
        """
        Return bridge inventory data length
        """
        return self.artable[self._length_col][0].as_py()
    
    @property
    def pl_df(self):
        return pl.from_arrow(self.artable)
    
    def get_span_numbers(self, span_type: Literal['utama', 'kanan', 'kiri', 'all'], seq: int = 1):
        """
        Get all available span numbers.
        """
        return self._sups.get_span_numbers(span_type=span_type, seq=seq)
    
    def get_span_count(self, span_type: Literal['utama', 'kanan', 'kiri'], seq: int = 1) -> int:
        """
        Get span count.
        """
        return self._sups.get_span_count(span_type, seq)
    
    def has_unique_span_number(self) -> dict:
        """
        Check if the span number from all span/seq has unique span number.
        """
        return self._sups.has_unique_span_number()
    
    def has_monotonic_span_number(self, span_type: str) -> bool:
        """
        Check if the span number of the specified span has monotonic pattern.
        """
        return self._sups.has_monotonic_span_number(span_type = span_type)

    def has_monotonic_span_seq_number(self) -> dict:
        """
        Check if the span sequence number of the specified span has monotonic pattern.
        """
        return self._sups.has_monotonic_span_seq_number()
    
    def has_monotonic_subs_number(self):
        """
        Check if the substructure number is monotonic for every span type and sequence.
        """
        return self.subs.has_monotonic_subs_number()
    
    def has_unique_subs_number(self):
        """
        Check if the subs number is unique from all span/seq.
        """
        return self.subs.has_unique_subs_number()
    
    def total_span_length(self, span_type: Literal['utama', 'kanan', 'kiri']) -> float:
        """
        Total span length from the specified span type.
        """
        return self._sups.total_length(span_type)
    
    def select_span_structure(self, structure: str) -> pl.DataFrame:
        """
        Select span based on its structure type.
        """
        return self._sups.select_structure_type(structure)
    
    def span_subs_count(self) -> dict:
        """
        Count number of substructure for evvery span/seq.
        """
        out = self.subs.span_subs_count()

        for _span in out:
            out[_span]['SPAN_COUNT'] = self.get_span_count(_span[0].lower(), _span[1])

        return out
