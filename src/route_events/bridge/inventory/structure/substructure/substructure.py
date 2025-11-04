import pyarrow as pa
import polars as pl
from typing import List, Optional, Annotated
from pydantic import TypeAdapter, Field, AliasChoices, ConfigDict, StringConstraints
from .subs_schema import SubstructureSchema
from ..element import StructureElement, ElementSchema


class Substructure(object):
    @classmethod
    def from_invij(
        cls, 
        bridge_id: str, 
        inv_year: int, 
        data: list, 
        validate: bool=True
    ):
        """
        Create Substructure object from INVIJ JSON format.
        """
        if validate:
            subs_schema = SubstructureSchema()
            element_schema = ElementSchema()

            class ElementModel(element_schema.model):
                L4: Optional[List] = Field(
                    validation_alias=AliasChoices('l4', 'L4')
                )

            class SubsModel(subs_schema.model):
                BRIDGE_ID: str = str(bridge_id).upper()
                INV_YEAR: int = inv_year
                ELEMEN: List[ElementModel] = Field(
                    validation_alias=AliasChoices('elemen', 'ELEMEN')
                )

            data = [SubsModel.model_validate(_data).model_dump(by_alias=True) for _data in data]
            df = pl.DataFrame(data).drop(['ELEMEN'])
        else:
            df = pl.DataFrame(data).rename(str.upper).drop(['ELEMEN'])

        # Initialize Substructure
        subs = cls(df.to_arrow(), validate=False)

        # Element data and object initiation
        elements = [StructureElement.from_invij(
            sub['ELEMEN'],
            {   
                subs._bridge_id_col: str(bridge_id).upper(),
                subs._inv_year_col: inv_year,
                subs._span_type_col: sub['SPAN_TYPE'],
                subs._span_seq_col: sub['SPAN_SEQ'],
                subs._abt_status_col: sub[subs._abt_status_col],
                subs._abt_num_col: sub[subs._abt_num_col]
            }
        ) for sub in data if len(sub['ELEMEN']) != 0]

        # Add Element to Substructure
        for element in elements:
            subs.add_l3_l4_elements(element)

        return subs
    
    def __init__(self, data: pa.Table, validate=True):
        # Default columns name
        self._bridge_id_col = 'BRIDGE_ID'
        self._inv_year_col = 'INV_YEAR'
        self._span_type_col = 'SPAN_TYPE'
        self._span_seq_col = 'SPAN_SEQ'

        # Substructure default columns
        self._abt_num_col = 'SUBS_NUMBER'
        self._abt_status_col = 'STATUS'

        if validate:
            subs_schema = SubstructureSchema()

            class SubsModel(subs_schema.model):
                BRIDGE_ID: Annotated[str, StringConstraints(to_upper=True)]
                INV_YEAR: int

            ta = TypeAdapter(List[SubsModel])
            ta.validate_python(pl.DataFrame(data).rows(named=True))

        # Arrow Table
        self.artable = data

        # Elements
        self._elements = None

        # Could only store single BRIDGE_ID and INV_YEAR
        if self.pl_df[[self._bridge_id_col, self._inv_year_col]].unique().shape[0] > 1:
            raise ValueError("Input data contains multiple BRIDGE_ID and INV_YEAR")
    
    def add_l3_l4_elements(self, obj):
        """
        Add Element to Substructure object.
        """
        if type(obj) != StructureElement:
            raise TypeError(f"Could not only set element with StructureElement object, not with {type(obj)}.")

        if obj.artable.num_rows == 0:
            return self
        
        # Check if Element span parents exists in this class
        if not pl.from_arrow(obj.artable).join(
            self.pl_df,
            on=[
                self._bridge_id_col,
                self._span_type_col,
                self._span_seq_col,
                self._abt_status_col,
                self._abt_num_col
            ],
            how='anti'
        ).is_empty():
            raise ValueError("Element with parent substructure does not exists in this object.")

        if self._elements is None:
            self._elements = obj
        else:
            self._elements.artable = pl.concat([pl.from_arrow(obj.artable), pl.from_arrow(self._elements.artable)]).to_arrow()

        return self
    
    @property
    def elements(self):
        return self._elements
        
    @property
    def pl_df(self):
        return pl.from_arrow(self.artable)
    
    @property
    def bridge_id(self):
        """
        Return Bridge ID of the substructure.
        """
        return self.pl_df[self._bridge_id_col][0]
    
    @property
    def inv_year(self):
        """
        Return inventory year of the data.
        """
        return self.pl_df[self._inv_year_col][0]
    
    def has_monotonic_subs_number(self) -> dict:
        """
        Check if the substructure number is monotonic for every span type and sequence.
        """
        results = self.pl_df.group_by(
            [self._span_type_col, self._span_seq_col, self._abt_status_col]
        ).agg(
            pl.col(self._abt_num_col).
            sort().
            diff().
            fill_null(0).
            le(1).
            all()
        ).to_dicts()

        out_dict = {}

        for row in results:
            out_dict[(row[self._span_type_col], row[self._span_seq_col])] = row[self._abt_num_col]
        
        return out_dict
    
    def has_unique_subs_number(self) -> dict:
        """
        Check if the subs number is unique from all span/seq
        """
        results = self.pl_df.group_by(
            [self._span_type_col, self._span_seq_col, self._abt_status_col]
        ).agg(
            is_unique = pl.col(self._abt_num_col).n_unique() == 
            pl.col(self._abt_num_col).count()
        ).to_dicts()

        out_dict = {}

        for row in results:
            out_dict[(
                row[self._span_type_col],
                row[self._span_seq_col]
            )] = row['is_unique']

        return out_dict
    
    def span_subs_count(self) -> pl.DataFrame:
        """
        Count number of substructure for every span/seq.
        """
        results = self.pl_df.group_by(
            [self._span_type_col, self._span_seq_col]
        ).agg(
            pl.col(self._abt_num_col).n_unique()
        ).to_dicts()

        out_dict = {}

        for row in results:
            out_dict[(
                row[self._span_type_col],
                row[self._span_seq_col]
            )] = {
                'SUBS_COUNT': row[self._abt_num_col]
            }
            
        return out_dict
