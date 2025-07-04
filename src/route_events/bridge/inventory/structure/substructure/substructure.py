import pyarrow as pa
import polars as pl
from typing import List, Optional
from .subs_schema import SubstructureSchema
from ..element import StructureElement, ElementSchema


class Substructure(object):
    @classmethod
    def from_invij(cls, bridge_id: str, inv_year: int, span_num: int, span_type: str, 
                   span_seq: int, data: list, validate: bool=True):
        """
        Create Substructure object from INVIJ JSON format.
        """
        if validate:
            subs_schema = SubstructureSchema()
            element_schema = ElementSchema()

            class ElementModel(element_schema.model):
                L4: Optional[List]

            class SubsModel(subs_schema.model):
                ELEMEN: List[ElementModel]

            [SubsModel.model_validate(_data) for _data in data]

        df = pl.DataFrame(data).drop(['ELEMEN'])

        df = df.with_columns(**{
            "BRIDGE_ID": pl.lit(bridge_id),
            "INV_YEAR": pl.lit(inv_year),
            "SPAN_NUMBER": pl.lit(span_num),
            "SPAN_TYPE": pl.lit(span_type),
            "SPAN_SEQ": pl.lit(span_seq)
        })

        # Initialize Substructure
        subs = cls(df.to_arrow(), validate=False)

        # Element data and object initiation
        elements = [StructureElement.from_invij(
            sub['ELEMEN'],
            {   
                subs._bridge_id_col: bridge_id,
                subs._inv_year_col: inv_year,
                subs._span_num_col: span_num,
                subs._span_type_col: span_type,
                subs._span_seq_col: span_seq,
                subs._abt_status_col: sub[subs._abt_status_col],
                subs._abt_num_col: sub[subs._abt_num_col]
            }
        ) for sub in data]

        # Add Element to Substructure
        for element in elements:
            subs.add_l3_l4_elements(element)

        return subs
    
    def __init__(self, data: pa.Table, validate=True):
        # Default columns name
        self._bridge_id_col = 'BRIDGE_ID'
        self._inv_year_col = 'INV_YEAR'
        self._span_num_col = 'SPAN_NUMBER'
        self._span_type_col = 'SPAN_TYPE'
        self._span_seq_col = 'SPAN_SEQ'

        # Substructure default columns
        self._abt_num_col = 'SUBS_NUMBER'
        self._abt_status_col = 'STATUS'

        if validate:
            subs_schema = SubstructureSchema()

            class SubsModel(subs_schema.model):
                BRIDGE_ID: str
                INV_YEAR: int
                SPAN_NUMBER: int
                SPAN_TYPE: str
                SPAN_SEQ: int

            SubsModel.model_validate(pl.from_arrow(data).row(0, named=True))

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

        # Check if Element span parents exists in this class
        if not pl.from_arrow(obj.artable).join(
            self.pl_df,
            on=[
                self._bridge_id_col,
                self._span_num_col,
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
            self._elements.artable = pa.concat_tables([self._elements.artable, obj.artable])

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
            [self._span_type_col, self._span_seq_col]
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
            [self._span_type_col, self._span_seq_col]
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
            pl.col(self._span_num_col).n_unique(),
            pl.col(self._abt_num_col).n_unique()
        ).to_dicts()

        out_dict = {}

        for row in results:
            out_dict[(
                row[self._span_type_col],
                row[self._span_seq_col]
            )] = {
                self._span_num_col: row[self._span_num_col],
                self._abt_num_col: row[self._abt_num_col]
            }
            
        return out_dict
