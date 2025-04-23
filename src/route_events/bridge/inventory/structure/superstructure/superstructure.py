import pyarrow as pa
from route_events.bridge.inventory.structure.substructure import Substructure, SubstructureSchema
from route_events.bridge.inventory.structure.element import StructureElement, ElementSchema
from .sups_schema import SuperstructureSchema
import polars as pl
from typing import List, Optional, Literal
import json


class Superstructure(object):
    @classmethod
    def from_invij_popup(cls, bridge_id: str, inv_year: int, data: dict, validate=True, span_num_col: str='SPAN_NUMBER',
                         span_type_col: str='SPAN_TYPE', span_seq_col: str='SPAN_SEQ_COL'):
        """
        Create Superstructure object from INVIJ JSON format, with only initializing the superstructure.
        """
        if validate:
            def _upper_case_input(d: dict):
                data_u_str = json.dumps(d).upper().replace("NULL", "null").replace("FALSE", "false").replace("TRUE", "true")
                return json.loads(data_u_str)

            sups_schema = SuperstructureSchema()

            class SupsModel(sups_schema.model):
                pass

            spans_data = [SupsModel.model_validate(_upper_case_input(_data)).model_dump(by_alias=True) for _data in data]
        else:
            spans_data = data
        
        # Spans DataFrame
        # Drop the BANGUNAN_BAWAH and ELEMEN from DataFrame
        spans_df = pl.DataFrame(spans_data)

        # Add bridge_id and inv_year
        spans_df = spans_df.with_columns(**{
            "BRIDGE_ID": pl.lit(bridge_id),
            "INV_YEAR": pl.lit(inv_year)
        })

        # Superstructure object
        spans = cls(spans_df.to_arrow(), validate=False)

        return spans
        
    @classmethod
    def from_invij(cls, bridge_id: str, inv_year: int, data: dict, validate=True, subs_key='BANGUNAN_BAWAH',
                   span_num_col: str='SPAN_NUMBER', span_type_col: str='SPAN_TYPE', span_seq_col: str = 'SPAN_SEQ'):
        """
        Create Superstructure object from INVIJ JSON format.
        """
        if validate:
            def _upper_case_input(d: dict):
                data_u_str = json.dumps(d).upper().replace("NULL", "null").replace("FALSE", "false").replace("TRUE", "true")
                return json.loads(data_u_str)

            sups_schema = SuperstructureSchema()
            subs_schema = SubstructureSchema()
            element_schema = ElementSchema()

            class ElementModel(element_schema.model):
                L4: Optional[List]

            class SubsModel(subs_schema.model):
                ELEMEN: List[ElementModel]

            class SupsModel(sups_schema.model):
                BANGUNAN_BAWAH: List[SubsModel]
                ELEMEN: List[ElementModel]

            spans_data = [SupsModel.model_validate(_upper_case_input(_data)).model_dump(by_alias=True) for _data in data]
        else:
            spans_data = data

        # Spans DataFrame
        # Drop the BANGUNAN_BAWAH and ELEMEN from DataFrame
        spans_df = pl.DataFrame(spans_data).drop([subs_key, 'ELEMEN'])

        # Add bridge_id and inv_year
        spans_df = spans_df.with_columns(**{
            "BRIDGE_ID": pl.lit(bridge_id),
            "INV_YEAR": pl.lit(inv_year)
        })

        # Superstructure object
        spans = cls(spans_df.to_arrow(), validate=False)

        # Substructure data and object initiation
        subs = [Substructure.from_invij(
            bridge_id,
            inv_year,
            span[span_num_col], 
            span[span_type_col],
            span[span_seq_col],
            span[subs_key],
            validate=False
        ) for span in spans_data]
        
        # Element data and object initiation
        elements = [StructureElement.from_invij(
            span['ELEMEN'],
            {   
                spans._bridge_id_col: bridge_id,
                spans._inv_year_col: inv_year,
                span_num_col: span[span_num_col],
                span_type_col: span[span_type_col],
                span_seq_col: span[span_seq_col]
            }
        ) for span in spans_data]

        # Add Substructure to Superstructure
        for substruct in subs:
            spans.add_substructure(substruct)

        # Add Element to Superstructure
        for element in elements:
            spans.add_l3l4_elements(element)

        return spans
    
    def __init__(self, data: pa.Table, validate=True):
        if validate:
            sups_schema = SuperstructureSchema()

            class SupsModel(sups_schema.model):
                BRIDGE_ID: str
                INV_YEAR: int
            
            SupsModel.model_validate(pl.from_arrow(data).row(0, named=True))

        # Default columns name
        self._bridge_id_col = 'BRIDGE_ID'
        self._inv_year_col = 'INV_YEAR'
        self._span_num_col = 'SPAN_NUMBER'
        self._span_type_col = 'SPAN_TYPE'
        self._span_seq_col = 'SPAN_SEQ'
        self._span_len_col = 'SPAN_LENGTH'
        self._span_struct_col = 'SUPERSTRUCTURE'
        self._span_width_col = 'FLOOR_WIDTH'
        self._span_sidew_col = 'SIDEWALK_WIDTH'

        # Data
        self.artable = data
        self._subs = None
        self._elements = None

        # Could only store single BRIDGE_ID and INV_YEAR
        if self.pl_df[[self._bridge_id_col, self._inv_year_col]].unique().shape[0] > 1:
            raise ValueError("Input data contains multiple BRIDGE_ID and INV_YEAR")

    def add_substructure(self, obj: Substructure, replace=False):
        """
        Add Substructure to the Superstructure object. If replace=False then append.
        """
        if type(obj) != Substructure:
            raise TypeError(f"Could only set substructure with Substructure object, not with {type(obj)}.")

        # Check if Substructure span parents exists in this class
        if self.pl_df.join(pl.from_arrow(obj.artable), 
                           on=[self._span_num_col, self._span_type_col, self._span_seq_col], 
                           how='inner').shape[0] != obj.artable.shape[0]:
            raise ValueError(f"Substructure with parent does not exists in this object.")

        if (self._subs is None) or replace:
            self._subs = obj
        else:
            concat_artable = pa.concat_tables([self._subs.artable, obj.artable])

            # Create new Substructure object
            subs = Substructure(concat_artable)
            subs.add_l3_l4_elements(self._subs.elements)
            subs.add_l3_l4_elements(obj.elements)

            # Replace with new Substructure object
            self._subs = subs

        return self
    
    def add_l3l4_elements(self, obj):
        """
        Add Element to Superstructure object.
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
                self._span_seq_col
            ],
            how='anti'
        ).is_empty():
            raise ValueError("Element with parent span does not exists in this object.")

        if self._elements is None:
            self._elements = obj
        else:
            self._elements.artable = pa.concat_tables([self._elements.artable, obj.artable])

        return self
    
    @property
    def subs(self):
        return self._subs
        
    @property
    def elements(self):
        return self._elements
        
    @property
    def pl_df(self):
        return pl.from_arrow(self.artable)
    
    @property
    def bridge_id(self):
        """
        Return Bridge ID of the superstructure.
        """
        return self.pl_df[self._bridge_id_col][0]
    
    @property
    def inv_year(self):
        """
        Return inventory year of the data.
        """
        return self.pl_df[self._inv_year_col][0]
    
    @property
    def last_span_number(self):
        """
        Return the last span number from the main span.
        """
        return self.pl_df.filter(
            pl.col(self._span_type_col) == 'UTAMA'
        ).select(pl.col(self._span_num_col)).max()[self._span_num_col][0]
    
    def has_unique_span_number(self) -> dict:
        """
        Check if the span number from all span/seq is unique.
        """
        results = self.pl_df.group_by(
            [self._span_type_col, self._span_seq_col]
        ).agg(
            is_unique = pl.col(self._span_num_col).count() ==
            pl.col(self._span_num_col).n_unique()
        ).to_dicts()

        out_dict = {}

        for row in results:
            out_dict[(
                row[self._span_type_col],
                row[self._span_seq_col]
            )] = row['is_unique']

        return out_dict
    
    def has_monotonic_span_number(
            self, 
            span_type: Literal['utama', 'kanan', 'kiri'],
            seq: int = 1
            ) -> bool:
        """
        Check if the span number of the specified span has monotonic pattern.
        """
        return self.pl_df.filter(
            (pl.col(self._span_type_col) == span_type.upper()) &
            (pl.col(self._span_seq_col) == seq)
            ).select(
                pl.col(self._span_num_col).
                diff().
                drop_nulls().
                eq(1).
                all()
            ).to_dicts()[0][self._span_num_col]
    
    def has_monotonic_span_seq_number(self) -> dict:
        """
        Check if the span sequence from all span type has monotonic pattern.
        """
        results = self.pl_df.group_by(
            self._span_type_col
        ).agg(
            pl.col(self._span_seq_col).
            sort().
            diff().
            fill_null(0).
            le(1).
            all()
            ).to_dicts()
        
        out_dict = {}

        for row in results:
            out_dict[row[self._span_type_col]] = row[self._span_seq_col]

        return out_dict
    
    def total_length(
            self, 
            span_type: Literal['utama', 'kanan', 'kiri'],
            seq: int = 1            
            ) -> float:
        """
        Total span length from the specified span type.
        """
        return self.pl_df.filter(
            (pl.col(self._span_type_col) == span_type.upper()) &
            (pl.col(self._span_seq_col) == seq)
        ).select(
            LENGTH=pl.col(self._span_len_col).sum()
        )[0, 0]
    
    def get_span_numbers(
            self, 
            span_type: Literal['utama', 'kanan', 'kiri', 'all'] = 'all',
            seq: int = 1
            ) -> pl.DataFrame:
        """
        Get all span numbers.
        """
        if span_type.lower() != 'all':
            return self.pl_df.filter(
                (pl.col(self._span_type_col) == span_type.upper()) &
                (pl.col(self._span_seq_col) == seq)
                ).group_by(
                    [self._span_type_col, self._span_seq_col]
                ).agg(pl.col(self._span_num_col))
        else:
            return self.pl_df.group_by(
                [self._span_type_col, self._span_seq_col]
                ).agg(pl.col(self._span_num_col))
        
    def get_span_count(
            self,
            span_type: Literal['utama', 'kanan', 'kiri'],
            seq: int = 1
    ) -> int:
        """
        Get span count.
        """
        return self.pl_df.filter(
            (pl.col(self._span_type_col) == span_type.upper()) &
            (pl.col(self._span_seq_col) == seq)
        ).group_by(
            [self._span_type_col, self._span_seq_col]
        ).agg(pl.col(self._span_num_col).count())[self._span_num_col][0]
            
    def select_structure_type(self, structure: str) -> pl.DataFrame:
        """
        Select span based on its structure type.
        """
        return self.pl_df.filter(
            pl.col(self._span_struct_col).str.starts_with(structure)
        )
    
    def join(self, other) -> pl.DataFrame:
        """
        Join other Superstructure object using span type, sequence and number.
        """
        if type(self) != type(other):
            raise TypeError("'other' is not a Superstructure object.")
        
        return self.pl_df.join(
            other, 
            on=[
            self._span_type_col, 
            self._span_seq_col, 
            self._span_num_col
            ]
            )
