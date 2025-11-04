from route_events.bridge import BridgeMasterRepoDB, DETAILED_STATE, POPUP_STATE
from route_events.bridge.inventory import BridgeInventory, BridgeInventoryRepo
from route_events.route import LRSRoute
from pydantic import ValidationError

from ..validation_result.result import ValidationResult
from typing import Literal
from sqlalchemy import Engine
from numpy import isclose
import polars as pl
import json


class BridgeInventoryValidation(object):
    def __init__(
            self,
            data: dict,
            validation_mode: Literal['UPDATE', 'INSERT', 'RETIRE'],
            lrs_grpc_host: str,
            sql_engine: Engine,
            ignore_review: bool = False,
            ignore_force: bool = False,
            **kwargs
    ):
        """
        Bridge Inventory data validation object.
        """

        if type(data) != dict:
            raise TypeError("Only accept dictionary as input data.")
        
        self.validation_mode = validation_mode.upper()

        # BridgeMasterRepoDB using GeoDatabase
        self._bm_repo = BridgeMasterRepoDB(
            sql_engine=sql_engine
            )
        
        # BridgeInventoryRepo using GeoDatabase
        self._repo = BridgeInventoryRepo(
            sql_engine=sql_engine
        )

        # DEVELOPMENT table name
        if kwargs.get("dev"):
            self._repo.inv_table_name = self._repo.inv_table_name + "_DEV"
            self._repo.sups_table_name = self._repo.sups_table_name + "_DEV"
            self._repo.subs_table_name = self._repo.subs_table_name + "_DEV"
            self._repo.sups_el_table_name = self._repo.sups_el_table_name + "_DEV"
            self._repo.subs_el_table_name = self._repo.subs_el_table_name + "_DEV"

        # Initial review messages
        review_msgs = []

        # Pydantic validation
        try:
            if kwargs.get('popup'):
                self._inv = BridgeInventory.from_invij_popup(data)
            else:
                self._inv = BridgeInventory.from_invij(data)
        except ValidationError as e:
            self._result = ValidationResult('failed_request')
            for error in e.errors():
                if 'review' in error['type']:
                    review_msgs.append(error['msg'])
                else:
                    self._result.add_message(error['msg'], 'rejected')
            
            if self.get_status() == 'rejected':
                return
            else:
                if kwargs.get('popup'):
                    self._inv = BridgeInventory.from_invij_popup(
                        data, ignore_review_err=True
                    )
                else:
                    self._inv = BridgeInventory.from_invij(
                        data, ignore_review_err=True
                    )
        
        # LRS object
        try:
            self._lrs = LRSRoute.from_feature_service(lrs_grpc_host, route=self._inv.linkid)
        except:
            print(self._inv.linkid)
            raise
        
        # ValidationResult for tracking and storing validation result and status.
        _ignored_msg = None

        if ignore_review:
            _ignored_msg = ['review']
        elif ignore_force:
            _ignored_msg = ['force']
        elif ignore_force and ignore_review:
            _ignored_msg = ['review', 'force']

        self._result = ValidationResult(self._inv.id, ignore_in=_ignored_msg)

        # BridgeMaster data with the same bridge ID
        self._bm = self._bm_repo.get_by_bridge_id(self._inv.id)

        # BridgeInventory with the same bridge ID
        self._available_inv_years = self._repo.get_available_years(self._inv.id)

        if len(self._available_inv_years) == 0:
            self._current_inv = None
        else:
            self._current_inv = self._repo.get_by_bridge_id(
                self._inv.id, 
                inv_year=max(self._available_inv_years)
            )

        # Add review messages from Pydantic validation.
        for msg in review_msgs:
            self._result.add_message(msg, 'review')

        # Reject the input data if the input data bridge ID does not have master data available.
        if self._bm is None:
            self._result.add_message("Jembatan belum memiliki data pokok jembatan.", "rejected")

        if self._lrs is None:
            self._result.add_message(f'Ruas {self._inv.linkid} bukan merupakan jalan nasional.', 'rejected')

        if (self._current_inv is None) and (self.validation_mode == 'UPDATE'):
            self._result.add_message(f"Jembatan {self._inv.id} tidak tersedia di untuk diupdate.", 'rejected')

    def get_all_messages(self):
        """
        Get validation result messages.
        """
        return self._result.get_all_messages()
    
    def get_status(self):
        """
        Get validation process status.
        """
        return self._result.status
    
    def has_sups_check(self):
        """
        Check if the inventory data has superstructure data.
        """
        if self._inv.sups is None:
            self._result.add_message('Jembatan tidak memiliki data bangunan atas.', 'error')
        
        return self
    
    def has_subs_check(self):
        """
        Check if the inventory data has substructure data.
        """
        if self._inv.subs is None:
            self._result.add_message('Jembatan tidak memiliki data bangunan bawah.', 'error', 'force')
    
    def base_check(
            self, 
            validate_length:bool=True, 
            validate_width:bool=True
        ):
        """
        Base validation function.
        """
        # Check if data has superstructure
        self.has_sups_check()

        # Check if DETAIL inventory has substructure
        if self._inv.inventory_state == DETAILED_STATE:
            self.has_subs_check()
            self.master_data_distance_check()
            self.lrs_distance_check()
            self.compare_total_span_length_to_inv_length_check()
            self.span_width_check()
            self.compare_length_to_master_data_check()
        
        if (self._inv.inventory_state == POPUP_STATE) and validate_length:
            self.compare_total_span_length_to_inv_length_check()
            self.compare_length_to_master_data_check()
    
        if (self._inv.inventory_state == POPUP_STATE) and validate_width:
            self.span_width_check()

        self.main_span_structure_type_check()
        self.main_span_num_check()
        self.span_num_unique_check()
        self.other_span_num_exist_in_main_span_check()
        self.span_seq_check()
        self.master_data_bridge_number_comparison()

        # Only execute if input data has substructure
        if self._inv.subs is not None:
            self.subs_num_unique_check()
            self.span_subs_count_check()
        
        return
    
    def update_check(self):
        """
        Update check.
        """
        self.previous_data_exists_check(should_exists=True)

        # Try to insert substructure to the BridgeInventory object, if the object does not have it
        # Try to insert if the current data is not empty and current data has it.
        # self.get_status is error probably means the current data does not exists, while trying to update.
        if (self._inv.subs is None) and (self._current_inv.subs is not None) and (self.get_status != 'error'):
            try:
                self._inv.sups.add_substructure(self._current_inv.subs)
            except ValueError:
                self._result.add_message('Bangunan atas tidak cocok dengan bangunan bawah inventori jembatan yang sudah ada.', 'error')

        if self.get_status != 'error':
            self.base_check()

            self.superstructure_no_changes()
            self.floor_width_no_changes()
            self.sidewalk_width_no_changes()
            self.compare_main_span_length()

        return
    
    def insert_check(self, validate_length:bool=True, validate_width:bool=True):
        """
        Insert check.
        """
        self.previous_data_exists_check(should_exists=False)

        if self.get_status != 'error':
            self.base_check(
                validate_length=validate_length,
                validate_width=validate_width
            )
    
        return
    
    def previous_data_exists_check(self, should_exists: bool = True):
        """
        Check if previous data exists.
        """
        if should_exists and (self._current_inv is None):
            msg = "Jembatan belum memiliki data inventori."
            self._result.add_message(msg, 'error')

        elif (not should_exists) and (not self._current_inv is None):
            msg = "Jembatan sudah memiliki data inventori."
            self._result.add_message(msg, 'error')

        return
        
    def main_span_num_check(self):
        """
        Main span number check. The main span's number should have monotonic pattern and start from 1.
        """
        if not self._inv.has_monotonic_span_number('UTAMA'):
            msg = "Bentang utama tidak memiliki nomor yang berurutan."
            self._result.add_message(msg, 'error')

        if self._inv.get_span_numbers('utama')[self._inv._sups._span_num_col].list.min()[0] != 1:
            msg = "Bentang utama memiliki nomor bentang yang tidak dimulai dari 1"
            self._result.add_message(msg, 'error')

        return self
    
    def span_num_unique_check(self):
        """
        Check if span number is unique for every span type/seq.
        """
        spans = self._inv.has_unique_span_number()
        for span in spans:
            if not spans[span]:  # Means the span does not have unique span number.
                msg = f"Bentang {span[0]}/{span[1]} memiliki nomor bentang yang tidak unik."
                self._result.add_message(msg, 'error')

        return self
    
    def subs_num_unique_check(self):
        """
        Check if subs number is unique for every span type/seq.
        """
        spans = self._inv.has_unique_subs_number()
        for span in spans:
            if not spans[span]:  # Means the span does not have unique subs number.
                msg = f"Bentang {span[0]}/{span[1]} memiliki nomor bangunan bawah yang tidak unik."
                self._result.add_message(msg, 'error')
            
        return self
    
    def other_span_num_exist_in_main_span_check(self):
        """
        Other span number/seq should exists in the 
        """
        df = self._inv.get_span_numbers(span_type='ALL')
        
        # Query span/seq with span number which does not exists in main span.
        errors = df.with_columns(
            REF=df.filter(pl.col(self._inv.sups._span_type_col) == 'UTAMA')[self._inv.sups._span_num_col]
            ).filter(
                pl.col('REF').list.set_difference(self._inv.sups._span_num_col).list.len() != 0
            ).rows(named=True)
        
        for error in errors:
            span = error[self._inv.sups._span_type_col]
            seq = error[self._inv.sups._span_seq_col]
            invalid_num = error[self._inv.sups._span_num_col]
            # main_num = error['REF']

            msg = f"Bentang {span}/{seq} memiliki nomor bentang yang tidak sepenuhnya cocok dengan nomor bentang utama, yaitu {invalid_num}"

            self._result.add_message(msg, 'error')

    def span_seq_check(self):
        """
        Span sequence number should have monotonic pattern.
        """
        result = self._inv.has_monotonic_span_seq_number()

        for span in result:
            if not result[span]:  # Error, the span does not have correct sequence number
                msg = f"Bentang {span} memiliki urutan pelebaran yang tidak mengikuti kelipatan 1."
                self._result.add_message(msg, 'error')
        
        return self
    
    def compare_length_to_master_data_check(self):
        """
        Compare inventory length with bridge master length data.
        """
        if not isclose(self._inv.length, self._bm.length):
            msg = f"Panjang jembatan pada data inventori ({self._inv.length}m) tidak sama dengan panjang data pokok ({self._bm.length}m)"
            self._result.add_message(msg, 'review', 'review')

        return self
    
    def compare_total_span_length_to_inv_length_check(self):
        """
        Compare inventory total span length data (main span) with inventory total length data.
        If the main superstructure type (first letter) is ```'A', 'B', 'W', or 'Y'```, then the main span total length could be equal to the bridge length.
        """
        total_main_span_len = self._inv.total_span_length('utama')
        culvert_types = ['A', 'B', 'W', 'Y']
        main_span_type = self._inv.span_type[0]  # Only use the first letter

        if (not bool(isclose(total_main_span_len, self._inv.length))):
            if (self._inv.length > total_main_span_len) and ((self._inv.length - total_main_span_len) > 1):
                # If there is difference between main span total length and inventory bridge length and main span length > inventory length.
                # The difference is at least 1m to be notified.
                msg = f"Total panjang bentang utama ({total_main_span_len}m) tidak sama dengan panjang data inventori ({self._inv.length}m). Perlu ditinjau ulang."
                self._result.add_message(msg, 'review', 'review')
            else:
                msg = f"Total panjang bentang utama ({total_main_span_len}m) lebih panjang dari panjang data inventory ({self._inv.length}m)."
                self._result.add_message(msg, "error")
        elif main_span_type not in culvert_types:
            # If main span total length is equal to inventory bridge length, then raise error message
            msg = f"Total panjang bentang utama ({total_main_span_len}m) sama dengan panjang data inventori ({self._inv.length}m)."
            self._result.add_message(msg, 'error', 'force')

        return self
    
    def span_subs_count_check(self):
        """
        Check if the span number matches the substructure number. The substructure number should be N of superstrcuture + 1
        """
        spans = self._inv.span_subs_count()

        for span in spans:
            span_count = spans[span]['SPAN_COUNT']
            subs_count = spans[span]['SUBS_COUNT']

            if subs_count != (span_count + 1):
                msg = f"Bentang {span[0]}/{span[1]} memiliki jumlah bangunan bawah yang tidak cocok dengan jumlah bangunan atas, yaitu {span_count} bangunan atas dan {subs_count} bangunan bawah."

                self._result.add_message(msg, 'error')

        return self
    
    def _span_no_changes(self, column: str, column_alias: str):
        """
        Check if there is changes in superstructure attributes.
        """
        joined = self._inv.sups.join(self._current_inv.sups)

        diff = joined.filter(
            pl.col(column) !=
            pl.col(column + '_right')
        ).select(
            [
                self._inv.sups._span_type_col,
                self._inv.sups._span_seq_col,
                self._inv.sups._span_num_col,
                column,
                column + '_right'
            ]
        ).to_dicts()

        for row in diff:
            span_type = row[self._inv.sups._span_type_col]
            span_seq = row[self._inv.sups._span_seq_col]
            span_num = row[self._inv.sups._span_num_col]
            current_value = row[column]
            prev_value = row[column + '_right']


            msg = f"""Bentang {span_type}/{span_seq} nomor {span_num} memiliki {column_alias} yang berbeda dengan data yang sudah ada, 
            yaitu {current_value} sedangkan data sebelumnya adalah {prev_value}"""

            self._result.add_message(msg, 'review', 'review')

    def superstructure_no_changes(self):
        """
        Check if there is no changes in superstructure type.
        """
        self._span_no_changes(self._inv.sups._span_struct_col, 'bangunan atas')

        return self
    
    def floor_width_no_changes(self):
        """
        Check if there is no changes in span floor width.
        """
        self._span_no_changes(self._inv.sups._span_width_col, 'lantai kendaraan')

        return self
    
    def sidewalk_width_no_changes(self):
        """
        Check if there is no changes in sidewalk width.
        """
        self._span_no_changes(self._inv.sups._span_sidew_col, 'lebar trotoar')

        return self
    
    def compare_main_span_length(self):
        """
        Compare span length and count, if there is changes in main span count then there is also should be increase in total main span length.
        Decrease in number of span in main span is not allowed.
        """
        current_count = self._inv.sups.get_span_count('utama', 1)
        current_length = self._inv.sups.total_length('utama', 1)

        prev_count = self._current_inv.sups.get_span_count('utama', 1)
        prev_length = self._current_inv.sups.total_length('utama', 1)

        if current_count < prev_count:
            msg = f"Jumlah bentang pada bentang utama mengalami penurunan dari {prev_count} ke {current_count}."
            self._result.add_message(msg, 'erorr')
        
        if (current_count > prev_count) and (current_length <= prev_length):
            msg = f"Jumlah bentang pada bentang utama mengalami kenaikan, tapi panjang bentang utama tidak mengalami kenaikan."
            self._result.add_message(msg, 'error')

        if current_length < prev_length:
            msg = f"Panjang total bentang utama mengalami penurunan."
            self._result.add_message(msg, 'error')

        return self
    
    def main_span_structure_type_check(self):
        """
        Compare the main span structure type with the structure type from profile/header data. At least a single span in the
        main spans has the same structure as the profile/header data.
        """
        if not self._inv.span_type in self._inv.get_main_span_structure():
            msg = f"Tipe bentang {self._inv.span_type} tidak cocok dengan tipe bentang utama {self._inv.get_main_span_structure()}"
            self._result.add_message(msg, 'error')

        return self
    
    def lrs_distance_check(self, threshold=30):
        """
        Check the bridge location relative to the LRS.
        """
        distance = self._lrs.distance_to_point(self._inv._point_4326.X, self._bm._point_4326.Y)

        if distance > threshold:
            msg = f"Jembatan berjarak {distance}m dari geometri LRS."
            self._result.add_message(msg, status='error', ignore_in='force')

        return self
    
    def master_data_distance_check(self, threshold=30):
        """
        Check the bridge location relative to the bridge master data.
        """
        distance = self._bm._point_lambert.distance_to(self._inv._point_lambert)

        if distance > threshold:
            msg = f"Jembatan berjarak {distance}m dari data pokok jembatan."
            self._result.add_message(msg, status='error', ignore_in='force')

        return self
    
    def master_data_bridge_number_comparison(self):
        """
        Check if the bridge number between inventory data and master data is different.
        """
        if self._bm.number != self._inv.number:
            msg = f"Jembatan memiliki nomor {self._inv.number} yang berbeda dengan nomor data pokok, yaitu {self._bm.number}."
            self._result.add_message(msg, 'error', 'force')

        return self
    
    def span_width_check(self):
        """
        The bridge width should match the narrowest span width.
        """
        span_width = self._inv.sups.min_width()
        bridge_width = self._inv.width

        if not isclose(span_width, bridge_width):
            msg = f"Lebar jembatan tidak cocok dengan lebar bentang paling sempit, yaitu {bridge_width}m sedangkan lebar bentang paling sempit adalah {span_width}m"
            self._result.add_message(msg, 'error', 'force')
    
    def put_data(self):
        """
        Write inventory data to database.
        """
        self._repo.put(self._inv)

        return self
    
    def update_master_data(self):
        """
        Update master data in the database.
        """
        # If there is no event then dont update.
        if len(self._bm.get_all_events()) > 0:
            self._bm_repo.retire(self._bm)
            self._bm_repo.insert(self._bm)
            self._bm_repo.append_events(self._bm)
    
    def merge_master_data(self):
        """
        Update master data attribute using the inventory data profile. The attributes currently being updated is.
        1. Length
        2. Location 
        """
        if self._inv.inventory_state == DETAILED_STATE:
            self._bm.length = self._inv.length
            # self._bm.update_coordinate(
            #     lon=self._inv.longitude,
            #     lat=self._inv.latitude
            # )
        elif self._inv.inventory_state == POPUP_STATE:
            self._bm.length = self._inv.length
    
    def invij_json_result(self, as_dict=False):
        """
        Create JSON string containing the validation result.
        """
        if as_dict:
            return self._result.to_invij_format()
        else:
            return json.dumps(self._result.to_invij_format())
