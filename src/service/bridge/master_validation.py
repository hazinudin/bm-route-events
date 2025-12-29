from route_events.bridge import BridgeMaster
from route_events.bridge import BridgeMasterRepoDB, BridgeInventoryRepo, BridgeInventory
from route_events.route import LRSRoute
from pydantic import ValidationError

from ..validation_result.result import ValidationResult
from sqlalchemy import Engine
from numpy import isclose
from typing import Literal
import json


class BridgeMasterValidation(object):
    def __init__(
            self, 
            data: dict, 
            validation_mode: Literal['UPDATE', 'INSERT', 'ARCHIVE'], 
            lrs_grpc_host: str,
            sql_engine: Engine,
            ignore_review: bool = False,
            ignore_force: bool = False
    ):
        """
        Bridge Master data validation object.
        """

        if type(data) != dict:
            raise TypeError("Only accept dictionary as input data.")
               
        self.validation_mode = validation_mode.upper()
        
        # BridgeMasterRepo for pulling comparison data
        # self._repo = BridgeMasterRepo(bridge_grpc_host)
        
        # BridgeMasterRepoDB using GeoDatabase
        self._repo = BridgeMasterRepoDB(
            sql_engine=sql_engine
            )
        
        # Bridge inventory repo
        self._inv_repo = BridgeInventoryRepo(
            sql_engine=sql_engine
        )
        
        # Initial review messages
        review_msgs = []

        # Pydantic validation
        try:
            self._bm = BridgeMaster.from_invij(data)
        except ValidationError as e:
            self._result = ValidationResult('failed_request')
            for error in e.errors():
                if 'review' in error['type']:
                    review_msgs.append(error['msg'])
                elif error['type'] != 'missing':
                    self._result.add_message(error['msg'], 'error')
                else:
                    self._result.add_message(error['msg'], 'rejected')
            
            if self.get_status() in ['rejected', 'error']:
                return
            else:
                self._bm = BridgeMaster.from_invij(data, ignore_review_err=True)

        # LRS object
        try:
            self._lrs = LRSRoute.from_feature_service(lrs_grpc_host, route=self._bm.linkid)
        except:
            print(self._bm.linkid)
            raise
        
        # Comparison data
        self._current_bm = self._repo.get_by_bridge_id(bridge_id=self._bm.id)
        self._inv = None

        # ValidationResult for tracking and storing validation result and status
        _ignored_msg = None

        if ignore_review:
            _ignored_msg = ['review']
        elif ignore_force:
            _ignored_msg = ['force']
        elif ignore_force and ignore_review:
            _ignored_msg = ['review', 'force']
            
        self._result = ValidationResult(self._bm.id, ignore_in=_ignored_msg)

        # Add review message from Pydantic validation.
        for msg in review_msgs:
            self._result.add_message(msg, 'review')

        if (self._current_bm is None) and (self.validation_mode == 'UPDATE'):
            self._result.add_message(f"Jembatan {self._bm.id} tidak tersedia di untuk diupdate.", 'rejected')

        if (self.validation_mode != 'ARCHIVE') and (self._lrs is None):
            self._result.add_message(f'Ruas {self._bm.linkid} bukan merupakan jalan nasional.', 'rejected')

    @property
    def inv(self) -> None | BridgeInventory:
        """
        Get the bridge inventory data with the same bridge ID.
        """
        if self._inv is None:
            inv = self._inv_repo.get_by_bridge_id(self._bm.id, self._bm.master_survey_year)
            self._inv = inv

        return self._inv
    
    def get_all_messages(self):
        """
        Get validation result messages.
        """
        return self._result.get_all_messages()
    
    def get_status(self):
        """
        Get validation process status
        """
        return self._result.status

    def base_data_check(self):
        """
        Execute data check for every request mode.
        """
        self.bridge_num_check()
        self.lrs_distance_check()
        self.bridge_inv_distance_check()
        self.bridge_inv_length_comparison()

        return self
    
    def insert_check(self):
        """
        Execute data check for INSERT request mode.
        """
        self.base_data_check()
        self.bridge_near_check()

        return self
    
    def update_check(self):
        """
        Execute data check for UPDATE request mode.
        """
        self.base_data_check()
        self.bridge_old_distance_check()
        self.compare_bridge_length()

        return self

    def bridge_num_check(self):
        """
        Execute all check related to the bridge number.
        """
        if self.validation_mode == 'INSERT':
            self.bridge_num_already_exists(exclude_self=False)
 
        if self.validation_mode == 'UPDATE':
            self.bridge_num_already_exists(exclude_self=True)
            self.bridge_num_is_different()

        self.bridge_num_format_check()
        self.bridge_num_prov_check()

        return self

    def bridge_num_already_exists(self, exclude_self=True):
        """
        Check if bridge number exists on repository.
        """
        other_bridges_count = self._repo.get_by_bridge_number(self._bm.number, return_count_only=True)

        if exclude_self:
            other_bridges_count = other_bridges_count - 1

        if other_bridges_count > 0:
            msg = f"Nomor jembatan {self._bm.number} sudah terdaftar di GeoDatabase."
            self._result.add_message(msg, status='error', ignore_in='force')

        return self
    
    def bridge_num_is_different(self):
        """
        Check if bridge number is different with bridge number in repository.
        """
        if self._current_bm.number != self._bm.number:
            msg = "Jembatan memiliki nomor yang berbeda dengan data eksisting."
            self._result.add_message(msg, status='review', ignore_in='review')
            
            # Generate bridge number updated event in the other bridge
            self._current_bm.number = self._bm.number
            self._bm.add_events(self._current_bm.get_latest_event())

        return self
    
    def bridge_num_format_check(self):
        """
        Check if the bridge number format is correct.
        """
        if not self._bm.has_correct_num_format:
            msg = f"Nomor jembatan {self._bm.number} merupakan nomor dengan format yang tidak valid."
            self._result.add_message(msg, status='error')
    
        return self
    
    def bridge_num_prov_check(self):
        """
        Check if the bridge number first two digit match the inputted province code.
        """
        if not self._bm.has_correct_prov_in_num:
            msg = f"Nomor jembatan {self._bm.number} memiliki kode provinsi yang berbeda dengan data provinsi yang diinput."
            self._result.add_message(msg, status='error')

        return self
    
    def value_range_domain_check(self):
        """
        Check if inputted value is within valid range.
        """
        columns = self._bm.get_out_of_range_columns()

        for col in columns.keys():
            val = columns[col]['val']
            is_review = columns[col]['is_review']

            msg = f"{col} memiliki nilai yang berada di luar rentang/domain yang valid, yaitu {val}."

            if is_review:
                self._result.add_message(msg, status='review')
            else:
                self._result.add_message(msg, status='error')
        
        return self
    
    def bridge_stat_check(self):
        """
        Compare bridge status with road status where the bridge is attached.
        """
        if self._lrs.status != self._bm.status:
            msg = f"Jembatan memiliki status {self._bm.status} sedangkan ruas tempat jembatan berada memiliki status {self._lrs.status}"
            self._result.add_message(msg, status='error')
        
        return self
    
    def cons_year_check(self):
        """
        Compare the bridge construction year with the input year.
        """
        if self._bm.construction_year > self._bm.master_survey_year:
            msg = f"Tahun bangun jembatan {self._bm.construction_year} sedangkan tahun survey {self._bm.master_survey_year}"
            self._result.add_message(msg, status='error')

        return self
    
    def lrs_distance_check(self, threshold=30):
        """
        Check the bridge location relative to the LRS.
        """
        distance = self._lrs.distance_to_point(
            long=self._bm._point_4326.X, 
            lat=self._bm._point_4326.Y
            )

        if distance > threshold:
            msg = f"Jembatan berjarak {distance}m dari geometri LRS."
            self._result.add_message(msg, status='error', ignore_in='force')

        return self
    
    def compare_bridge_length(self):
        """
        Compare bridge input length with the data from repo.
        """
        if not isclose(self._bm.length, self._current_bm.length):
            msg = f"Jembatan mengalami perubahan panjang dari {self._current_bm.length} ke {self._bm.length}."
            self._result.add_message(msg, status='review', ignore_in='review')

            # Generate the event
            self._current_bm.length = self._bm.length
            self._bm.add_events(self._current_bm.get_latest_event())

        return self
    
    def bridge_old_distance_check(self, threshold=30):
        """
        Compare the bridge location to the existing data in repo (bridge with the same ID).
        """
        distance = self._bm.distance_to(self._current_bm)

        if distance > threshold:
            msg = f"Jembatan berjarak {distance}m dari data jembatan lama."
            self._result.add_message(msg, status='error', ignore_in='force')

        return self
    
    def bridge_inv_distance_check(self, threshold=30):
        """
        Compare the bridge location to the inventory data location.
        """
        if self.inv is None:
            return self
        
        distance = self._bm._point_lambert.distance_to(self.inv._point_lambert)

        if distance > threshold:
            msg = f"Jembatan berjarak {distance}m dari data inventori."
            self._result.add_message(msg, status='error', ignore_in='force')

        return self
    
    def bridge_inv_length_comparison(self):
        """
        Compare bridge length with the inventory data.
        """
        if self.inv is None:
            return self
        
        if not isclose(self._bm.length, self._inv.length):
            msg = f"Jembatan memiliki panjang yang berbeda dengan data inventori, panjang jembatan data ini adalah {self._bm.length}m sedangkan data inventori {self._inv.length}m."
            self._result.add_message(msg, status='review', ignore_in='review')
        
        return self

    def bridge_near_check(self, radius=50):
        """
        Search if there is other bridges within the search radius.
        """
        other_bridges_count = self._repo.get_nearest(self._bm, radius, return_count_only=True)

        if other_bridges_count > 0:
            msg = f"Terdapat {other_bridges_count} jembatan lain yang berjarak kurang dari {radius}m."
            self._result.add_message(msg, status='error', ignore_in='force')

        return self

    def put_data(self):
        """
        Write Bridge Master data through BridgeMasterRepo.
        """
        if self.validation_mode == 'INSERT':
            self._repo.insert(self._bm)

        if self.validation_mode == 'UPDATE':
            if len(self._bm.get_all_events()) > 0:
                self._repo.retire(self._bm)
                self._repo.insert(self._bm)
                self._repo.append_events(self._bm)
            else:
                self._repo.update(self._bm)
        
        elif self.validation_mode == 'ARCHIVE':
            self._repo.retire(self._bm)

        return self
    
    def invij_json_result(self, as_dict=False):
        """
        Create JSON string containing the validation results.
        """
        if as_dict:
            return self._result.to_invij_format()
        else:
            return json.dumps(self._result.to_invij_format())