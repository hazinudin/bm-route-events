from .msg import ValidationMessages
import polars as pl
import pyarrow as pa
from typing import Literal, List, Union
from pydantic import BaseModel
import base64


class ValidationResult(object):
    def __init__(self, id: str, ignore_in: list = None):
        """
        Object for tracking and storing the validation result.
        """
        self._state = ['rejected', 'error', 'review', 'verified']

        self._msg = ValidationMessages(
            id=id, 
            allowed_status = self._state[:-1],
            allowed_ignored_status = ['review', 'force']
        )
        
        self._ignore_in = ignore_in

    @classmethod
    def from_validation_process(cls, id: str):
        """
        Create ValidationResult from a new validation process.
        """
        return cls(bridge_id=id)
    
    def get_all_messages(self)->pl.DataFrame:
        """
        Return all messages in Polars DataFrame.
        """
        return self._msg.df
    
    def get_filtered_msg(self)->pl.DataFrame:
        """
        Retual all messages that is not ignored.
        """
        if self._ignore_in is None:
            return self.get_all_messages()
        else:
            return self._msg.filter(self._ignore_in)

    def add_message(
            self, 
            msg: str, 
            status: Literal['rejected', 'error', 'review'], 
            ignore_in: Literal['force', 'review'] = None
        ):
        """
        Add message to ValidationMessage
        """
        self._msg.add_message(msg, status, ignore_in)

        return self

    def add_messages(
            self,  
            df: pl.DataFrame, 
            status: Literal['rejected', 'error', 'review'], 
            ignore_in: Literal['review', 'force'] = None
        ):
        """
        Concat a DataFrame containing messages.
        """
        self._msg.add_messages(df, status, ignore_in)

        return self

    @property
    def status(self):
        """
        Validation result status.
        """
        if self.get_filtered_msg().is_empty():
            return self._state[-1]
        else:
            return self._state[
                self.get_filtered_msg().
                sort('status_idx')['status_idx'].min()
            ]
        
    def to_smd_format(
            self, 
            show_all_msg: bool = False,
            as_dict: bool = True,
            ignore_force: bool = False
        ) -> Union[dict | str]:
        """
        Serialize error messages to SMD dictionary format. Output status is either "Rejected" or "Succeeded".
        
        Example:
        {
            "status": "Succeeded",
            "messages": [
                {"linkid": "01001", "status": "error_sanggah", "msg": "Koordinat melenceng."},
                {"linkid": "01001", "status": "error_sanggah", "msg": "STA melenceng."}
            ]
        }

        OR 

        {
            "status": "Rejected",
            "messages": ["Rejected message"]
        }        
        """
        if show_all_msg:
            df = self.get_all_messages()
        else:
            df = self.get_filtered_msg().filter(
                pl.col('status') == self.status
            )

        class SMDMessages(BaseModel):
            linkid: str
            status: str
            msg: str

        class SMDOutput(BaseModel):
            status: Literal['Succeeded', 'Rejected']
            messages: Union[List | List[SMDMessages]] = []
        
        if self.status == 'rejected':
            out_obj = SMDOutput(status="Rejected")
        else:
            out_obj = SMDOutput(status="Succeeded")

        # Translate to SMD status
        # 'error' with ignore_in = 'force' will be 'error_sanggah'
        df = df.with_columns(
            status=pl.when(
                pl.col('status').eq('error') & 
                pl.col('ignore_in').eq('force')
            ).then(
                pl.lit('error_sanggah')
            ).when(
                pl.col('status').eq('review')
            ).then(
                pl.lit('ToBeReviewed')
            ).otherwise(
                pl.col('status')
            )
        ).select(
            pl.col('id').alias('linkid'),
            pl.col('status'),
            pl.col('msg')
        )

        if self.status == 'rejected':
            # The .unique prevents duplicated message to be returned
            out_obj.messages.extend(df.unique(subset=['linkid', 'msg'])['msg'].to_list())
        else:
            out_obj.messages.extend(df.to_dicts())

        if as_dict:
            return out_obj.model_dump()
        else:
            return out_obj.model_dump_json()
    
    def to_invij_format(self) -> dict:
        """
        Serialize error message to INVI-J dictionary format.
        """
        msgs = self.get_filtered_msg().select(
            ['status', 'msg']
            ).unique(
                subset=['status', 'msg']  # Prevents duplicated message to be returned
            ).group_by(
                'status'
            ).all().rows_by_key(
                'status', named=False
            )
        
        if self.status == 'rejected':
            general_status = 'failed'
            general_errors = msgs[self.status][0][0]
            status = 'unverified'
        else:
            general_status = 'verified'
            general_errors = []

            if self.status == 'error':
                status = 'failed'
            else:
                status = self.status

        out_dict = {
            "general":{
                "status": general_status,
                "error": general_errors
            },
            "status": status,
            "error": [],
            "review": []
        }

        if status not in ['verified', 'unverified']:
            out_dict[self.status] = msgs[self.status][0][0]

        return out_dict
    
    def to_arrow_base64(self) -> str:
        """
        Return Apache Arrow batches as bytes encoded in base64 encoding.
        """

        sink = pa.BufferOutputStream()
        schema = self.get_all_messages().to_arrow().schema

        with pa.ipc.new_stream(sink, schema) as writer:
            for batch in self.get_all_messages().to_arrow().to_batches():
                writer.write_batch(batch)

        return base64.b64encode(sink.getvalue().to_pybytes()).decode('utf-8')
