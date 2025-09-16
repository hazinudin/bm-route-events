import pyarrow as pa
import json
from pydantic import (
    create_model, 
    Field, 
    field_validator, 
    ValidatorFunctionWrapHandler, 
    AliasChoices, 
    ValidationInfo, 
    model_validator, 
    ModelWrapValidatorHandler,
    StringConstraints
    )
from pydantic_core import (
    PydanticCustomError, 
    ValidationError, 
    InitErrorDetails
    )
from typing import Optional, Literal, Annotated
from datetime import datetime as dt
from polars import String, Int64, Float64
from enum import IntEnum
import re


CUSTOM_ERROR_MSG = {
    "less_than": "Nilai {0}={1} berada di luar rentang valid.",
    "less_than_equal": "Nilai {0}={1} berada di luar rentang valid.",
    "greater_than_equal": "Nilai {0}={1} berada di luar rentang valid.",
    "greater_than": "Nilai {0}={1} berada di luar rentang valid.",
    "literal_error": "Nilai {0}={1} tidak termasuk di dalam domain valid.",
    "int_parsing": "Nilai {0}={1} bukan merupakan nilai numerik.",
    "int_type": "Nilai {0} harus diisi.",
    "float_parsing": "Nilai {0}={1} bukan merupakan nilai numerik.",
    "float_type": "Nilai {0} harus diisi.",
    "string_type": "Nilai {0} harus diisi.",
    "string_too_long": "{0} memiliki isi dengan jumlah karakter melebihi 255"
}

def truncate_str(v:str):
    """
    Truncate string to match the 255 limitation.
    """
    return v[:255]

def serialize_date_str(v:any):
    """
    Serialize date string to timestamp float. Before validator.
    """
    try:
        dttime = dt.strptime(v, '%d/%m/%Y')  # This probably raise an error.
        return dttime  # This one should not raise an error.
    except:
        raise PydanticCustomError(
            'datetime_parsing', 
            f'Tanggal {v} tidak sesuai dengan format dd/mm/yyyy.', 
            dict(input=v)
            )
    
def serialize_float_to_int(v:any):
    """
    Serialize string float into integer. Example 5.002 to 5, this will raise an error if int(5.002)
    """
    try:
        if v is None:
            return v
        else:
            val_ = round(float(v))
            return val_
        
    except ValueError:
        raise PydanticCustomError(
            'int_parsing',
            'Input is neither a float or integer, unable serialize.',
            dict(input_value=v)
        )

def generate_custom_review_msg(v: any, handler: ValidatorFunctionWrapHandler, info: ValidationInfo):
    """
    Generate review message based on its range.
    """
    try:
        return handler(v)
    except ValidationError as e:
        error = e.errors()[0]

        if error['type'] in [
            'greater_than',
            'greater_than_equal',
            'less_than',
            'less_than_equal'
        ]:
            raise PydanticCustomError(
                error['type'] + '_review', # Add review suffix
                CUSTOM_ERROR_MSG[error['type']].format(info.field_name, v), 
                dict(input=error['input'])
                )
        else:
            raise e
        
def bypass_review_error(v: any, handler:ValidatorFunctionWrapHandler):
    """
    Bypass handler for review columns.
    """
    try:
        return handler(v)
    except ValidationError:
        return v
        
def generate_missing_custom_msg(v: any, handler: ModelWrapValidatorHandler):
    """
    Generate custom error message for model validator.
    Error loc could be like this (BANGUNAN_ATAS, 0, BANGUNAN_BAWAH, 0, LONGITUDE), only use the last position for error message.
    """
    errors = []
    input_val_re = "(?:,|{| )'input(?:_value)?': (\w+|.\w+.|\d*\.*\d*|.\d*\.*\d*.)(?:,|}| )"

    try:
        return handler(v)
    except ValidationError as e:
        for error in e.errors():
            err_type = error['type']

            if err_type in CUSTOM_ERROR_MSG:
                re_val = re.findall(input_val_re, str(error))

                if len(re_val) != 0:
                    error_val = re_val[0]
                else:
                    error_val = error['input']
                
                errors.append(
                    InitErrorDetails(
                        type = PydanticCustomError(
                                err_type,
                                CUSTOM_ERROR_MSG[err_type].format(error['loc'][-1], error_val),
                            ),
                        loc=error['loc'],
                        input=dict(input=error['input'])
                    )
                )

            elif err_type == 'missing':
                errors.append(
                    InitErrorDetails(
                        type = PydanticCustomError(
                                err_type,
                                f"Data input tidak memiliki {error['loc'][-1]}"
                            ),
                        loc=error['loc'],
                        input=dict(input=error['input'])
                        )
                    )
            
            else:
                errors.append(
                    InitErrorDetails(
                    type = PydanticCustomError(error['type'], error['msg']),
                    loc = error['loc'],
                    input=dict(input=error['input'])
                    )
                )

        raise ValidationError.from_exception_data(title='validation_error', line_errors=errors)

class RouteEventsSchema(object):
    """
    Generate Pyarrow Schema and Pydantic Model from schema JSON configuration file.
    """
    def __init__(self, file_path, ignore_review_err=False):
        # Load the config JSON
        with open(file_path) as jf:
            schema_dict = json.load(jf)['column_details']

        self.schema_dict = schema_dict
        self.input_schema = pa.schema([])
        self.metadata_keys = b'details'  # Keys used to access Field metadata
        self.translate_mapping = dict()  # Dictionary for mapping column name from input to database column.
        self.db_upper_to_lower_case = dict()  # Dictionary for uppercase to lowercase mapping.
        self.date_cols = list()  # Columns which are date type
        self.db_date_cols = list()  # Database columns which are date type
        self.strptime_format = "%d/%m/%Y"
        self.review_fields = []  # Fields with range review option
        
        # Pydantic BaseModel kwargs
        self.model_kwargs = dict()

        # Polars schema
        self.pl_schema = dict()

        # Pydantic field validators
        self.validators = dict()
        self.validators['model_validator_'] = model_validator(mode='wrap')(generate_missing_custom_msg)

        for col in schema_dict:
            if schema_dict[col].get('skip'):  # If "skip" is True
                continue

            dtype = schema_dict[col]['dtype']  # Mandatory
            range = schema_dict[col].get('range')
            domain = schema_dict[col].get('domain')
            db_col = schema_dict[col].get('db_col')
            allow_null = schema_dict[col].get('allow_null')
            upper_case = schema_dict[col].get('uppercase')  # Only for string column, default is True

            if upper_case is None:
                upper_case = True

            if allow_null is None:
                allow_null = False  # Set default value to False

            if db_col is None:
                db_col = col  # Set default value of db col same as the input column

            # Input field metadata
            # Containing: value range, domain, strptime and database column mapping.
            input_metadata = {"range": range, 
                              "domain": domain, 
                              "translate_name": db_col, 
                              "strptime": None}
            
            # Pydantic and Patito Field object kwargs
            # Accepts uppercase and lowercase alias
            field_kwargs = {
                "validation_alias": AliasChoices(col, db_col, col.lower(), col.upper()), 
                "alias": db_col
            }
            
            # Pydantic valid range
            if range is not None:
                if range['eq_upper']:
                    field_kwargs['le'] = range['upper']
                else:
                    field_kwargs['lt'] = range['upper']
                
                if range['eq_lower']:
                    field_kwargs['ge'] = range['lower']
                else:
                    field_kwargs['gt'] = range['lower']

                # Review range.
                if range.get('review'):
                    field_kwargs['description'] = {"review": True}
                    self.review_fields.append(col)
                    
                    # If review is ignored then validation handler is bypassed.
                    if ignore_review_err:
                        self.validators[bypass_review_error.__name__+'_'+col] = field_validator(col, mode='wrap')(bypass_review_error)
                    else:
                        self.validators[serialize_date_str.__name__+'_'+col] = field_validator(col, mode='wrap')(generate_custom_review_msg)
                
            self.translate_mapping[col] = db_col
            self.db_upper_to_lower_case[db_col] = db_col.lower()
            
            if dtype == 'string':
                pa_type = pa.string()  # Pyarrow string
                pyd_type = Annotated[str, StringConstraints(to_upper=upper_case)]  # Pydantic type
                self.validators[truncate_str.__name__+'_'+col] = field_validator(col, mode='before')(truncate_str)
                field_kwargs['coerce_number_to_str'] = True
                field_kwargs['max_length'] = 255
                self.pl_schema[col] = String
            elif dtype == 'double':
                pa_type = pa.float64()
                pyd_type = float
                self.pl_schema[col] = Float64
            elif dtype == 'integer':
                pa_type = pa.int64()
                pyd_type = int
                self.pl_schema[col] = Int64

                # Add string serializer
                self.validators[serialize_float_to_int.__name__+'_'+col] = field_validator(col, mode='before')(serialize_float_to_int)
            elif dtype == 'date':  # Date is inputted as string
                pa_type = pa.string()
                pyd_type = dt  # Type is datetime
                self.pl_schema[col] = String

                input_metadata['strptime'] = self.strptime_format  # Default time format
                self.date_cols.append(col)
                self.db_date_cols.append(db_col)

                # Pydantic date field validator kwargs
                self.validators[serialize_date_str.__name__+'_'+col] = field_validator(col, mode='before')(serialize_date_str)
            else:
                continue

            # Set Literal for Pydantic data type
            if domain is not None:
                if dtype == 'integer':
                    _enum = IntEnum('_enum', {f'_{x}': x for x in domain})
                elif dtype == 'string':
                    pyd_type = Literal[tuple([str(_).lower() for _ in domain] + [str(_).upper() for _ in domain])]
                    pyd_type = Annotated[pyd_type, StringConstraints(to_upper=True)]
                else:
                    pyd_type = Literal[tuple(domain)]

            # Set Optional for Pydantic data type
            if allow_null:
                pyd_type = Optional[pyd_type]

            # Set all column to nullable=False
            input_field = pa.field(col, pa_type, 
                                   metadata={"details": json.dumps(input_metadata)},
                                   nullable=allow_null)

            # Create Pydantic create_model kwargs
            self.model_kwargs[col] = (
                pyd_type,
                Field(**field_kwargs)
            )
            
            # Append the Pyarrow Schema
            self.input_schema = self.input_schema.append(input_field)

        # Create Pydantic model
        self.model = create_model('validation', __validators__=self.validators, **self.model_kwargs)
