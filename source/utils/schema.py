from datetime import date, datetime
from decimal import Decimal

from dateutil.parser import parse as datetime_parser
from messytables import types as mt_types

MESSYTABLES_TYPES_TO_FIELD_TYPE = {
    mt_types.StringType: 'string',
    mt_types.BoolType: 'boolean',
    mt_types.DateType: 'datetime',
    mt_types.DateUtilType: 'datetime',
    mt_types.DecimalType: 'decimal',
    mt_types.FloatType: 'float',
    mt_types.IntegerType: 'integer',

}


FIELD_TYPE_NA_VALUES = {
    'string': '______(NULL_ENDOR_NULL)______',
    'integer': 1234567890,
    'long': 1234567890,
    'decimal': 1234567890.0,
    'float': 1234567890.0,
    'double': 1234567890.0,
    'date': date(1970, 1, 2),
    'datetime': datetime(1970, 1, 2),
}


STR_TO_PYTHON_CONVERTERS = {
    'string': lambda x: '' if x is None else str(x),
    'integer': lambda x: int(x),
    'long': lambda x: long(x),
    'decimal': lambda x: Decimal(x),
    'float': lambda x: float(x),
    'double': lambda x: float(x),
    'date': lambda x: datetime_parser(x),
    'datetime': lambda x: datetime_parser(x),
    'boolean': lambda x: bool(x)
}
