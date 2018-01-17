import unittest
from six import BytesIO

import spavro.io
from spavro.io import FastDatumWriter
# from spavro.io import SlowDatumWriter as FastDatumWriter
# from spavro.exceptions import AvroTypeException, SchemaParseException
from spavro.schema import SchemaParseException, AvroException


from spavro import schema

invalid_schemas = (
    ("true_is_not_a_schema", '"True"'),
    ("unquoted_true_is_not_a_schema", 'True'),
    ("missing_type", '{"no_type": "test"}'),
    ("invalid_type", '{"type": "panther"}'),
    ("missing_required_size_in_fixed", '''{"type": "fixed",
         "name": "Missing size"}'''),
    ("missing_required_name_in_fixed", '''{"type": "fixed",
         "size": 314}'''),
    ("enum_symbols_not_an_array", '''{"type": "enum",
         "name": "Status",
         "symbols": "Normal Caution Critical"}''')
)


class TestSchemaParsing(unittest.TestCase):
    pass


# def create_good_case(schema, datum):
#     write_schema = spavro.schema.parse(schema)
#     def test_write_good_data(self):
#         fastbuff = BytesIO()
#         fastencoder = spavro.io.FastBinaryEncoder(fastbuff)
#         fdw = FastDatumWriter(write_schema)
#         fdw.write(datum, fastencoder)
#     return test_write_good_data


def create_exception_case(local_schema):
    # print(schema)
    def test_parse_invalid_schema(self):
        with self.assertRaises((SchemaParseException, AvroException)) as context:
            spavro.schema.parse(local_schema)
    return test_parse_invalid_schema


# def make_good_cases(cases):
#     for name, datum, schema in cases:
#         test_method = create_good_case(schema, datum)
#         test_method.__name__ = 'test_good_data_{}'.format(name)
#         setattr(TestValidData, test_method.__name__, test_method)


def make_exception_cases(cases):
    for name, local_schema in cases:
        test_method = create_exception_case(local_schema)
        test_method.__name__ = 'test_invalid_schema_{}'.format(name)
        setattr(TestSchemaParsing, test_method.__name__, test_method)


# make_good_cases(valid_data)
make_exception_cases(invalid_schemas)
