import unittest
from six import BytesIO

import spavro.io
from spavro.io import FastDatumWriter
# from spavro.io import SlowDatumWriter as FastDatumWriter
from spavro.exceptions import AvroTypeException


valid_data = (
("int_at_the_upper_boundary", 2147483647, '"int"'),
("int_at_the_lower_boundary", -2147483648, '"int"'),
("long_at_the_upper_boundary", 9223372036854775807, '"long"'),
("long_at_the_lower_boundary", -9223372036854775808, '"long"'),
("interger_data_float_schema", 123, '"float"'),
# booleans are considered an integer type? fascinating
("boolean_data_float_schema", True, '"float"'),
("boolean_data_integer_schema", True, '"int"'),
("optional_field", {"value": 100}, '{"fields": [{"type": ["null", "string"], "name": "id"}, {"type": "int", "name": "value"}], "type": "record", "name": "test_schema"}'),
("fixed", b'\x01\x01\x01\x01\x01\x01\x01\x01', '{"name": "testfix", "type": "fixed", "size": 8}'),
("make_sure_null_term_doesnt_break", b'\x01\x01\x00\x01\x01\x01\x01\x01', '{"name": "testfix", "type": "fixed", "size": 8}'),
)

invalid_data = (
("missing_required_field_1", {"value": 100}, '{"fields": [{"type": "string", "name": "id"}, {"type": "int", "name": "value"}], "type": "record", "name": "test_schema"}'),
("missing_required_field_2", {"id": "bork"}, '{"fields": [{"type": "string", "name": "id"}, {"type": "int", "name": "value"}], "type": "record", "name": "test_schema"}'),
("string_data_long_schema", u'boom!', '"long"'),
("string_data_boolean_schema", u"boom!", '"boolean"'),
("int_data_boolean_schema", 123, '"boolean"'),
("float_data_int_schema", 123.456, '"long"'),
("null_data_string_schema", None, '"string"'),
("null_data_int_schema", None, '"int"'),
("null_data_boolean_schema", None, '"boolean"'),
("mismatch_fixed_data_fixed_schema", b'\x97', '{"name": "testfix", "type": "fixed", "size": 8}'),
("int_too_big", 2147483648, '"int"'),
("int_too_small", -2147483649, '"int"'),
("long_too_big", 9223372036854775808, '"long"'),
("long_too_small", -9223372036854775809, '"long"'),
("wrong_data_in_array", [1, u'B'], '{"type": "array", "items": "int"}'),
)


class TestValidData(unittest.TestCase):
    pass


def create_good_case(schema, datum):
    write_schema = spavro.schema.parse(schema)
    def test_write_good_data(self):
        fastbuff = BytesIO()
        fastencoder = spavro.io.FastBinaryEncoder(fastbuff)
        fdw = FastDatumWriter(write_schema)
        fdw.write(datum, fastencoder)
    return test_write_good_data


def create_exception_case(schema, datum):
    print(schema)
    write_schema = spavro.schema.parse(schema)
    def test_write_invalid_data(self):
        with self.assertRaises(AvroTypeException) as context:
            fastbuff = BytesIO()
            fastencoder = spavro.io.FastBinaryEncoder(fastbuff)
            fdw = FastDatumWriter(write_schema)
            fdw.write(datum, fastencoder)
        print(context.exception)
    return test_write_invalid_data


def make_good_cases(cases):
    for name, datum, schema in cases:
        test_method = create_good_case(schema, datum)
        test_method.__name__ = 'test_good_data_{}'.format(name)
        setattr(TestValidData, test_method.__name__, test_method)


def make_exception_cases(cases):
    for name, datum, schema in cases:
        test_method = create_exception_case(schema, datum)
        test_method.__name__ = 'test_invalid_data_{}'.format(name)
        setattr(TestValidData, test_method.__name__, test_method)


make_good_cases(valid_data)
make_exception_cases(invalid_data)
