# Copyright (C) 2018 Pluralsight LLC

import unittest
from six import BytesIO

import spavro.io
from spavro.io import FastDatumWriter
# from spavro.io import SlowDatumWriter as FastDatumWriter
# from spavro.exceptions import AvroTypeException


simple_write_cases = (
    ("float_and_null", 3.14159, '["null", "float"]', b"\x02\xd0\x0fI@"),
    ("float_and_double", 3.14159, '["null", "double"]', b"\x02n\x86\x1b\xf0\xf9!\t@"),
    ("float_and_null_with_int", 150, '["null", "float"]', b'\x02\x00\x00\x16C'),
    ("promote_int_to_float", 314159, '["null", "float"]', b"\x02\xe0e\x99H"),
    ("dont_promte_int_to_float", 314159, '["null", "float", "int"]', b"\x04\xde\xac&"),
    ("promote_int_to_double", 314159, '["null", "double"]', b"\x02\x00\x00\x00\x00\xbc,\x13A"),
    ("dont_promte_int_to_double", 314159, '["null", "double", "int"]', b"\x04\xde\xac&"),
    ("promote_string_to_bytes", u"testing123", '["null", "bytes"]', b"\x02\x14testing123"),
    ("dont_promote_string_to_bytes", u"testing123", '["null", "bytes", "string"]', b"\x04\x14testing123")
)


class TestUnionWriter(unittest.TestCase):
    pass


def create_write_case(schema, datum, expected):
    write_schema = spavro.schema.parse(schema)

    def test_write_good_data(self):
        fastbuff = BytesIO()
        fastencoder = spavro.io.FastBinaryEncoder(fastbuff)
        fdw = FastDatumWriter(write_schema)
        fdw.write(datum, fastencoder)
        self.assertEqual(fastbuff.getvalue(), expected)
    return test_write_good_data


def make_write_cases(cases):
    for name, datum, schema, expected in cases:
        test_method = create_write_case(schema, datum, expected)
        test_method.__name__ = 'test_simple_union_write_{}'.format(name)
        setattr(TestUnionWriter, test_method.__name__, test_method)


make_write_cases(simple_write_cases)
