# Copyright (C) 2017 Pluralsight LLC

import unittest
import six
from six import BytesIO as StringIO
# try:
#   from cStringIO import StringIO
# except ImportError:
#   from StringIO import StringIO
try:
    type(unicode)
except NameError:
    unicode = str

import json
import spavro.schema
import spavro.io
# from spavro.fast_binary import make_union_writer, get_writer
from spavro.io import FastDatumWriter, SlowDatumWriter


class TestOldVsNew(unittest.TestCase):
    def setUp(self):
        self.fdw = FastDatumWriter()
        self.sdw = SlowDatumWriter()

cases = (
("null", "null", None),
("int", "int", 1234),
("long", "long", 1234567890),
("float", "float", 1234.123),
("double", "double", 1234.12345),
("boolean", "boolean", True),
("string", "string", unicode('adsfasdf09809dsf-=adsf')),
("a_fixed_record", {"type": "fixed", "size": 4, "name": "fixeddata"}, b'Obj\x01'),
("an_enum_record", {"type": "enum", "symbols": ["A", "B", "C"], "name": "enumdata"}, "A"),
("an_array", {"type": "array", "items": "int"}, [1, 2, 3, 1234, 4321]),
("a_map", {"type": "map", "values": "int"}, {"L1": 1, "L2": 2, "L3": 3, "L4": 4}),
("union_test_string", ["null", "int", "string"], unicode("whassssuuuup")),
("union_test_int", ["null", "int", "string"], 1234),
("union_test_null", ["null", "int", "string"], None),
("a_record",
    {"type": "record", "name": "recorddata", "fields": [{"name": "field1", "type": "long"},
                                                        {"name": "field2", "type": "string"}]},
                                                        {"field1": 1234, "field2": unicode("whassssuuuuup")}),
("recursive_defined_record",
{u'fields': [{u'type': [u'null', u'string', {u'fields': [{u'type': u'Lisp', u'name': u'car'},
                                                         {u'type': u'Lisp', u'name': u'cdr'}],
                        u'type': u'record', u'name': u'Cons'}],
             u'name': u'value'}],
 u'type': u'record',
 u'name': u'Lisp'},
{'value': {'car': {'value': 'head'}, 'cdr': {'value': None}}}),
("union_of_two_records_recordA",
    [{"type": "record", "name": "recorddata", "fields": [{"name": "fieldA1", "type": "long"},
                                                        {"name": "fieldA2", "type": "string"}]},
     {"type": "record", "name": "recorddata2", "fields": [{"name": "fieldB1", "type": {"type": "fixed", "name": "fixedbytes", "size": 4}},
                                                           {"name": "fieldB2", "type": "string"}]}],
 {"fieldA1": 1234, "fieldA2": unicode("whassssuuuuup")}),
("union_of_two_records_recordB",
    [{"type": "record", "name": "recorddata", "fields": [{"name": "fieldA1", "type": "long"},
                                                        {"name": "fieldA2", "type": "string"}]},
     {"type": "record", "name": "recorddata2", "fields": [{"name": "fieldB1", "type": {"type": "fixed", "name": "fixedbytes", "size": 4}},
                                                           {"name": "fieldB2", "type": "string"}]}],
 {"fieldB1": b'\x01\x02\x03\x04', "fieldB2": unicode("Nother Record")})
)


def create_case(schema, datum):
    def compare_old_and_new(self):
        fastbuff = StringIO()
        slowbuff = StringIO()
        fastencoder = spavro.io.FastBinaryEncoder(fastbuff)
        slowencoder = spavro.io.SlowBinaryEncoder(slowbuff)
        write_schema = spavro.schema.parse(json.dumps(schema))
        for i in range(10):
            self.fdw.write_data(write_schema, datum, fastencoder)
            self.sdw.write_data(write_schema, datum, slowencoder)
        self.assertEqual(fastbuff.getvalue(), slowbuff.getvalue())
    return compare_old_and_new


def make_cases():
    for name, schema, datum in cases:
        test_method = create_case(schema, datum)
        test_method.__name__ = 'test_write_old_vs_new_{}'.format(name)
        setattr(TestOldVsNew, test_method.__name__, test_method)

make_cases()
