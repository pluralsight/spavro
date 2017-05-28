import unittest
from spavro import io
try:
  from cStringIO import StringIO
except ImportError:
  from StringIO import StringIO


record1 = {"type": "record", "name": "R1", "fields": [{"name": "A", "type": "int"}, {"name": "B", "type": "string"}]}
record2 = {"type": "record", "name": "R2", "fields": [{"name": "C", "type": "string"}, {"name": "D", "type": "string"}]}

simple_union = '''['int', 'string', {0}]'''.format(record1)
# two records
complex_union = '''['float', 'null', {0}, {1}]'''.format(record1, record2)


def write_datum(schema, datum):
    writer = StringIO()
    encoder = io.BinaryEncoder(writer)
    datum_writer = io.DatumWriter(schema)
    datum_writer.write(datum, encoder)
    return writer, encoder, datum_writer


class TestFastWrite(unittest.TestCase):
    def test_simple_union(self):
        pass

    def test_complex_union(self):
        pass

    def test_string_and_fixed(self):
        schema = ['string', {"type": "fixed", "size": 256}]
        pass

    def test_fixed_and_enum_and_string(self):
        schema = ['string', {"type": "fixed", "size": 256}, {"type": "enum", "symbols": ["TEST", "NOTEST"]}]

