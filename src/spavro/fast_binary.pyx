# from libc.stdio cimport printf

import sys
import struct
from binascii import crc32
from spavro import schema

# TODO(hammer): shouldn't ! be < for little-endian (according to spec?)
if sys.version_info >= (2, 5, 0):
    struct_class = struct.Struct
else:
    class SimpleStruct(object):
        def __init__(self, format):
            self.format = format

        def pack(self, *args):
            return struct.pack(self.format, *args)

        def unpack(self, *args):
            return struct.unpack(self.format, *args)
    struct_class = SimpleStruct

STRUCT_INT = struct_class('!I')         # big-endian unsigned int
STRUCT_LONG = struct_class('!Q')        # big-endian unsigned long long
STRUCT_FLOAT = struct_class('!f')     # big-endian float
STRUCT_DOUBLE = struct_class('!d')    # big-endian double
STRUCT_CRC32 = struct_class('>I')     # big-endian unsigned int


def read_long(fo):
    '''Read a long using zig-zag binary encoding'''
    cdef:
        unsigned long long accum
        int temp_datum
        char* c_raw
        long long result
        int shift
    raw = fo.read(1)
    c_raw = raw
    temp_datum = <int>c_raw[0]
    accum = temp_datum & 0x7F
    shift = 7
    while (temp_datum & 0x80) != 0:
        raw = fo.read(1)
        c_raw = raw
        temp_datum = <int>c_raw[0]
        accum |= (temp_datum & 0x7F) << shift
        shift += 7
    result = (accum >> 1) ^ -(accum & 1)
    return result

def read_bytes(fo):
    '''Bytes are a marker for length of bytes and then binary data'''
    return fo.read(read_long(fo))


def read_null(fo):
    """
    null is written as zero bytes
    """
    return None


def read_boolean(fo):
    """
    a boolean is written as a single byte 
    whose value is either 0 (false) or 1 (true).
    """
    return fo.read(1) == b'\x01'

def read_float(fo):
    """
    A float is written as 4 bytes.
    The float is converted into a 32-bit integer using a method equivalent to
    Java's floatToIntBits and then encoded in little-endian format.
    """
    data = fo.read(4)
    cdef char* y = data
    return (<float*>y)[0]

def read_double(fo):
    """
    A double is written as 8 bytes.
    The double is converted into a 64-bit integer using a method equivalent to
    Java's doubleToLongBits and then encoded in little-endian format.
    """
    data = fo.read(8)
    cdef char* y = data
    return (<double*>y)[0]

def read_utf8(fo):
    """
    A string is encoded as a long followed by
    that many bytes of UTF-8 encoded character data.
    """
    return unicode(read_bytes(fo), "utf-8")

# ======================================================================
from collections import namedtuple
Field = namedtuple('Field', ['name', 'reader'])

cdef unicode get_type(schema):
    if isinstance(schema, list):
        return u"union"
    elif isinstance(schema, dict):
        return unicode(schema['type'])  # "record"
    else:
        return unicode(schema)


def make_union_reader(union_schema):
    cdef list readers = [get_reader(schema) for schema in union_schema]

    def union_reader(fo):
        '''Read the long index for which schema to process, then use that'''
        union_index = read_long(fo)
        return readers[union_index](fo)

    return union_reader


def make_record_reader(schema):
    cdef list fields = [Field(field['name'], get_reader(field['type'])) for field in schema['fields']]

    def record_reader(fo):
        return {field.name: field.reader(fo) for field in fields}
    return record_reader


def make_enum_reader(schema):
    cdef list symbols = schema['symbols']

    def enum_reader(fo):
        return symbols[read_long(fo)]
    return enum_reader

def make_array_reader(schema):
    item_reader = get_reader(schema['items'])
    def array_reader(fo):
        cdef long block_count
        cdef list read_items = []
        block_count = read_long(fo)
        while block_count != 0:
            if block_count < 0:
                block_count = -block_count
                block_size = read_long(fo)
            for i in range(block_count):
                read_items.append(item_reader(fo))
            block_count = read_long(fo)
        return read_items
    return array_reader

def make_map_reader(schema):
    value_reader = get_reader(schema['values'])

    def map_reader(fo):
        cdef long block_count = read_long(fo)
        cdef dict read_items = {}
        while block_count != 0:
            if block_count < 0:
                block_count = -block_count
                block_size = read_long(fo)
            for i in range(block_count):
                key = read_utf8(fo)
                read_items[key] = value_reader(fo)
            block_count = read_long(fo)
        return read_items
    return map_reader

def make_fixed_reader(schema):
    cdef long size = schema['size']

    def fixed_reader(fo):
        return fo.read(size)
    return fixed_reader

def make_null_reader(schema):
    return read_null

def make_string_reader(schema):
    return read_utf8

def make_boolean_reader(schema):
    return read_boolean

def make_double_reader(schema):
    return read_double

def make_long_reader(schema):
    return read_long

def make_byte_reader(schema):
    return read_bytes

def make_float_reader(schema):
    return read_float

type_map = {
    'union': make_union_reader,
    'record': make_record_reader,
    'null': make_null_reader,
    'string': make_string_reader,
    'boolean': make_boolean_reader,
    'double': make_double_reader,
    'float': make_float_reader,
    'long': make_long_reader,
    'bytes': make_byte_reader,
    'int': make_long_reader,
    'fixed': make_fixed_reader,
    'enum': make_enum_reader,
    'array': make_array_reader,
    'map': make_map_reader
}

schema_cache = {}

class Placeholder(object):
    def __init__(self):
        self.reader = None

    def __call__(self, fo):
        self.reader(fo)

def get_reader(schema):
    cdef unicode schema_type = get_type(schema)
    if schema_type == u'record':
        placeholder = Placeholder()
        # using a placeholder because this is recursive and the reader isn't defined
        # yet and nested records might refer to this parent schema name
        schema_cache[schema['name']] = placeholder
        reader = type_map[schema_type](schema)
        # now that we've returned, assign the reader to the placeholder
        # so that the execution will work
        placeholder.reader = reader
    else:
        try:
            reader = type_map[schema_type](schema)
        except KeyError:
            reader = schema_cache[schema_type]

    return reader

# ======================================================================


class BinaryDecoder(object):
    """Read leaf values."""
    def __init__(self, reader):
        """
        reader is a Python object on which we can call read, seek, and tell.
        """
        self.reader = reader

    # # read-only properties
    # reader = property(lambda self: self._reader)

    def read(self, n):
        """
        Read n bytes.
        """
        return self.reader.read(n)

    def read_null(self):
        """
        null is written as zero bytes
        """
        return None

    def read_boolean(self):
        """
        a boolean is written as a single byte 
        whose value is either 0 (false) or 1 (true).
        """
        return self.reader.read(1) == b'\x01'

    def read_long(self):
        """
        int and long values are written using variable-length, zig-zag coding.
        """
        cdef:
            unsigned long long accum
            int temp_datum
            char* c_raw
            long long result
            int shift
        raw = self.reader.read(1)
        c_raw = raw
        temp_datum = <int>c_raw[0]
        accum = temp_datum & 0x7F
        shift = 7
        while (temp_datum & 0x80) != 0:
            raw = self.reader.read(1)
            c_raw = raw
            temp_datum = <int>c_raw[0]
            accum |= (temp_datum & 0x7F) << shift
            shift += 7
        result = (accum >> 1) ^ -(accum & 1)
        return result

    read_int = read_long

    def read_float(self):
        """
        A float is written as 4 bytes.
        The float is converted into a 32-bit integer using a method equivalent to
        Java's floatToIntBits and then encoded in little-endian format.
        """
        data = self.reader.read(4)
        cdef char* y = data
        return (<float*>y)[0]

    def read_double(self):
        """
        A double is written as 8 bytes.
        The double is converted into a 64-bit integer using a method equivalent to
        Java's doubleToLongBits and then encoded in little-endian format.
        """
        data = self.reader.read(8)
        cdef char* y = data
        return (<double*>y)[0]


    def read_bytes(self):
        """
        Bytes are encoded as a long followed by that many bytes of data. 
        """
        return self.reader.read(self.read_long())

    def read_utf8(self):
        """
        A string is encoded as a long followed by
        that many bytes of UTF-8 encoded character data.
        """
        return unicode(self.read_bytes(), "utf-8")

    def check_crc32(self, bytes):
        checksum = STRUCT_CRC32.unpack(self.read(4))[0];
        if crc32(bytes) & 0xffffffff != checksum:
            raise schema.AvroException("Checksum failure")

    def skip_null(self):
        pass

    def skip_boolean(self):
        self.skip(1)

    def skip_int(self):
        self.skip_long()

    def skip_long(self):
        b = ord(self.read(1))
        while (b & 0x80) != 0:
            b = ord(self.read(1))

    def skip_float(self):
        self.skip(4)

    def skip_double(self):
        self.skip(8)

    def skip_bytes(self):
        self.skip(self.read_long())

    def skip_utf8(self):
        self.skip_bytes()

    def skip(self, n):
        self.reader.seek(self.reader.tell() + n)


class BinaryEncoder(object):
    """Write leaf values."""
    def __init__(self, writer):
        """
        writer is a Python object on which we can call write.
        """
        self.writer = writer

    # read-only properties
    # writer = property(lambda self: self._writer)

    def write(self, datum):
        """Write an abritrary datum."""
        self.writer.write(datum)

    def write_null(self, datum):
        """
        null is written as zero bytes
        """
        pass

    def write_boolean(self, char datum):
        """
        a boolean is written as a single byte 
        whose value is either 0 (false) or 1 (true).
        """
        cdef char x = 1 if datum else 0
        self.writer.write((<char *>&x)[:sizeof(char)])

    def write_int(self, long long signed_datum):
        """int and long values are written using variable-length, zig-zag coding.
        """
        cdef:
            unsigned long long datum
            char temp_datum
        datum = (signed_datum << 1) ^ (signed_datum >> 63)
        while datum > 127:
            temp_datum = (datum & 0x7f) | 0x80
            self.writer.write((<char *>&temp_datum)[:sizeof(char)])
            datum >>= 7
        self.writer.write((<char *>&datum)[:sizeof(char)])

    write_long = write_int

    def write_float(self, float datum):
        """
        A float is written as 4 bytes.
        The float is converted into a 32-bit integer using a method equivalent to
        Java's floatToIntBits and then encoded in little-endian format.
        """
        self.writer.write((<char *>&datum)[:sizeof(float)])

    def write_double(self, double datum):
        """
        A double is written as 8 bytes.
        The double is converted into a 64-bit integer using a method equivalent to
        Java's doubleToLongBits and then encoded in little-endian format.
        """
        self.writer.write((<char *>&datum)[:sizeof(double)])
        # bits = STRUCT_LONG.unpack(STRUCT_DOUBLE.pack(datum))[0]
        # self.write(chr((bits) & 0xFF))
        # self.write(chr((bits >> 8) & 0xFF))
        # self.write(chr((bits >> 16) & 0xFF))
        # self.write(chr((bits >> 24) & 0xFF))
        # self.write(chr((bits >> 32) & 0xFF))
        # self.write(chr((bits >> 40) & 0xFF))
        # self.write(chr((bits >> 48) & 0xFF))
        # self.write(chr((bits >> 56) & 0xFF))

    def write_bytes(self, char* datum):
        """
        Bytes are encoded as a long followed by that many bytes of data. 
        """
        cdef:
            long byte_count
        byte_count = len(datum)
        self.write_long(byte_count)
        self.writer.write(datum)

    def write_utf8(self, char* datum):
        """
        A string is encoded as a long followed by
        that many bytes of UTF-8 encoded character data.
        """
        # datum = datum.encode("utf-8")
        self.write_bytes(datum)

    def write_crc32(self, bytes):
        """
        A 4-byte, big-endian CRC32 checksum
        """
        self.write(STRUCT_CRC32.pack(crc32(bytes) & 0xffffffff))
