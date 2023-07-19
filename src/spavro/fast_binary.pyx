# Copyright (C) 2018 Pluralsight LLC
'''Fast Cython extension for reading / writing and validating AVRO records.

The main edge this code has is that it parses the schema only once and creates
a reader/writer call tree from the schema shape. All reads and writes then
no longer consult the schema saving lookups.'''

cimport cpython.array as array
from libc.string cimport memcpy

import six
INT_MIN_VALUE = -(1 << 31)
INT_MAX_VALUE = (1 << 31) - 1
LONG_MIN_VALUE = -(1 << 63)
LONG_MAX_VALUE = (1 << 63) - 1


cdef long long read_long(fo):
    '''Read a long using zig-zag binary encoding'''
    cdef:
        unsigned long long accum
        unsigned long long temp_datum
        char* c_raw
        int shift = 7
    # this ping-pong casting is required for Python 2.7
    # not sure why exactly
    raw = fo.read(1)
    c_raw = raw
    temp_datum = c_raw[0]
    accum = temp_datum & 0x7F
    while (temp_datum & 0x80) != 0:
        raw = fo.read(1)
        c_raw = raw
        temp_datum = c_raw[0]
        accum |= (temp_datum & 0x7F) << shift
        shift += 7
    # to convert from the zig zag value back to regular int
    # bit shift right 1 bit, then xor with the lsb * -1 (which would flip all
    # the bits if it is '1' reversing the 2's compliment)
    return (accum >> 1) ^ -(accum & 1)


cdef bytes read_bytes(fo):
    '''Bytes are a marker for length of bytes and then binary data'''
    return fo.read(read_long(fo))


cdef read_null(fo):
    """
    null is written as zero bytes
    """
    return None


cdef bint read_boolean(fo):
    """
    a boolean is written as a single byte 
    whose value is either 0 (false) or 1 (true).
    """
    return fo.read(1) == b'\x01'


cdef float read_float(fo):
    """
    A float is written as 4 bytes.
    The float is converted into a 32-bit integer using a method equivalent to
    Java's floatToIntBits and then encoded in little-endian format.
    """
    data = fo.read(4)
    cdef char* y = data
    return (<float*>y)[0]


cdef double read_double(fo):
    """
    A double is written as 8 bytes.
    The double is converted into a 64-bit integer using a method equivalent to
    Java's doubleToLongBits and then encoded in little-endian format.
    """
    data = fo.read(8)
    cdef char* y = data
    return (<double*>y)[0]

cdef unicode read_utf8(fo):
    """
    A string is encoded as a long followed by
    that many bytes of UTF-8 encoded character data.
    """
    byte_data = read_bytes(fo)
    return unicode(byte_data, "utf-8")

# ======================================================================
from collections import namedtuple
ReadField = namedtuple('ReadField', ['name', 'reader', 'skip'])
WriteField = namedtuple('WriteField', ['name', 'writer'])

cpdef unicode get_type(schema):
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
        cdef long long union_index = read_long(fo)
        try:
            return readers[union_index](fo)
        except IndexError:
            raise TypeError("Unable to process union schema {}, union index '{}' doesn't exist. This is most likely because the read schema being used is not compatible with the write schema used to write the data.".format(repr(union_schema), union_index))
    union_reader.__reduce__ = lambda: (make_union_reader, (union_schema,))
    return union_reader


def make_record_reader(schema):
    cdef list fields = [ReadField(field['name'], get_reader(field['type']), get_type(field['type']) == 'skip') for field in schema['fields']]

    def record_reader(fo):
        return {field.name: field.reader(fo) for field in fields if not (field.skip and field.reader(fo) is None)}
    record_reader.__reduce__ = lambda: (make_record_reader, (schema,))
    return record_reader


def make_enum_reader(schema):
    cdef list symbols = schema['symbols']

    def enum_reader(fo):
        return symbols[read_long(fo)]
    enum_reader.__reduce__ = lambda: (make_enum_reader, (schema,))
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
    array_reader.__reduce__ = lambda: (make_array_reader, (schema,))
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
            for _ in range(block_count):
                key = read_utf8(fo)
                read_items[key] = value_reader(fo)
            block_count = read_long(fo)
        return read_items
    map_reader.__reduce__ = lambda: (make_map_reader, (schema,))
    return map_reader

def make_fixed_reader(schema):
    cdef long size = schema['size']

    def fixed_reader(fo):
        return fo.read(size)
    fixed_reader.__reduce__ = lambda: (make_fixed_reader, (schema,))
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


def make_skip_reader(schema):
    # this will create a regular reader that will iterate the bytes
    # in the avro stream properly
    value_reader = get_reader(schema['value'])
    def read_skip(fo):
        value_reader(fo)
        return None
    read_skip.__reduce__ = lambda: (make_skip_reader, (schema,))
    return read_skip


def make_default_reader(schema):
    value = schema["value"]
    def read_default(fo):
        return value
    read_default.__reduce__ = lambda: (make_default_reader, (schema,))
    return read_default


reader_type_map = {
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
    'map': make_map_reader,
    'skip': make_skip_reader,
    'default': make_default_reader
}

schema_cache = {}

class ReaderPlaceholder(object):
    def __init__(self):
        self.reader = None

    def __call__(self, fo):
        return self.reader(fo)

def get_reader(schema):
    cdef unicode schema_type = get_type(schema)
    if schema_type in ('record', 'fixed', 'enum'):
        placeholder = ReaderPlaceholder()
        # using a placeholder because this is recursive and the reader isn't defined
        # yet and nested records might refer to this parent schema name
        namespace = schema.get('namespace')
        name = schema.get('name')
        if namespace and "." not in name:
           fullname = '.'.join([namespace, name])
        else:
            fullname = name
        schema_cache[fullname] = placeholder
        reader = reader_type_map[schema_type](schema)
        # now that we've returned, assign the reader to the placeholder
        # so that the execution will work
        placeholder.reader = reader
        return reader
    try:
        reader = reader_type_map[schema_type](schema)
    except KeyError:
        reader = schema_cache[schema_type]

    return reader

# ======================================================================


cdef void write_int(outbuf, long long signed_datum):
    """int and long values are written using variable-length, zig-zag coding.
    """
    cdef:
        unsigned long long datum
        char temp_datum
    datum = (signed_datum << 1) ^ (signed_datum >> 63)
    while datum > 127:
        temp_datum = (datum & 0x7f) | 0x80
        outbuf.write((<char *>&temp_datum)[:sizeof(char)])
        datum >>= 7
    outbuf.write((<char *>&datum)[:sizeof(char)])

write_long = write_int


cdef void write_bytes(outbuf, datum):
    """
    Bytes are encoded as a long followed by that many bytes of data. 
    """
    cdef long byte_count = len(datum)
    write_long(outbuf, byte_count)
    outbuf.write(datum)


cdef void write_utf8(outbuf, datum):
    """
    Unicode are encoded as write_bytes of the utf-8 encoded data.
    """
    write_bytes(outbuf, datum.encode("utf-8"))


cdef void write_float(outbuf, float datum):
    """
    A float is written as 4 bytes.
    The float is converted into a 32-bit integer using a method equivalent to
    Java's floatToIntBits and then encoded in little-endian format.
    """
    outbuf.write((<char *>&datum)[:sizeof(float)])


cdef void write_double(outbuf, double datum):
    """
    A double is written as 8 bytes.
    The double is converted into a 64-bit integer using a method equivalent to
    Java's doubleToLongBits and then encoded in little-endian format.
    """
    outbuf.write((<char *>&datum)[:sizeof(double)])


cdef write_boolean(outbuf, char datum):
    """A boolean is written as a single byte whose value is either 0 (false) or
    1 (true)."""
    cdef char x = 1 if datum else 0
    outbuf.write((<char *>&x)[:sizeof(char)])


cdef void write_int_to_array(array.array outbuf, long long signed_datum):
    """int and long values are written using variable-length, zig-zag coding.
    """
    cdef:
        unsigned long long datum
        char temp_datum
        size_t size = len(outbuf)
    datum = (signed_datum << 1) ^ (signed_datum >> 63)
    while datum > 127:
        temp_datum = (datum & 0x7f) | 0x80
        array.resize_smart(outbuf, size + 1)
        outbuf.data.as_uchars[size] = temp_datum
        size += 1
        datum >>= 7
    array.resize_smart(outbuf, size + 1)
    outbuf.data.as_uchars[size] = datum

cdef void write_long_to_array(array.array outbuf, long long signed_datum):
    """int and long values are written using variable-length, zig-zag coding.
    """
    cdef:
        unsigned long long datum
        char temp_datum
        size_t size = len(outbuf)
    datum = (signed_datum << 1) ^ (signed_datum >> 63)
    while datum > 127:
        temp_datum = (datum & 0x7f) | 0x80
        array.resize_smart(outbuf, size + 1)
        outbuf.data.as_uchars[size] = temp_datum
        size += 1
        datum >>= 7
    array.resize_smart(outbuf, size + 1)
    outbuf.data.as_uchars[size] = datum


cdef void write_bytes_to_array(array.array outbuf, datum):
    """
    Bytes are encoded as a long followed by that many bytes of data.
    """
    cdef:
        size_t datum_size = len(datum)
        size_t size = 0
    write_long_to_array(outbuf, datum_size)
    size = len(outbuf)
    array.resize_smart(outbuf, size + datum_size)
    memcpy(outbuf.data.as_chars + size, <const char*>datum, datum_size)


cdef void write_utf8_to_array(array.array outbuf, datum):
    """
    Unicode are encoded as write_bytes_to_array of the utf-8 encoded data.
    """
    write_bytes_to_array(outbuf, datum.encode("utf-8"))


cdef void write_float_to_array(array.array outbuf, float datum):
    """
    A float is written as 4 bytes.
    The float is converted into a 32-bit integer using a method equivalent to
    Java's floatToIntBits and then encoded in little-endian format.
    """
    cdef:
        size_t datum_size = sizeof(float)
        size_t size = len(outbuf)
    array.resize_smart(outbuf, size + datum_size)
    memcpy(outbuf.data.as_chars + size, <const char*>&datum, datum_size)

cdef void write_double_to_array(array.array outbuf, double datum):
    """
    A double is written as 8 bytes.
    The double is converted into a 64-bit integer using a method equivalent to
    Java's doubleToLongBits and then encoded in little-endian format.
    """
    cdef:
        size_t datum_size = sizeof(double)
        size_t size = len(outbuf)
    array.resize_smart(outbuf, size + datum_size)
    memcpy(outbuf.data.as_chars + size, <const char*>&datum, datum_size)


cdef void write_fixed_to_array(array.array outbuf, datum):
    """A fixed writer writes out exactly the bytes up to a count"""
    cdef:
        size_t datum_size = len(datum)
        size_t size = len(outbuf)
    array.resize_smart(outbuf, size + datum_size)
    memcpy(outbuf.data.as_chars + size, <const char*>datum, datum_size)


cdef void write_boolean_to_array(array.array outbuf, char datum):
    """A boolean is written as a single byte whose value is either 0 (false) or
    1 (true)."""
    cdef:
        char x = 1 if datum else 0
        size_t size = len(outbuf)
    array.resize_smart(outbuf, size + 1)
    outbuf.data.as_uchars[size] = x


cdef void write_enum_to_array(array.array outbuf, char datum, list symbols):
    cdef int enum_index = symbols.index(datum)
    write_int_to_array(outbuf, enum_index)


cdef void write_array_to_array(array.array outbuf, list datum, list item_writer):
    cdef:
        size_t item_count = len(datum)
    if item_count > 0:
        write_long_to_array(outbuf, item_count)
    for item in datum:
        execute(outbuf, item, item_writer)
    write_long_to_array(outbuf, 0)


cdef void write_map_to_array(array.array outbuf, dict datum, list map_value_writer):
    cdef:
        size_t item_count = len(datum)
    if item_count > 0:
        write_long_to_array(outbuf, item_count)
    for key, val in datum.iteritems():
        write_utf8_to_array(outbuf, key)
        execute(outbuf, val, map_value_writer)
    write_long_to_array(outbuf, 0)


avro_to_py = {
    u"string": unicode,
    u"int": int,
    u"long": int,
    u"boolean": bool,
    u"null": type(None),
    u"float": float,
    u"double": float,
    u"array": list,
    u"record": dict,
    u"enum": unicode,
    u"fixed": bytes,
    u"bytes": bytes,
    u"map": dict
}


# ===============================
CheckField = namedtuple('CheckField', ['name', 'check'])

def get_check(schema):
    schema = lookup_schema(schema)
    cdef unicode schema_type = get_type(schema)
    return check_type_map[schema_type](schema)


def make_record_check(schema):
    cdef list fields = [CheckField(field['name'], get_check(field['type'])) for field in schema['fields']]
    def record_check(datum):
        return isinstance(datum, dict) and all([field.check(datum.get(field.name)) for field in fields])
    return record_check


def make_enum_check(schema):
    cdef list symbols = schema['symbols']
    def enum_check(datum):
        return datum in symbols
    return enum_check


def make_null_check(schema):
    return lambda datum: datum is None

def check_string(datum):
    return isinstance(datum, basestring)

def make_string_check(schema):
    return check_string

def make_long_check(schema):
    return lambda datum: isinstance(datum, int) or isinstance(datum, long)

def make_boolean_check(schema):
    return lambda datum: isinstance(datum, bool)

def make_float_check(schema):
    return lambda datum: isinstance(datum, int) or isinstance(datum, long) or isinstance(datum, float)

def make_double_check(schema):
    return lambda datum: isinstance(datum, int) or isinstance(datum, long) or isinstance(datum, float)

def make_byte_check(schema):
    return lambda datum: isinstance(datum, str) or isinstance(datum, bytes)

def make_array_check(schema):
    item_check = get_check(schema['items'])
    def array_check(datum):
        return isinstance(datum, list) and all([item_check(item) for item in datum])
    return array_check

def make_union_check(union_schema):
    cdef list union_checks = [get_check(schema) for schema in union_schema]
    def union_check(datum):
        return any([check(datum) for check in union_checks])
    return union_check

def make_fixed_check(schema):
    cdef int size = schema['size']
    def fixed_check(datum):
        return (isinstance(datum, str) or isinstance(datum, bytes)) and len(datum) == size
    return fixed_check

def make_map_check(schema):
    map_value_check = get_check(schema['values'])
    def map_check(datum):
        return isinstance(datum, dict) and all([check_string(key) and map_value_check(value) for key, value in datum.items()])
    return map_check

check_type_map = {
    'union': make_union_check,
    'record': make_record_check,
    'null': make_null_check,
    'string': make_string_check,
    'boolean': make_boolean_check,
    'double': make_double_check,
    'float': make_float_check,
    'long': make_long_check,
    'bytes': make_byte_check,
    'int': make_long_check,
    'fixed': make_fixed_check,
    'enum': make_enum_check,
    'array': make_array_check,
    'map': make_map_check
}

# ====================

def lookup_schema(schema):
    '''Check if a schema is a standard type, if not, lookup in the custom
    schema dictionary and replace the custom name with the expanded original
    schema.'''
    check_type = get_type(schema)
    if check_type in writer_type_map:
        return schema
    return custom_schema[check_type]


cdef void create_promotions_for_union(dict writer_lookup_dict):
    '''Take the writer lookup for a union and create some aliases and promotion
    cases, and store those back into the writer lookup.'''
    # handle promotion cases
    # long and int are encoded the same way
    if int in writer_lookup_dict:
        writer_lookup_dict[long] = writer_lookup_dict[int]
    # py2 and py3 both handle str/unicode differently
    if unicode in writer_lookup_dict:
        writer_lookup_dict[str] = writer_lookup_dict[unicode]
    # allow the use of ints to 'find' float unions
    if float in writer_lookup_dict and int not in writer_lookup_dict:
        writer_lookup_dict[int] = writer_lookup_dict[float]
    # allow strings to be used for byte data, if there's no string type
    if bytes in writer_lookup_dict and str not in writer_lookup_dict:
        writer_lookup_dict[str] = writer_lookup_dict[bytes][0], lambda output_buffer, val: writer_lookup_dict[bytes][1](output_buffer, val.encode('utf-8'))
    if bytes in writer_lookup_dict and unicode not in writer_lookup_dict:
        writer_lookup_dict[unicode] = writer_lookup_dict[bytes][0], lambda output_buffer, val: writer_lookup_dict[bytes][1](output_buffer, val.encode('utf-8'))


def make_union_writer(union_schema):
    cdef list type_list = [get_type(lookup_schema(schema)) for schema in union_schema]
    # cdef dict writer_lookup
    # cdef list record_list
    cdef dict writer_lookup_dict
    cdef char simple_union
    cdef list lookup_result
    cdef long idx

    # if there's more than one kind of record in the union
    # or if there's a string, enum or fixed combined in the union
    # or there is both a record and a map in the union then it's no longer
    # simple. The reason is that reocrds and maps both correspond to python
    # dict so a simple python type lookup isn't enough to schema match.
    # enums, strings and fixed are all python data type unicode or string
    # so those won't work either when mixed
    simple_union = not(type_list.count('record') > 1 or
                      len(set(type_list) & set(['string', 'enum', 'fixed', 'bytes'])) > 1 or
                      len(set(type_list) & set(['record', 'map'])) > 1)

    if simple_union:
        writer_lookup_dict = {avro_to_py[get_type(lookup_schema(schema))]: (idx, get_writer(schema)) for idx, schema in enumerate(union_schema)}

        create_promotions_for_union(writer_lookup_dict)

        # warning, this will fail if there's both a long and int in a union
        # or a float and a double in a union (which is valid but nonsensical
        # in python but valid in avro)
        def simple_writer_lookup(datum):
            try:
                return writer_lookup_dict[type(datum)]
            except KeyError:
                raise TypeError("{} - Invalid type ({}) in union. Schema: {}".format(repr(datum), type(datum), union_schema))

        writer_lookup = simple_writer_lookup
    else:
        writer_lookup_dict = {}
        for idx, schema in enumerate(union_schema):
            python_type = avro_to_py[get_type(lookup_schema(schema))]
            # TODO: if fixed and bytes are in the schema then we should check fixed before bytes
            # I think, since that's more efficient space wise?
            if python_type in writer_lookup_dict:
                writer_lookup_dict[python_type] = writer_lookup_dict[python_type] + [(idx, get_check(schema), get_writer(schema))]
            else:
                writer_lookup_dict[python_type] = [(idx, get_check(schema), get_writer(schema))]

        create_promotions_for_union(writer_lookup_dict)
        # if int in writer_lookup_dict:
        #     writer_lookup_dict[long] = writer_lookup_dict[int]
        # if unicode in writer_lookup_dict:
        #     writer_lookup_dict[str] = writer_lookup_dict[unicode]
        # if float in writer_lookup_dict and int not in writer_lookup_dict:
        #     writer_lookup_dict[int] = writer_lookup_dict[float]
        # if bytes in writer_lookup_dict and str not in writer_lookup_dict:
        #     writer_lookup_dict[str] = writer_lookup_dict[bytes]


        def complex_writer_lookup(datum):
            cdef:
                long idx
                list lookup_result
            try:
                lookup_result = writer_lookup_dict[type(datum)]
            except KeyError:
                raise TypeError("{} - Invalid type ({}) in union. Schema: {}".format(repr(datum), type(datum), union_schema))
            if len(lookup_result) == 1:
                idx, get_check, writer = lookup_result[0]
            else:
                for idx, get_check, writer in lookup_result:
                    if get_check(datum):
                        break
                else:
                    raise TypeError("No matching schema for datum: {}".format(repr(datum)))
            return idx, writer

        writer_lookup = complex_writer_lookup

    return [0, writer_lookup]

def make_record_writer(schema):
    cdef list fields = [WriteField(field['name'], get_writer(field['type'])) for field in schema['fields']]
    return [1, fields]

def make_null_writer(schema):
    return [2]

def make_string_writer(schema):
    return [3]

def make_boolean_writer(schema):
    '''Create a boolean writer, adds a validation step before the actual
    write function'''
    return [4]

def make_double_writer(schema):
    return [5]

def make_float_writer(schema):
    return [6]

def make_long_writer(schema):
    '''Create a long writer, adds a validation step before the actual
    write function to make sure the long value doesn't overflow'''
    return [7]

def make_byte_writer(schema):
    return [8]

def make_int_writer(schema):
    '''Create a int writer, adds a validation step before the actual
    write function to make sure the int value doesn't overflow'''
    return [9]

def make_fixed_writer(schema):
    '''A writer that must write X bytes defined by the schema'''
    cdef long size = schema['size']
    # note: not a char* because those are null terminated and fixed
    # has no such limitation
    return [10, size]

def make_enum_writer(schema):
    cdef list symbols = schema['symbols']
    return [11, symbols]

def make_array_writer(schema):
    cdef list item_writer = get_writer(schema['items'])
    return [12, item_writer]

def make_map_writer(schema):
    cdef list map_value_writer = get_writer(schema['values'])
    return [13, map_value_writer]


# writer
writer_type_map = {
    'union': make_union_writer,
    'record': make_record_writer,
    'null': make_null_writer,
    'string': make_string_writer,
    'boolean': make_boolean_writer,
    'double': make_double_writer,
    'float': make_float_writer,
    'long': make_long_writer,
    'bytes': make_byte_writer,
    'int': make_int_writer,
    'fixed': make_fixed_writer,
    'enum': make_enum_writer,
    'array': make_array_writer,
    'map': make_map_writer
}

custom_schema = {}

class WriterPlaceholder(object):
    def __init__(self):
        self.writer = None


def get_writer(schema):
    cdef unicode schema_type = get_type(schema)

    if schema_type in ('record', 'fixed', 'enum'):
        placeholder = WriterPlaceholder()
        # using a placeholder because this is recursive and the writer isn't defined
        # yet and nested records might refer to this parent schema name
        namespace = schema.get('namespace')
        name = schema.get('name')
        if namespace and "." not in name:
            fullname = '.'.join([namespace, name])
        else:
            fullname = name
        custom_schema[fullname] = schema
        schema_cache[fullname] = placeholder
        writer = writer_type_map[schema_type](schema)
        # now that we've returned, assign the writer to the placeholder
        # so that the execution will work
        placeholder.writer = writer
        return writer
    try:
        writer = writer_type_map[schema_type](schema)
    except KeyError:
        # lookup the schema by unique previously defined name,
        # i.e. a custom type
        writer = schema_cache[schema_type].writer

    return writer


import struct
from binascii import crc32


class FastBinaryEncoder(object):
    """Write leaf values."""
    def __init__(self, writer):
        """
        writer is a Python object on which we can call write.
        """
        self.writer = writer

    def write(self, datum):
        self.writer.write(datum)

    def write_null(self, datum):
        pass

    def write_boolean(self, datum):
        write_boolean(self.writer, datum)

    def write_int(self, datum):
        write_int(self.writer, datum)

    def write_long(self, datum):
        write_long(self.writer, datum)

    def write_float(self, datum):
        write_float(self.writer, datum)

    def write_double(self, datum):
        write_double(self.writer, datum)

    def write_bytes(self, datum):
        write_bytes(self.writer, datum)

    def write_utf8(self, datum):
        write_utf8(self.writer, datum)

    def write_crc32(self, bytes):
        """
        A 4-byte, big-endian CRC32 checksum
        """
        self.writer.write(struct.pack("!I", crc32(bytes) & 0xffffffff))



class FastBinaryDecoder(object):
    """Read leaf values."""
    def __init__(self, reader):
        """
        reader is a Python object on which we can call read, seek, and tell.
        """
        self.reader = reader

    def read(self, n):
        return self.reader.read(n)

    def read_null(self):
        return None

    def read_boolean(self):
        return read_boolean(self.reader)

    def read_int(self):
        return read_long(self.reader)

    def read_long(self):
        return read_long(self.reader)

    def read_float(self):
        return read_float(self.reader)

    def read_double(self):
        return read_double(self.reader)

    def read_bytes(self):
        return read_bytes(self.reader)

    def read_utf8(self):
        return read_utf8(self.reader)

    def check_crc32(self, bytes):
        checksum = struct.unpack("!I", self.reader.read(4))[0]
        if crc32(bytes) & 0xffffffff != checksum:
            raise RuntimeError("Checksum failure")

    def skip_null(self):
        pass

    def skip_boolean(self):
        self.reader.read(1)

    def skip_int(self):
        read_long(self.reader)

    def skip_long(self):
        read_long(self.reader)

    def skip_float(self):
        read_float(self.reader)

    def skip_double(self):
        read_double(self.reader)

    def skip_bytes(self):
        read_bytes(self.reader)

    def skip_utf8(self):
        read_utf8(self.reader)

    def skip(self, n):
        self.reader.seek(self.reader.tell() + n)


cdef void write_union_to_array(array.array outbuf, datum, writer_lookup):
    idx, data_writer = writer_lookup(datum)  # TODO: cdef int, list = f()?
    write_long_to_array(outbuf, idx)
    execute(outbuf, datum, data_writer)


cdef void write_record_to_array(array.array outbuf, dict datum, list fields):
    for field in fields:
        try:
            execute(outbuf, datum.get(field.name), field.writer)
        except TypeError as e:
            raise TypeError("Error writing record schema at fieldname: '{}', datum: '{}'".format(field.name, repr(datum.get(field.name))))


cdef void execute(array.array outbuf, datum, list writer):
    cdef unsigned int writer_f = writer[0]

    if writer_f == 0:  # make_union_writer
        write_union_to_array(outbuf, datum, writer[1])

    elif writer_f == 1:  # make_record_writer
        write_record_to_array(outbuf, datum, writer[1])

    # skip make_null_writer

    elif writer_f == 3:  # make_string_writer
        if not isinstance(datum, six.string_types):
            raise TypeError("{} - is not a string value.".format(repr(datum)))
        write_utf8_to_array(outbuf, datum)

    elif writer_f == 4:  # make_boolean_writer
        if not isinstance(datum, bool):
            raise TypeError("{} - Not a boolean value.".format(repr(datum)))
        write_boolean_to_array(outbuf, datum)

    elif writer_f == 5:  # make_double_writer
        write_double_to_array(outbuf, datum)

    elif writer_f == 6:  # make_float_writer
        write_float_to_array(outbuf, datum)

    elif writer_f == 7:  # make_long_writer
        if not (isinstance(datum, six.integer_types)
                        and LONG_MIN_VALUE <= datum <= LONG_MAX_VALUE):
            raise TypeError("{} - Non integer value or overflow.".format(repr(datum)))
        write_long_to_array(outbuf, datum)

    elif writer_f == 8:  # make_byte_writer
        write_bytes_to_array(outbuf, datum)

    elif writer_f == 9:  # make_int_writer
        if not isinstance(datum, six.integer_types):
            raise TypeError("Schema violation, {} is not an example of integer".format(datum))
        if not INT_MIN_VALUE <= datum <= INT_MAX_VALUE:
            raise TypeError("Schema violation, value overflow. {} can't be stored in schema".format(datum))
        write_long_to_array(outbuf, datum)

    elif writer_f == 10:  # make_fixed_writer
        write_fixed_to_array(outbuf, datum)

    elif writer_f == 11:  # make_enum_writer
        write_enum_to_array(outbuf, datum, writer[1])

    elif writer_f == 12:  # make_union_writer
        write_array_to_array(outbuf, datum, writer[1])

    elif writer_f == 13:  # make_union_writer
        write_map_to_array(outbuf, datum, writer[1])


def write(iobuffer, datum, writer):
    cdef array.array outbuf = array.array('B', [])
    execute(outbuf, datum, writer)
    iobuffer.write(outbuf.data.as_chars[:len(outbuf)])
