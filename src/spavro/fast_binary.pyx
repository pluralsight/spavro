# Copyright (C) 2018 Pluralsight LLC
'''Fast Cython extension for reading / writing and validating AVRO records.

The main edge this code has is that it parses the schema only once and creates
a reader/writer call tree from the schema shape. All reads and writes then
no longer consult the schema saving lookups.'''

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


def read_boolean(fo):
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
        union_index = read_long(fo)
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
        record_name = schema.get('name')
        if namespace:
           namspace_record_name = '.'.join([namespace, record_name])
        else:
            namspace_record_name = record_name
        schema_cache[namspace_record_name] = placeholder
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


cdef void write_null(outbuf, datum):
    pass


cdef void write_fixed(outbuf, datum):
    """A fixed writer writes out exactly the bytes up to a count"""
    outbuf.write(datum)


cdef write_boolean(outbuf, char datum):
    """A boolean is written as a single byte whose value is either 0 (false) or
    1 (true)."""
    cdef char x = 1 if datum else 0
    outbuf.write((<char *>&x)[:sizeof(char)])


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
        return all([item_check(item) for item in datum])
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
        if int in writer_lookup_dict:
            writer_lookup_dict[long] = writer_lookup_dict[int]
        if unicode in writer_lookup_dict:
            writer_lookup_dict[str] = writer_lookup_dict[unicode]
        # warning, this will fail if there's both a long and int in a union
        # or a float and a double in a union (which is valid but nonsensical
        # in python but valid in avro)
        def simple_writer_lookup(datum):
            try:
                return writer_lookup_dict[type(datum)]
            except KeyError:
                raise TypeError("{} - Invalid type in union. Schema: {}".format(repr(datum), schema))

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

        if int in writer_lookup_dict:
            writer_lookup_dict[long] = writer_lookup_dict[int]
        if unicode in writer_lookup_dict:
            writer_lookup_dict[str] = writer_lookup_dict[unicode]

        def complex_writer_lookup(datum):
            cdef:
                long idx
                list lookup_result
            try:
                lookup_result = writer_lookup_dict[type(datum)]
            except KeyError:
                raise TypeError("{} - Invalid datum for type in union. Schema: {}".format(repr(datum), schema))
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

    def write_union(outbuf, datum):
        idx, data_writer = writer_lookup(datum)
        write_long(outbuf, idx)
        data_writer(outbuf, datum)
    write_union.__reduce__ = lambda: (make_union_writer, (union_schema,))
    return write_union

def make_enum_writer(schema):
    cdef list symbols = schema['symbols']

    # the datum can be str or unicode?
    def write_enum(outbuf, basestring datum):
        cdef int enum_index = symbols.index(datum)
        write_int(outbuf, enum_index)
    write_enum.__reduce__ = lambda: (make_enum_writer, (schema,))
    return write_enum


def make_record_writer(schema):
    cdef list fields = [WriteField(field['name'], get_writer(field['type'])) for field in schema['fields']]

    def write_record(outbuf, datum):
        for field in fields:
            try:
                field.writer(outbuf, datum.get(field.name))
            except TypeError as e:
                raise TypeError("Error writing record schema at fieldname: '{}', datum: '{}'".format(field.name, repr(datum.get(field.name))))
    write_record.__reduce__ = lambda: (make_record_writer, (schema,))
    return write_record


def make_array_writer(schema):
    item_writer = get_writer(schema['items'])

    def write_array(outbuf, list datum):
        cdef long item_count = len(datum)
        if item_count > 0:
            write_long(outbuf, item_count)
        for item in datum:
            item_writer(outbuf, item)
        write_long(outbuf, 0)
    write_array.__reduce__ = lambda: (make_array_writer, (schema,))
    return write_array


def make_map_writer(schema):
    map_value_writer = get_writer(schema['values'])

    def write_map(outbuf, datum):
        cdef long item_count = len(datum)
        if item_count > 0:
            write_long(outbuf, item_count)
        for key, val in datum.iteritems():
            write_utf8(outbuf, key)
            map_value_writer(outbuf, val)
        write_long(outbuf, 0)
    write_map.__reduce__ = lambda: (make_map_writer, (schema,))
    return write_map


def make_boolean_writer(schema):
    '''Create a boolean writer, adds a validation step before the actual
    write function'''
    def checked_boolean_writer(outbuf, datum):
        if not isinstance(datum, bool):
            raise TypeError("{} - Not a boolean value. Schema: {}".format(repr(datum), schema))
        write_boolean(outbuf, datum)
    return checked_boolean_writer


def make_fixed_writer(schema):
    '''A writer that must write X bytes defined by the schema'''
    cdef long size = schema['size']
    # note: not a char* because those are null terminated and fixed
    # has no such limitation
    def checked_write_fixed(outbuf, datum):
        if len(datum) != size:
            raise TypeError("{} - Size Mismatch ({}) for Fixed data. Schema: {}".format(repr(datum), len(datum), schema))
        write_fixed(outbuf, datum)
    return checked_write_fixed


def make_int_writer(schema):
    '''Create a int writer, adds a validation step before the actual
    write function to make sure the int value doesn't overflow'''
    def checked_int_write(outbuf, datum):
        if not (isinstance(datum, six.integer_types)
                        and INT_MIN_VALUE <= datum <= INT_MAX_VALUE):
            raise TypeError("{} - Non integer value or overflow. Schema: {}".format(datum, schema))
        write_long(outbuf, datum)
    return checked_int_write


def make_long_writer(schema):
    '''Create a long writer, adds a validation step before the actual
    write function to make sure the long value doesn't overflow'''
    def checked_long_write(outbuf, datum):
        if not (isinstance(datum, six.integer_types)
                        and LONG_MIN_VALUE <= datum <= LONG_MAX_VALUE):
            raise TypeError("{} - Non integer value or overflow. Schema: {}".format(repr(datum), schema))
        write_long(outbuf, datum)
    return checked_long_write


def make_string_writer(schema):
    def checked_string_writer(outbuf, datum):
        if not isinstance(datum, six.string_types):
            raise TypeError("{} - is not a string value. Schema: {}".format(repr(datum), schema))
        write_utf8(outbuf, datum)
    return checked_string_writer


def make_byte_writer(schema):
    return write_bytes


def make_float_writer(schema):
    return write_float


def make_double_writer(schema):
    return write_double


def make_null_writer(schema):
    return write_null


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

    def __call__(self, fo, val):
        return self.writer(fo, val)


def get_writer(schema):
    cdef unicode schema_type = get_type(schema)

    if schema_type in ('record', 'fixed', 'enum'):
        placeholder = WriterPlaceholder()
        # using a placeholder because this is recursive and the writer isn't defined
        # yet and nested records might refer to this parent schema name
        namespace = schema.get('namespace')
        name = schema.get('name')
        if namespace:
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
        writer = schema_cache[schema_type]

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
