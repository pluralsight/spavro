# Modifications copyright (C) 2017 Pluralsight LLC

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
Input/Output utilities, including:

 * i/o-specific constants
 * i/o-specific exceptions
 * schema validation
 * leaf value encoding and decoding
 * datum reader/writer stuff (?)

Also includes a generic representation for data, which
uses the following mapping:

    * Schema records are implemented as dict.
    * Schema arrays are implemented as list.
    * Schema maps are implemented as dict.
    * Schema strings are implemented as unicode.
    * Schema bytes are implemented as str.
    * Schema ints are implemented as int.
    * Schema longs are implemented as long.
    * Schema floats are implemented as float.
    * Schema doubles are implemented as float.
    * Schema booleans are implemented as bool.
"""
import struct
from spavro import schema
import sys
from binascii import crc32

try:
    import json
except ImportError:
    import simplejson as json

import six
# python3
try:
    long
except NameError:
    long = int

from spavro.schema_resolve import resolve
#
# Constants
#

INT_MIN_VALUE = -(1 << 31)
INT_MAX_VALUE = (1 << 31) - 1
LONG_MIN_VALUE = -(1 << 63)
LONG_MAX_VALUE = (1 << 63) - 1

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

#
# Exceptions
#

from spavro.exceptions import AvroTypeException, SchemaResolutionException

#
# Validate
#


def validate(expected_schema, datum):
    """Determine if a python datum is an instance of a schema."""
    schema_type = expected_schema.type
    if schema_type == 'null':
        return datum is None
    elif schema_type == 'boolean':
        return isinstance(datum, bool)
    elif schema_type == 'string':
        return isinstance(datum, six.string_types)
    elif schema_type == 'bytes':
        return isinstance(datum, bytes)
    elif schema_type == 'int':
        return (isinstance(datum, six.integer_types)
                        and INT_MIN_VALUE <= datum <= INT_MAX_VALUE)
    elif schema_type == 'long':
        return (isinstance(datum, six.integer_types)
                        and LONG_MIN_VALUE <= datum <= LONG_MAX_VALUE)
    elif schema_type in ['float', 'double']:
        return (isinstance(datum, six.integer_types)
                        or isinstance(datum, float))
    elif schema_type == 'fixed':
        return isinstance(datum, bytes) and len(datum) == expected_schema.size
    elif schema_type == 'enum':
        return datum in expected_schema.symbols
    elif schema_type == 'array':
        return (isinstance(datum, list) and
            False not in [validate(expected_schema.items, d) for d in datum])
    elif schema_type == 'map':
        return (isinstance(datum, dict) and
            False not in [isinstance(k, six.string_types) for k in datum.keys()] and
            False not in
                [validate(expected_schema.values, v) for v in datum.values()])
    elif schema_type in ['union', 'error_union']:
        return True in [validate(s, datum) for s in expected_schema.schemas]
    elif schema_type in ['record', 'error', 'request']:
        return (isinstance(datum, dict) and
            False not in
                [validate(f.type, datum.get(f.name)) for f in expected_schema.fields])

#
# Decoder/Encoder
#
import logging
log = logging.getLogger(__name__)
use_fast = False
try:
    from spavro.fast_binary import get_reader, get_writer
    from spavro.fast_binary import FastBinaryEncoder, FastBinaryDecoder
    use_fast = True
except ImportError:
    log.warn("Failed to load spavro C extension, using default slow Encoder/Decoder")
from spavro.binary import BinaryEncoder as SlowBinaryEncoder, BinaryDecoder as SlowBinaryDecoder

#
# SlowDatumReader/Writer
#


class SlowDatumReader(object):
    """Deserialize Avro-encoded data into a Python data structure."""
    @staticmethod
    def check_props(schema_one, schema_two, prop_list):
        for prop in prop_list:
            if getattr(schema_one, prop) != getattr(schema_two, prop):
                return False
        return True

    @staticmethod
    def match_schemas(writers_schema, readers_schema):
        w_type = writers_schema.type
        r_type = readers_schema.type
        if 'union' in [w_type, r_type] or 'error_union' in [w_type, r_type]:
            return True
        elif (w_type in schema.PRIMITIVE_TYPES and r_type in schema.PRIMITIVE_TYPES
                    and w_type == r_type):
            return True
        elif (w_type == r_type == 'record' and
                    SlowDatumReader.check_props(writers_schema, readers_schema, 
                                                                    ['fullname'])):
            return True
        elif (w_type == r_type == 'error' and
                    SlowDatumReader.check_props(writers_schema, readers_schema, 
                                                                    ['fullname'])):
            return True
        elif (w_type == r_type == 'request'):
            return True
        elif (w_type == r_type == 'fixed' and 
                    SlowDatumReader.check_props(writers_schema, readers_schema, 
                                                                    ['fullname', 'size'])):
            return True
        elif (w_type == r_type == 'enum' and 
                    SlowDatumReader.check_props(writers_schema, readers_schema, 
                                                                    ['fullname'])):
            return True
        elif (w_type == r_type == 'map' and 
                    SlowDatumReader.check_props(writers_schema.values,
                                                                    readers_schema.values, ['type'])):
            return True
        elif (w_type == r_type == 'array' and 
                    SlowDatumReader.check_props(writers_schema.items,
                                                                    readers_schema.items, ['type'])):
            return True
        
        # Handle schema promotion
        if w_type == 'int' and r_type in ['long', 'float', 'double']:
            return True
        elif w_type == 'long' and r_type in ['float', 'double']:
            return True
        elif w_type == 'float' and r_type == 'double':
            return True
        return False

    def __init__(self, writers_schema=None, readers_schema=None):
        """
        As defined in the Avro specification, we call the schema encoded
        in the data the "writer's schema", and the schema expected by the
        reader the "reader's schema".
        """
        self._writers_schema = writers_schema
        self._readers_schema = readers_schema 

    # read/write properties
    def set_writers_schema(self, writers_schema):
        self._writers_schema = writers_schema
    writers_schema = property(lambda self: self._writers_schema,
                                                        set_writers_schema)
    def set_readers_schema(self, readers_schema):
        self._readers_schema = readers_schema
    readers_schema = property(lambda self: self._readers_schema,
                                                        set_readers_schema)
    
    def read(self, decoder):
        if self.readers_schema is None:
            self.readers_schema = self.writers_schema
        return self.read_data(self.writers_schema, self.readers_schema, decoder)

    def read_data(self, writers_schema, readers_schema, decoder):
        # schema matching
        if not SlowDatumReader.match_schemas(writers_schema, readers_schema):
            fail_msg = 'Schemas do not match.'
            raise SchemaResolutionException(fail_msg, writers_schema, readers_schema)

        # schema resolution: reader's schema is a union, writer's schema is not
        if (writers_schema.type not in ['union', 'error_union']
                and readers_schema.type in ['union', 'error_union']):
            for s in readers_schema.schemas:
                if SlowDatumReader.match_schemas(writers_schema, s):
                    return self.read_data(writers_schema, s, decoder)
            fail_msg = 'Schemas do not match.'
            raise SchemaResolutionException(fail_msg, writers_schema, readers_schema)

        # function dispatch for reading data based on type of writer's schema
        if writers_schema.type == 'null':
            return decoder.read_null()
        elif writers_schema.type == 'boolean':
            return decoder.read_boolean()
        elif writers_schema.type == 'string':
            return decoder.read_utf8()
        elif writers_schema.type == 'int':
            return decoder.read_int()
        elif writers_schema.type == 'long':
            return decoder.read_long()
        elif writers_schema.type == 'float':
            return decoder.read_float()
        elif writers_schema.type == 'double':
            return decoder.read_double()
        elif writers_schema.type == 'bytes':
            return decoder.read_bytes()
        elif writers_schema.type == 'fixed':
            return self.read_fixed(writers_schema, readers_schema, decoder)
        elif writers_schema.type == 'enum':
            return self.read_enum(writers_schema, readers_schema, decoder)
        elif writers_schema.type == 'array':
            return self.read_array(writers_schema, readers_schema, decoder)
        elif writers_schema.type == 'map':
            return self.read_map(writers_schema, readers_schema, decoder)
        elif writers_schema.type in ['union', 'error_union']:
            return self.read_union(writers_schema, readers_schema, decoder)
        elif writers_schema.type in ['record', 'error', 'request']:
            return self.read_record(writers_schema, readers_schema, decoder)
        else:
            fail_msg = "Cannot read unknown schema type: %s" % writers_schema.type
            raise schema.AvroException(fail_msg)

    def skip_data(self, writers_schema, decoder):
        if writers_schema.type == 'null':
            return decoder.skip_null()
        elif writers_schema.type == 'boolean':
            return decoder.skip_boolean()
        elif writers_schema.type == 'string':
            return decoder.skip_utf8()
        elif writers_schema.type == 'int':
            return decoder.skip_int()
        elif writers_schema.type == 'long':
            return decoder.skip_long()
        elif writers_schema.type == 'float':
            return decoder.skip_float()
        elif writers_schema.type == 'double':
            return decoder.skip_double()
        elif writers_schema.type == 'bytes':
            return decoder.skip_bytes()
        elif writers_schema.type == 'fixed':
            return self.skip_fixed(writers_schema, decoder)
        elif writers_schema.type == 'enum':
            return self.skip_enum(writers_schema, decoder)
        elif writers_schema.type == 'array':
            return self.skip_array(writers_schema, decoder)
        elif writers_schema.type == 'map':
            return self.skip_map(writers_schema, decoder)
        elif writers_schema.type in ['union', 'error_union']:
            return self.skip_union(writers_schema, decoder)
        elif writers_schema.type in ['record', 'error', 'request']:
            return self.skip_record(writers_schema, decoder)
        else:
            fail_msg = "Unknown schema type: %s" % writers_schema.type
            raise schema.AvroException(fail_msg)

    def read_fixed(self, writers_schema, readers_schema, decoder):
        """
        Fixed instances are encoded using the number of bytes declared
        in the schema.
        """
        return decoder.read(writers_schema.size)

    def skip_fixed(self, writers_schema, decoder):
        return decoder.skip(writers_schema.size)

    def read_enum(self, writers_schema, readers_schema, decoder):
        """
        An enum is encoded by a int, representing the zero-based position
        of the symbol in the schema.
        """
        # read data
        index_of_symbol = decoder.read_int()
        if index_of_symbol >= len(writers_schema.symbols):
            fail_msg = "Can't access enum index %d for enum with %d symbols"\
                                 % (index_of_symbol, len(writers_schema.symbols))
            raise SchemaResolutionException(fail_msg, writers_schema, readers_schema)
        read_symbol = writers_schema.symbols[index_of_symbol]

        # schema resolution
        if read_symbol not in readers_schema.symbols:
            fail_msg = "Symbol %s not present in Reader's Schema" % read_symbol
            raise SchemaResolutionException(fail_msg, writers_schema, readers_schema)

        return read_symbol

    def skip_enum(self, writers_schema, decoder):
        return decoder.skip_int()

    def read_array(self, writers_schema, readers_schema, decoder):
        """
        Arrays are encoded as a series of blocks.

        Each block consists of a long count value,
        followed by that many array items.
        A block with count zero indicates the end of the array.
        Each item is encoded per the array's item schema.

        If a block's count is negative,
        then the count is followed immediately by a long block size,
        indicating the number of bytes in the block.
        The actual count in this case
        is the absolute value of the count written.
        """
        read_items = []
        block_count = decoder.read_long()
        while block_count != 0:
            if block_count < 0:
                block_count = -block_count
                block_size = decoder.read_long()
            for i in range(block_count):
                read_items.append(self.read_data(writers_schema.items,
                                                                                 readers_schema.items, decoder))
            block_count = decoder.read_long()
        return read_items

    def skip_array(self, writers_schema, decoder):
        block_count = decoder.read_long()
        while block_count != 0:
            if block_count < 0:
                block_size = decoder.read_long()
                decoder.skip(block_size)
            else:
                for i in range(block_count):
                    self.skip_data(writers_schema.items, decoder)
            block_count = decoder.read_long()

    def read_map(self, writers_schema, readers_schema, decoder):
        """
        Maps are encoded as a series of blocks.

        Each block consists of a long count value,
        followed by that many key/value pairs.
        A block with count zero indicates the end of the map.
        Each item is encoded per the map's value schema.

        If a block's count is negative,
        then the count is followed immediately by a long block size,
        indicating the number of bytes in the block.
        The actual count in this case
        is the absolute value of the count written.
        """
        read_items = {}
        block_count = decoder.read_long()
        while block_count != 0:
            if block_count < 0:
                block_count = -block_count
                block_size = decoder.read_long()
            for i in range(block_count):
                key = decoder.read_utf8()
                read_items[key] = self.read_data(writers_schema.values,
                                                        readers_schema.values, decoder)
            block_count = decoder.read_long()
        return read_items

    def skip_map(self, writers_schema, decoder):
        block_count = decoder.read_long()
        while block_count != 0:
            if block_count < 0:
                block_size = decoder.read_long()
                decoder.skip(block_size)
            else:
                for i in range(block_count):
                    decoder.skip_utf8()
                    self.skip_data(writers_schema.values, decoder)
            block_count = decoder.read_long()

    def read_union(self, writers_schema, readers_schema, decoder):
        """
        A union is encoded by first writing a long value indicating
        the zero-based position within the union of the schema of its value.
        The value is then encoded per the indicated schema within the union.
        """
        # schema resolution
        index_of_schema = int(decoder.read_long())
        if index_of_schema >= len(writers_schema.schemas):
            fail_msg = "Can't access branch index %d for union with %d branches"\
                                 % (index_of_schema, len(writers_schema.schemas))
            raise SchemaResolutionException(fail_msg, writers_schema, readers_schema)
        selected_writers_schema = writers_schema.schemas[index_of_schema]
        
        # read data
        return self.read_data(selected_writers_schema, readers_schema, decoder)

    def skip_union(self, writers_schema, decoder):
        index_of_schema = int(decoder.read_long())
        if index_of_schema >= len(writers_schema.schemas):
            fail_msg = "Can't access branch index %d for union with %d branches"\
                                 % (index_of_schema, len(writers_schema.schemas))
            raise SchemaResolutionException(fail_msg, writers_schema)
        return self.skip_data(writers_schema.schemas[index_of_schema], decoder)

    def read_record(self, writers_schema, readers_schema, decoder):
        """
        A record is encoded by encoding the values of its fields
        in the order that they are declared. In other words, a record
        is encoded as just the concatenation of the encodings of its fields.
        Field values are encoded per their schema.

        Schema Resolution:
         * the ordering of fields may be different: fields are matched by name.
         * schemas for fields with the same name in both records are resolved
             recursively.
         * if the writer's record contains a field with a name not present in the
             reader's record, the writer's value for that field is ignored.
         * if the reader's record schema has a field that contains a default value,
             and writer's schema does not have a field with the same name, then the
             reader should use the default value from its field.
         * if the reader's record schema has a field with no default value, and 
             writer's schema does not have a field with the same name, then the
             field's value is unset.
        """
        # schema resolution
        readers_fields_dict = readers_schema.fields_dict
        read_record = {}
        for field in writers_schema.fields:
            readers_field = readers_fields_dict.get(field.name)
            if readers_field is not None:
                field_val = self.read_data(field.type, readers_field.type, decoder)
                read_record[field.name] = field_val
            else:
                self.skip_data(field.type, decoder)

        # fill in default values
        if len(readers_fields_dict) > len(read_record):
            writers_fields_dict = writers_schema.fields_dict
            for field_name, field in readers_fields_dict.items():
                if field_name not in writers_fields_dict:
                    if field.has_default:
                        field_val = self._read_default_value(field.type, field.default)
                        read_record[field.name] = field_val
                    else:
                        fail_msg = 'No default value for field %s' % field_name
                        raise SchemaResolutionException(fail_msg, writers_schema,
                                                                                        readers_schema)
        return read_record

    def skip_record(self, writers_schema, decoder):
        for field in writers_schema.fields:
            self.skip_data(field.type, decoder)

    def _read_default_value(self, field_schema, default_value):
        """
        Basically a JSON Decoder?
        """
        if field_schema.type == 'null':
            return None
        elif field_schema.type == 'boolean':
            return bool(default_value)
        elif field_schema.type == 'int':
            return int(default_value)
        elif field_schema.type == 'long':
            return long(default_value)
        elif field_schema.type in ['float', 'double']:
            return float(default_value)
        elif field_schema.type in ['enum', 'fixed', 'string', 'bytes']:
            return default_value
        elif field_schema.type == 'array':
            read_array = []
            for json_val in default_value:
                item_val = self._read_default_value(field_schema.items, json_val)
                read_array.append(item_val)
            return read_array
        elif field_schema.type == 'map':
            read_map = {}
            for key, json_val in default_value.items():
                map_val = self._read_default_value(field_schema.values, json_val)
                read_map[key] = map_val
            return read_map
        elif field_schema.type in ['union', 'error_union']:
            return self._read_default_value(field_schema.schemas[0], default_value)
        elif field_schema.type == 'record':
            read_record = {}
            for field in field_schema.fields:
                json_val = default_value.get(field.name)
                if json_val is None: json_val = field.default
                field_val = self._read_default_value(field.type, json_val)
                read_record[field.name] = field_val
            return read_record
        else:
            fail_msg = 'Unknown type: %s' % field_schema.type
            raise schema.AvroException(fail_msg)


class SlowDatumWriter(object):
    """SlowDatumWriter for generic python objects."""
    def __init__(self, writers_schema=None):
        self._writers_schema = writers_schema

    # read/write properties
    def set_writers_schema(self, writers_schema):
        self._writers_schema = writers_schema
    writers_schema = property(lambda self: self._writers_schema,
                                                        set_writers_schema)

    def write(self, datum, encoder):
        # validate datum
        if not validate(self.writers_schema, datum):
            raise AvroTypeException(self.writers_schema, datum)
        self.write_data(self.writers_schema, datum, encoder)

    def write_data(self, writers_schema, datum, encoder):
        # function dispatch to write datum
        if writers_schema.type == 'null':
            encoder.write_null(datum)
        elif writers_schema.type == 'boolean':
            encoder.write_boolean(datum)
        elif writers_schema.type == 'string':
            encoder.write_utf8(datum)
        elif writers_schema.type == 'int':
            encoder.write_int(datum)
        elif writers_schema.type == 'long':
            encoder.write_long(datum)
        elif writers_schema.type == 'float':
            encoder.write_float(datum)
        elif writers_schema.type == 'double':
            encoder.write_double(datum)
        elif writers_schema.type == 'bytes':
            encoder.write_bytes(datum)
        elif writers_schema.type == 'fixed':
            self.write_fixed(writers_schema, datum, encoder)
        elif writers_schema.type == 'enum':
            self.write_enum(writers_schema, datum, encoder)
        elif writers_schema.type == 'array':
            self.write_array(writers_schema, datum, encoder)
        elif writers_schema.type == 'map':
            self.write_map(writers_schema, datum, encoder)
        elif writers_schema.type in ['union', 'error_union']:
            self.write_union(writers_schema, datum, encoder)
        elif writers_schema.type in ['record', 'error', 'request']:
            self.write_record(writers_schema, datum, encoder)
        else:
            fail_msg = 'Unknown type: %s' % writers_schema.type
            raise schema.AvroException(fail_msg)

    def write_fixed(self, writers_schema, datum, encoder):
        """
        Fixed instances are encoded using the number of bytes declared
        in the schema.
        """
        encoder.write(datum)

    def write_enum(self, writers_schema, datum, encoder):
        """
        An enum is encoded by a int, representing the zero-based position
        of the symbol in the schema.
        """
        index_of_datum = writers_schema.symbols.index(datum)
        encoder.write_int(index_of_datum)

    def write_array(self, writers_schema, datum, encoder):
        """
        Arrays are encoded as a series of blocks.

        Each block consists of a long count value,
        followed by that many array items.
        A block with count zero indicates the end of the array.
        Each item is encoded per the array's item schema.

        If a block's count is negative,
        then the count is followed immediately by a long block size,
        indicating the number of bytes in the block.
        The actual count in this case
        is the absolute value of the count written.
        """
        if len(datum) > 0:
            encoder.write_long(len(datum))
            for item in datum:
                self.write_data(writers_schema.items, item, encoder)
        encoder.write_long(0)

    def write_map(self, writers_schema, datum, encoder):
        """
        Maps are encoded as a series of blocks.

        Each block consists of a long count value,
        followed by that many key/value pairs.
        A block with count zero indicates the end of the map.
        Each item is encoded per the map's value schema.

        If a block's count is negative,
        then the count is followed immediately by a long block size,
        indicating the number of bytes in the block.
        The actual count in this case
        is the absolute value of the count written.
        """
        if len(datum) > 0:
            encoder.write_long(len(datum))
            for key, val in datum.items():
                encoder.write_utf8(key)
                self.write_data(writers_schema.values, val, encoder)
        encoder.write_long(0)

    def write_union(self, writers_schema, datum, encoder):
        """
        A union is encoded by first writing a long value indicating
        the zero-based position within the union of the schema of its value.
        The value is then encoded per the indicated schema within the union.
        """
        # resolve union
        for schema_index, candidate_schema in enumerate(writers_schema.schemas):
            if validate(candidate_schema, datum):
                break
        else:
            # nothing matched
            raise AvroTypeException(writers_schema, datum)

        # write data
        encoder.write_long(schema_index)
        self.write_data(candidate_schema, datum, encoder)

    def write_record(self, writers_schema, datum, encoder):
        """
        A record is encoded by encoding the values of its fields
        in the order that they are declared. In other words, a record
        is encoded as just the concatenation of the encodings of its fields.
        Field values are encoded per their schema.
        """
        for field in writers_schema.fields:
            self.write_data(field.type, datum.get(field.name), encoder)

# ===================


class FastDatumReader(object):
    """Deserialize Avro-encoded data into a Python data structure, Fast Version.

    Uses the new spavro C extension but maintains the old API.
    """
    @staticmethod
    def check_props(schema_one, schema_two, prop_list):
        for prop in prop_list:
            if getattr(schema_one, prop) != getattr(schema_two, prop):
                return False
        return True

    @staticmethod
    def match_schemas(writers_schema, readers_schema):
        w_type = writers_schema.type
        r_type = readers_schema.type
        if 'union' in [w_type, r_type] or 'error_union' in [w_type, r_type]:
            return True
        elif (w_type in schema.PRIMITIVE_TYPES and r_type in schema.PRIMITIVE_TYPES
                    and w_type == r_type):
            return True
        elif (w_type == r_type == 'record' and
                    FastDatumReader.check_props(writers_schema, readers_schema,
                                                                    ['fullname'])):
            return True
        elif (w_type == r_type == 'error' and
                    FastDatumReader.check_props(writers_schema, readers_schema,
                                                                    ['fullname'])):
            return True
        elif (w_type == r_type == 'request'):
            return True
        elif (w_type == r_type == 'fixed' and
                    FastDatumReader.check_props(writers_schema, readers_schema,
                                                                    ['fullname', 'size'])):
            return True
        elif (w_type == r_type == 'enum' and
                    FastDatumReader.check_props(writers_schema, readers_schema,
                                                                    ['fullname'])):
            return True
        elif (w_type == r_type == 'map' and
                    FastDatumReader.check_props(writers_schema.values,
                                                                    readers_schema.values, ['type'])):
            return True
        elif (w_type == r_type == 'array' and
                    FastDatumReader.check_props(writers_schema.items,
                                                                    readers_schema.items, ['type'])):
            return True

        # Handle schema promotion
        if w_type == 'int' and r_type in ['long', 'float', 'double']:
            return True
        elif w_type == 'long' and r_type in ['float', 'double']:
            return True
        elif w_type == 'float' and r_type == 'double':
            return True
        return False

    def __init__(self, writers_schema=None, readers_schema=None):
        """
        As defined in the Avro specification, we call the schema encoded
        in the data the "writer's schema", and the schema expected by the
        reader the "reader's schema".
        """
        self.schema_cache = {}
        if readers_schema:
            self.readers_schema = readers_schema
        if writers_schema:
            self.writers_schema = writers_schema
        if writers_schema and not readers_schema:
            self.readers_schema = writers_schema

    @property
    def writers_schema(self):
        return self._writers_schema

    @writers_schema.setter
    def writers_schema(self, parsed_writer_schema):
        '''Take a wrtiers schema object from the old avro API and run validation
        and schema reolution (if applicable)

        Since the old API would set this after object construction, we hook
        here so we can run schema validation and resolution once on assignment.
        This also makes the reader function for the resolved schema and uses
        that for data reading.
        '''
        self._writers_schema = parsed_writer_schema

        # create the reader function from the schema
        if hasattr(self, 'readers_schema'):
            # to_json is a terrible method name for something
            # that returns a python dict! :/
            resolved_schema = resolve(parsed_writer_schema.to_json(), self.readers_schema.to_json())
        else:
            # if no reader schema, then the resolved schema is just the writers
            # schema
            resolved_schema = parsed_writer_schema.to_json()
            self.readers_schema = parsed_writer_schema
        self.read_datum = get_reader(resolved_schema)

        # schema matching
        if not FastDatumReader.match_schemas(self.writers_schema, self.readers_schema):
            fail_msg = 'Schemas do not match.'
            raise SchemaResolutionException(fail_msg, self.writers_schema, self.readers_schema)

    def read(self, decoder):
        # keeping the API the same, we wrap the read_datum in read
        # and pass the underlying file object to the read_datum function
        return self.read_datum(decoder.reader)

    def read_data(self, writers_schema, readers_schema, decoder):
        resolved_schema = resolve(writers_schema.to_json(), readers_schema.to_json())
        # try:
        #     datum_reader = self.schema_cache[str(schema)]
        # except KeyError:
        datum_reader = get_reader(resolved_schema)
        # self.schema_cache[str(schema)] = datum_reader
        return datum_reader(decoder.reader)


class FastDatumWriter(object):
    """FastDatumWriter for generic python objects."""
    def __init__(self, writers_schema=None):
        self.writers_schema = writers_schema
        self.schema_cache = {}

    @property
    def writers_schema(self):
        return self._writers_schema

    @writers_schema.setter
    def writers_schema(self, parsed_writer_schema):
        self._writers_schema = parsed_writer_schema
        if parsed_writer_schema:
            # to_json is a terrible method name for something
            # that returns a python dict! :/
            self.write_datum = get_writer(parsed_writer_schema.to_json())

    def write(self, datum, encoder):
        # validate datum
        try:
            self.write_datum(encoder.writer, datum)
        except TypeError as ex:
            log.error(self.write_datum)
            log.exception("type error")
            raise AvroTypeException(self.writers_schema, datum)

    def write_data(self, schema, datum, encoder):
        try:
            datum_writer = self.schema_cache[str(schema)]
        except KeyError:
            datum_writer = get_writer(schema.to_json())
            self.schema_cache[str(schema)] = datum_writer
        datum_writer(encoder.writer, datum)

if use_fast:
    DatumReader = FastDatumReader
    DatumWriter = FastDatumWriter
    BinaryEncoder = FastBinaryEncoder
    BinaryDecoder = FastBinaryDecoder
else:
    DatumReader = SlowDatumReader
    DatumWriter = SlowDatumWriter
    BinaryEncoder = SlowBinaryEncoder
    BinaryDecoder = SlowBinaryDecoder