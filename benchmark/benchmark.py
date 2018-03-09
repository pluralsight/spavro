#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import sys
import random
import avro.schema
import avro.io
import spavro.schema
import spavro.io
import io
from timeit import default_timer as timer
from fastavro import schemaless_writer, schemaless_reader
from synthetic_records import generate_sample_records, generate_random_records
from synthetic_avro import create_avro


class ByteStream(io.BytesIO):
    '''Create a context managed bytesIO object'''
    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
        return False


def make_spavro_reader(schema):
    parsed_schema = spavro.schema.parse(json.dumps(schema))
    reader = spavro.io.DatumReader(parsed_schema)
    def read_func(data):
        bytes_reader = io.BytesIO(data)
        decoder = spavro.io.BinaryDecoder(bytes_reader)
        return reader.read(decoder)
    return read_func


def make_avro_reader(schema):
    if sys.version_info >= (3, 0):
        # why did they change it from parse to Parse in py3? huh?
        parsed_schema = avro.schema.Parse(json.dumps(schema))
    else:
        parsed_schema = avro.schema.parse(json.dumps(schema))
    reader = avro.io.DatumReader(parsed_schema)
    def read_func(data):
        bytes_reader = io.BytesIO(data)
        decoder = avro.io.BinaryDecoder(bytes_reader)
        return reader.read(decoder)
    return read_func


def make_fastavro_reader(schema):
    def read_func(data):
        buffer = io.BytesIO(data)
        return schemaless_reader(buffer, schema)
    return read_func


def make_avro_writer(schema, output):
    if sys.version_info >= (3, 0):
        # why did they change it from parse to Parse in py3? huh?
        parsed_schema = avro.schema.Parse(json.dumps(schema))
    else:
        parsed_schema = avro.schema.parse(json.dumps(schema))
    writer = avro.io.DatumWriter(parsed_schema)
    encoder = avro.io.BinaryEncoder(output)
    def write_func(datum):
        writer.write(datum, encoder)
    return write_func


def make_spavro_writer(schema, output):
    parsed_schema = spavro.schema.parse(json.dumps(schema))
    writer = spavro.io.DatumWriter(parsed_schema)
    encoder = spavro.io.BinaryEncoder(output)
    def write_func(datum):
        writer.write(datum, encoder)
    return write_func


def make_fastavro_writer(schema, output):
    def write_func(datum):
        schemaless_writer(output, schema, datum)
    return write_func


def test_serializer(name, write, test_array):
    start_time = timer()
    record_count = len(test_array)
    for idx, record in enumerate(test_array):
        write(record)
    total_time = timer() - start_time
    print("{}: {:.2f} records/sec".format(name, record_count / total_time))


def test_deserializer(name, read, test_array):
    start_time = timer()
    record_count = len(test_array)
    for idx, record in enumerate(test_array):
        read(record)
    total_time = timer() - start_time
    print("{}: {:.2f} records/sec".format(name, record_count / total_time))


def test_serializers(iterations=1, record_count=1000):
    random.seed("ALWAYSTHESAME")
    print("Generating sample records to serialize")
    # schema, test_array_generator = generate_sample_records(record_count)
    schema, test_array_generator = generate_random_records(30, record_count)
    test_array = list(test_array_generator)
    print("Let the games begin!")
    for i in range(iterations):
        print("Run #{}".format(i+1))
        for name, writer in [("Avro write", make_avro_writer(schema, io.BytesIO())),
                             ("Fastavro write", make_fastavro_writer(schema, io.BytesIO())),
                             ("Spavro write", make_spavro_writer(schema, io.BytesIO()))]:
            test_serializer(name, writer, test_array)
        print("-"*40)


def test_deserializers(iterations=1, record_count=1000):
    random.seed("ALWAYSTHESAME")
    print("Generating sample avro to deserialize")
    # schema, test_array_generator = generate_sample_records(record_count)
    schema, test_array_generator = generate_random_records(30, record_count)
    test_array = create_avro(schema, test_array_generator)
    print("Let the games begin!")
    for i in range(iterations):
        print("Run #{}".format(i+1))
        for name, reader in [("Avro read", make_avro_reader(schema)),
                             ("Fastavro read", make_fastavro_reader(schema)),
                             ("Spavro read", make_spavro_reader(schema))]:
            test_serializer(name, reader, test_array)
        print("-"*40)

test_serializers(5, 50000)
test_deserializers(5, 50000)