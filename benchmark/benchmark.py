#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import sys
import random
from collections import defaultdict
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


def time_serdes(name, test_func, test_array):
    start_time = timer()
    record_count = len(test_array)
    for idx, record in enumerate(test_array):
        test_func(record)
    total_time = timer() - start_time
    return record_count, total_time


def create_write_records(field_count, record_count):
    print("Generating sample records to serialize")
    # schema, test_array_generator = generate_sample_records(record_count)
    schema, test_array_generator = generate_random_records(field_count, record_count)
    test_array = list(test_array_generator)
    return schema, test_array


def create_read_records(field_count, record_count):
    print("Generating sample avro to deserialize")
    # schema, test_array_generator = generate_sample_records(record_count)
    schema, test_array_generator = generate_random_records(field_count, record_count)
    test_array = create_avro(schema, test_array_generator)
    return schema, test_array


def run_benchmarks(number_of_iterations=5):
    random.seed("ALWAYSTHESAME")
    results = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    # field count, record count tuples
    test_set = ((1, 5000000), (5, 1000000), (10, 500000), (50, 100000), (100, 50000), (500, 10000))

    for field_count, record_count in test_set:
        schema, test_data = create_read_records(field_count, record_count)
        # read
        read_functions = [("Avro", make_avro_reader(schema)),
                          ("Fastavro", make_fastavro_reader(schema)),
                          ("Spavro", make_spavro_reader(schema))]

        for name, reader in read_functions:
            for i in range(number_of_iterations):
                print("Run #{}".format(i+1))
                record_count, total_time = time_serdes(name, reader, test_data)
                results["read"][(field_count, record_count)][name].append(total_time)
                print("{}: {:.2f} records/sec".format(name, record_count / total_time))

        schema, test_data = create_write_records(field_count, record_count)
        # write
        write_functions = [("Avro", make_avro_writer(schema, io.BytesIO())),
                           ("Fastavro", make_fastavro_writer(schema, io.BytesIO())),
                           ("Spavro", make_spavro_writer(schema, io.BytesIO()))]
        for name, writer in write_functions:
            for i in range(number_of_iterations):
                print("Run #{}".format(i+1))
                record_count, total_time = time_serdes(name, writer, test_data)
                results["write"][(field_count, record_count)][name].append(total_time)
                print("{}: {:.2f} records/sec".format(name, record_count / total_time))
    return results


if __name__ == "__main__":
    benchmark_results = run_benchmarks()
    with open("benchmark_results.json", "w") as bmark_file:
        bmark_file.write(json.dumps(benchmark_results))
