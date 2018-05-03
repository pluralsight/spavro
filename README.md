[![Spavro Build](https://travis-ci.org/pluralsight/spavro.svg?branch=master)](https://travis-ci.org/pluralsight/spavro)

# (Sp)eedier Avro - Spavro

Spavro is a fork of the [official Apache AVRO python 2 implementation](https://github.com/apache/avro) with the goal of greatly improving data read deserialization and write serialization performance.

Spavro is also python 2/3 compatible (instead of a spearate project / implementation). [Currently tested](https://travis-ci.org/pluralsight/spavro) using python 2.7, 3.3, 3.4, 3.5 and 3.6. Python versions before 3.3 are not supported due to the use of unicode literals and other compatibility features.

## Implementation Details

There are three primary differences between the official implementation and Spavro. First, Spavro uses a C extension, created with Cython, to accelerate some of the low level binary serialization logic. Additionally Spavro uses a different model for handling schemas. Spavro attemps to parse the write and read schemas _once_ and only _once_ and creates recursive reader/writer functions from the schema definition. These reader/writer functions encode the type structure of the schema so no additional lookups are necessary while processing data. The last difference is that Spavro has been updated to be both Python 2 and Python 3 compatible using the `six` library. The official apache AVRO implementation has two separate codebases for Python 2 and Python 3 and spavro only has one.

This has the net effect of greatly improving the throughput of reading and writing individual datums, since the schema isn't interrogated for every datum. This can be especially beneficial for "compatible" schema reading where both a read and write schema are needed to be able to read a complete data set.

## Performance / Benchmarks


### Results

These tests were run using an AWS `m4.large` instance running CentOS 7. They were run with the following versions: `avro-python3==1.8.2`, `fastavro==0.17.9`, `spavro==1.1.10`. Python `3.6.4` was used for the python 3 tests.

The TLDR is that spavro has *14-23x* the throughput of the default Apache avro implementation and *2-4x* the throughput of the fastavro library (depending on the shape of the records).

### Deserialize avro records (read)


Records per second read:

![Read, 1 field, records per sec](https://github.com/pluralsight/spavro/raw/master/benchmark/results/read_1field_rec_per_sec.png?raw=true "Read, 1 field, records per sec")
![Read, 500 fields, records per sec](https://github.com/pluralsight/spavro/raw/master/benchmark/results/read_500field_rec_per_sec.png?raw=true "Read, 500 fields, records per sec")

Datums per second (individual fields) read:

![Read, fields per second](https://github.com/pluralsight/spavro/raw/master/benchmark/results/read_datum_per_sec.png?raw=true "Read, fields per second")

### Serialize avro records (write)


Records per second write:

![Write, 1 field, records per sec](https://github.com/pluralsight/spavro/raw/master/benchmark/results/write_1field_rec_per_sec.png?raw=true "Write, 1 field, records per sec")
![Write, 500 fields, records per sec](https://github.com/pluralsight/spavro/raw/master/benchmark/results/write_500field_rec_per_sec.png?raw=true "Write, 500 fields, records per sec")

Datums per second (individual fields) write:

![Write, fields per second](https://github.com/pluralsight/spavro/raw/master/benchmark/results/write_datum_per_sec.png?raw=true "Write, fields per second")


### Methodology

Benchmarks were performed with the `benchmark.py` script in the `/benchmarks` path in the repository (if you'd like to run your own tests).

Many of the records that led to the creation of spavro were of the form `{"type": "record", "name": "somerecord", "fields": [1 ... n fields usually with a type of the form of a union of ['null' and a primitive type]]}` so the benchmarks were created to simulate that type of record structure. I believe this is a _very_ common use case for avro so the benchmarks were created around this pattern.

The benchmark creates a random schema of a record with a mix of string, double, long and boolean types and a random record generator to test that schema. The pseudo-random generator is seeded with the same string to make the results deterministic (but with varied records). The number of fields in the record was varied from one to 500 and the performance of the avro implementations were tested for each of the cases.

The serializer and deserializer benchmarks create an array of simulated records in memory and then attempts to process them using the three different implementation as quickly as possible. This means the max working size is limited to memory (a combination of the number of records and the number of fields in the simulated record). For these benchmarks 5m datums were processed for each run (divided by the number of fields in each record).

Each run of the schema/record/implementation was repeated ten times and the time to complete was averaged.


## API

Spavro keeps the default Apache library's API. This allows spavro to be a drop-in replacement for code using the existing Apache implementation.

## Tests

Since the API matches the existing library, the majority of the existing Apache test suite is used to verify the correct operation of Spavro. Spavro adds some additional correctness tests to compare new vs old behaviors as well as some additional logic tests above and beyond the original library. Some of the java-based "map reduce" tests (specifically the tether tests) were removed because Spavro does not include the java code to implement that logic.

