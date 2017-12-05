[![Spavro Build](https://travis-ci.org/pluralsight/spavro.svg?branch=master)](https://travis-ci.org/pluralsight/spavro)

# (Sp)eedier Avro - Spavro

Spavro is a fork of the [official Apache AVRO python 2 implementation](https://github.com/apache/avro) with the goal of greatly improving data read deserialization and write serialization performance.

Spavro is also python 2/3 compatible (instead of a spearate project / implementation). [Currently tested](https://travis-ci.org/pluralsight/spavro) using python 2.7, 3.3, 3.4, 3.5 and 3.6. Python versions before 3.3 are not supported due to the use of unicode literals and other compatibility features.

## Implementation Details

There are three primary differences between the official implementation and Spavro. First, Spavro uses a C extension, created with Cython, to accelerate some of the low level binary serialization logic. Additionally Spavro uses a different model for handling schemas. Spavro attemps to parse the write and read schemas _once_ and only _once_ and creates recursive reader/writer functions from the schema definition. These reader/writer functions encode the type structure of the schema so no additional lookups are necessary while processing data. The last difference is that Spavro has been updated to be both Python 2 and Python 3 compatible using the `six` library. The official apache AVRO implementation has two separate codebases for Python 2 and Python 3 and spavro only has one.

This has the net effect of greatly improving the throughput of reading and writing individual datums, since the schema isn't interrogated for every datum. This can be especially beneficial for "compatible" schema reading where both a read and write schema are needed to be able to read a complete data set.

## Performance

Some (very non-scientific) benchmarks on an arbitray data set (using my development laptop) show a 2-17x improvement in serialization / deserialization throughput. A simple test was run using ~135k relatively large (5k) real world records. YMMV but in all cases spavro was faster than both the default apache implementation and the "fastavro" library.

### deserialize 135k avro records
default implementation
360.23 records/s

fastavro library
2003.62 records/s

spavro
6521.34 records/s

### Serialize 135k avro records
default implementation
464.61 records / s

fastavro library
686.62 records / s

spavro library
4719.05 records / s


## API

Spavro keeps the default Apache library's API. This allows spavro to be a drop-in replacement for code using the existing Apache implementation. 

## Tests

Since the API matches the existing library, the majority of the existing Apache test suite is used to verify the correct operation of Spavro. Spavro adds some additional correctness tests to compare new vs old behaviors as well as some additional logic tests above and beyond the original library. Some of the java-based "map reduce" tests (specifically the tether tests) were removed because Spavro does not include the java code to implement that logic.

