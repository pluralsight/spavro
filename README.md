# (Sp)eedier Avro - Spavro

Spavro is a fork of the official Apache AVRO python 2 implementation with the goal of greatly improving data read and write serialization performance.

Spavro is also python 2/3 compatible (instead of a spearate project / implementation). Tested using py2.7 and py3.5.


## Implementation Details

There are two primary differences between the official implementation and Spavro. First, Spavro uses a C extension, created with Cython, to accelerate some of the low level binary serialization logic. Additionally Spavro uses a different model for handling schemas. Spavro attemps to parse the write and read schemas _once_ and only _once_ and creates recursive reader/writer functions from the schema definition. These reader/writer functions encode the type structure of the schema so no additional lookups are necessary while processing data.

This has the net effect of greatly improving the throughput of reading and writing individual datums, since the schema isn't interrogated for every datum.

## Performance

Some (very non-scientific) benchmarks on an arbitray data set (using my development laptop) show a 5-17x improvement in serialization / deserialization throughput. A simple test was run using ~135k relatively large real world records.

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

All attempts were made to externally mimic the default library's API. This was done so that Spavro could be used as a drop-in replacement for the official implementation.

## Tests

Since the API matches the existing library, the majority of the existing test suite is used to verify the correct operation of Spavro. Spavro adds some additional correctness tests to compare new vs old behaviors as well as some additional logic tests above and beyond the original library. Some of the java-based "map reduce" tests (specifically the tether tests) were removed because Spavro does not include the java code to implement that logic.

