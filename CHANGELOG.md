Spavro Changelog
-----------------

1.1.3 - Dec 4, 2017
===================

- Fix source distribution Cython file inclusion (pull request)

1.1.2 - Nov 14, 2017
====================

- Add more type checking in the serializer. Some fast data types were leading to spavro not rejecting bad data.
- Add tests to verify that invalid (no schema conforming data) is rejected

1.1.1 - Oct 31, 2017
====================

- Fix bug with Enum adding it to the named types that can be namespaced.
- Fix bug with 32bit systems that could potentially trucate long data at 2^31 bits

1.1.0 - June 20, 2017
=====================

- Add code to support pickling spavro records. This allows the use of spavro in contexts like Spark that need to serialize the data to be shipped around.

1.0.0 - June 7, 2017
====================

- First release of spavro, speedier avro for python!
