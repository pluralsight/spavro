Spavro Changelog
-----------------

1.1.8, 1.1.9, 1.1.10 - Mar 19, 2018
============================

- Fix bug with C implementation of zig zag decoder. Additional unnecessary cast was clipping during the bit shifting for larger numbers.
- Skipping 1.1.8 and 1.1.9 was missing C cythonized code and created incompatibilities with python 2.7

1.1.7 - Mar 6, 2018
===================

- Fix bug with 'bytes' type in union schemas failing to parse

1.1.6 - Jan 17, 2018
====================

- Fix bug with reference types (named references) inside unions

1.1.5 - Jan 4, 2018
===================

- Remove accidental debug loglevel logging directive

1.1.4 - Dec 22, 2017
====================

- Add more helpful exception messages (mainly for Python 3 with chained exceptions) that will describe which field in a record datum failed and when ints and strings mismatch, show the datum and the schema.
- Fix some old non-py3 incompatible utility code to be py2/py3

1.1.3 - Dec 4, 2017
===================

- Fix source distribution Cython file inclusion ([pull request](https://github.com/pluralsight/spavro/pull/2))

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
