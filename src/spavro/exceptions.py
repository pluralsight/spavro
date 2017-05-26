from spavro import schema
import json


class AvroTypeException(schema.AvroException):
    """Raised when datum is not an example of schema."""
    def __init__(self, expected_schema, datum):
        pretty_expected = json.dumps(json.loads(str(expected_schema)), indent=2)
        fail_msg = "The datum %s is not an example of the schema %s"\
                             % (datum, pretty_expected)
        schema.AvroException.__init__(self, fail_msg)


class SchemaResolutionException(schema.AvroException):
    def __init__(self, fail_msg, writers_schema=None, readers_schema=None):
        if writers_schema:
            pretty_writers = json.dumps(json.loads(str(writers_schema)), indent=2)
            fail_msg += "\nWriter's Schema: %s" % pretty_writers
        if readers_schema:
            pretty_readers = json.dumps(json.loads(str(readers_schema)), indent=2)
            fail_msg += "\nReader's Schema: %s" % pretty_readers
        schema.AvroException.__init__(self, fail_msg)
