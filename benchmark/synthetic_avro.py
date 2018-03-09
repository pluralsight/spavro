import sys
import io
import spavro.schema
import spavro.io
import json


class ByteStream(io.BytesIO):
    '''Create a context managed bytesIO object'''
    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
        return False


def make_record_serializer(schema):
    parsed_schema = spavro.schema.parse(json.dumps(schema))
    writer = spavro.io.DatumWriter(parsed_schema)
    def write_func(datum):
        with ByteStream() as output:
            encoder = spavro.io.BinaryEncoder(output)
            writer.write(datum, encoder)
            return output.getvalue()
    return write_func

def create_avro(schema, records):
    avro_encode = make_record_serializer(schema)
    return [avro_encode(record) for record in records]
