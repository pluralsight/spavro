

PRIMITIVE = (
    u'null',
    u'boolean',
    u'string',
    u'bytes',
    u'int',
    u'long',
    u'float',
    u'double',
)


class Schema(object):
    def to_json(self):
        raise NotImplemented()

    def __str__(self):
        return str(self.type)

    def __repr__(self):
        return "<{} type='{}'>".format(self.__class__.__name__, self)


class PrimitiveSchema(Schema):
    def __init__(self, schema_name):
        self.type = schema_name


class RecordField(object):
    def __init__(self, fielddef):
        self.name = fielddef['name']
        self.type = parse_schema(fielddef['type'])

    def __str__(self):
        return str(self.type)

    def __repr__(self):
        return "<{} type='{}'>".format(self.__class__.__name__, self)


class RecordSchema(Schema):
    def __init__(self, schema):
        self.name = schema['name']
        self.type = schema['type']
        self.fields = [RecordField(field) for field in schema['fields']]


class UnionSchema(Schema):
    def __init__(self, schemas, names=None):
        self.type = 'union'
        self.schemas = [parse_schema(schema, names) for schema in schemas]


class EnumSchema(Schema):
    def __init__(self, schema):
        self.type = 'enum'
        self.symbols = schema['symbols']
        self.name = schema.get('name', None)


class ArraySchema(Schema):
    def __init__(self, schema):
        raise NotImplemented()


class MapSchema(Schema):
    def __init__(self, schema):
        raise NotImplemented()


class FixedSchema(Schema):
    def __init__(self, schema):
        raise NotImplemented()


# all complex types are represented by dictionaries
complex_types = {
    'record': RecordSchema,
    'enum': EnumSchema,
    'array': ArraySchema,
    'map': MapSchema,
    'fixed': FixedSchema
}


def parse_schema(schema, names=None):
    if type(schema) is list:
        return UnionSchema(schema)
    elif type(schema) is dict:
        if schema['type'] in complex_types:
            return complex_types[schema['type']](schema)
        elif schema['type'] in PRIMITIVE:
            # could add if 'logicalType' in schema as a double guard
            # this handles annotated schemas and logical types
            # ignores everything else in the dictionary
            return parse_schema(schema['type'])
    elif schema in PRIMITIVE:
        return PrimitiveSchema(schema)

    raise Exception("Invalid schema: {}".format(schema))
