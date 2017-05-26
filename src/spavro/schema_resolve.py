# resolve schemas
from fast_binary import get_type
# from io import SchemaResolutionException
from exceptions import SchemaResolutionException


def get_field_by_name(fields, name):
    field_names = [field['name'] for field in fields]
    return fields[field_names.index(name)]


def resolve_record(writer, reader):
    fields = []
    if writer['name'] != reader['name']:
        raise SchemaResolutionException("Schemas not compatible record names don't match")
    record_name = reader['name']

    writer_fields = [field['name'] for field in writer['fields']]
    reader_fields = [field['name'] for field in reader['fields']]
    # check the same names and the same order
    # if writer_fields == reader_fields:
    #     for field in writer_fields:
    #         return writer['fields']
    # check for defaults for records that are in reader
    # but not in writer and vice versa
    reader_but_not_writer = (set(reader_fields) - set(writer_fields))
    writer_but_not_reader = (set(writer_fields) - set(reader_fields))
    both_reader_and_writer = (set(writer_fields) & set(reader_fields))
    # run through the fields in writer order
    for field in writer['fields']:
        if field['name'] in both_reader_and_writer:
            fields.append({"name": field['name'], "type": resolve(field['type'],
                get_field_by_name(reader['fields'], field['name'])['type'])})
        elif field['name'] in writer_but_not_reader:
            ### special skip type record
            fields.append({"name": field['name'], "type": {"type": "skip", "value": field['type']}})

    for field in reader['fields']:
        if field['name'] in reader_but_not_writer:
            try:
                fields.append({"name": field['name'], "type": {"type": "default", "value": field['default']}})
            except KeyError:
                raise SchemaResolutionException("Schemas not compatible, no default value for field in reader's record that's not present in writer's record")
    return {"type": "record", "fields": fields}  # writer, reader


primitive_types = ('null', 'boolean', 'int', 'long', 'float', 'double', 'bytes', 'string' )


def resolve_array(writer, reader):
    return {'type': 'array', 'items': resolve(writer['items'], reader['items'])}


def resolve_map(writer, reader):
    return {'type': 'map', 'values': resolve(writer['values'], reader['values'])}


def resolve_enum(writer, reader):
    if writer['name'] != reader['name']:
        raise SchemaResolutionException("Schemas not compatible, enum names don't match")
    if set(writer['symbols']) - set(reader['symbols']):
        raise SchemaResolutionException("Schemas not compatible, symbol in writer's enum not present in reader's enum")
    return {'type': 'enum', 'name': reader['name'], 'symbols': [symbol for symbol in writer['symbols']]}


def resolve_union(writer, reader):
    union = []
    for w_type in writer:
        for r_type in reader:
            try:
                merged = resolve(w_type, r_type)
                union.append(w_type)
                break
            except SchemaResolutionException:
                continue
        else:
            raise SchemaResolutionException("Schema in writer's union not present in reader's union.")
    return union


promotable = ['int', 'long', 'float', 'double']


def resolve(writer, reader):
    writer_type = get_type(writer)
    reader_type = get_type(reader)

    if writer_type == reader_type:
        if reader_type in primitive_types:
            return reader
        if reader_type == 'array':
            return resolve_array(writer, reader)
        if reader_type == 'map':
            return resolve_map(writer, reader)
        if reader_type == 'enum':
            return resolve_enum(writer, reader)
        if reader_type == 'union':
            return resolve_union(writer, reader)
        if reader_type == "record":
            return resolve_record(writer, reader)
    else:
        # see if we've 'upgraded' to a union
        if reader_type == 'union':
            # if the writer type is in the reader's union
            # then jsut return the writer's schema
            if writer_type in [get_type(r) for r in reader]:
                type_index = [get_type(r) for r in reader].index(writer_type)
                return resolve(writer, reader[type_index])
            else:
                raise SchemaResolutionException("Writer schema not present in reader union")
        if writer_type in promotable and reader_type in promotable and promotable.index(writer_type) < promotable.index(reader_type):
            return writer
        raise SchemaResolutionException("Reader and Writer schemas are incompatible")



if __name__ == "__main__":
    test_schemas = [
({"fields": [{"default": "FOO",
                                 "type": {"symbols": ["FOO", "BAR"],
                                          "namespace": "",
                                          "type": "enum",
                                          "name": "F"},
                                 "name": "H"}
                                 ],
                     "type": "record",
                     "name": "Test"},
["int",
 {"fields": [{"default": "FOO",
                                 "type": {"symbols": ["FOO", "BAR"],
                                          "namespace": "",
                                          "type": "enum",
                                          "name": "F"},
                                 "name": "H"},
                                 {"name": "spork",
                                  "type": "int",
                                  "default": 1234}
                                 ],
                     "type": "record",
                     "name": "Test"}]),

({"type": "enum", "name": "bigby", "symbols": ["A", "C"]},
 {"type": "enum", "name": "bigby", "symbols": ["A", "B", "C"]}),

({"type": "array", "items": "string"},
{"type": "array", "items": ["int", "string"]}),

({"fields": [{"default": "FOO",
                                 "type": {"symbols": ["FOO", "BAR"],
                                          "namespace": "",
                                          "type": "enum",
                                          "name": "F"},
                                 "name": "H"}
                                 ],
                     "type": "record",
                     "name": "Test"},
 {"fields": [{"name": "spork",
                                  "type": "int",
                                  "default": 1234}
                                 ],
                     "type": "record",
                     "name": "Test"}),
]

    def test(writer, reader):
        return resolve(writer, reader)

    from pprint import pprint
    for writer, reader in test_schemas:
        pprint(writer)
        pprint(reader)
        pprint("-"*10)
        pprint(test(writer, reader))
        pprint("="*20)
