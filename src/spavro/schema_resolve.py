# Copyright (C) 2017 Pluralsight LLC

# resolve schemas
from spavro.fast_binary import get_type
from spavro.exceptions import SchemaResolutionException


def get_field_by_name(fields, name):
    '''Take a list of avro fields, scan the fields using the field name and
    return that field.'''
    field_names = [field['name'] for field in fields]
    return fields[field_names.index(name)]


def resolve_record(writer, reader):
    '''Take a writer and reader schema and return a 'meta' schema that allows
    transforming a previously written record into a new read structure.'''
    fields = []
    if writer['name'] != reader['name']:
        raise SchemaResolutionException("Schemas not compatible record names don't match")
    record_name = reader['name']
    optional = {}
    if "namespace" in writer and "namespace" in reader:
        optional["namespace"] = reader["namespace"]

    writer_fields = [field['name'] for field in writer['fields']]
    reader_fields = [field['name'] for field in reader['fields']]
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
    schema = {"type": "record", "fields": fields, "name": record_name}
    schema.update(optional)
    return schema


primitive_types = ('null', 'boolean', 'int', 'long', 'float', 'double', 'bytes', 'string' )


def resolve_array(writer, reader):
    '''Resolve a writer and reader array schema and recursively resolve the
    type for each array item schema'''
    return {'type': 'array', 'items': resolve(writer['items'], reader['items'])}


def resolve_map(writer, reader):
    '''Resolve a writer and reader map schema and resolve the type for the
    map's value schema'''
    return {'type': 'map', 'values': resolve(writer['values'], reader['values'])}


def resolve_enum(writer, reader):
    '''Compare a writer and reader enum and return a compatible enum'''
    if writer['name'] != reader['name']:
        raise SchemaResolutionException("Schemas not compatible, enum names don't match")
    if set(writer['symbols']) - set(reader['symbols']):
        raise SchemaResolutionException("Schemas not compatible, symbol in writer's enum not present in reader's enum")
    return {'type': 'enum', 'name': reader['name'], 'symbols': [symbol for symbol in writer['symbols']]}


def resolve_fixed(writer, reader):
    '''Take a fixed writer and reader schema and return the writers size value.
    '''
    if writer['name'] != reader['name'] or writer['size'] != reader['size']:
        raise SchemaResolutionException("Schemas not compatible, fixed names or sizes don't match")
    return {key: value for key, value in writer.items()}


def resolve_union(writer, reader):
    '''Take a writer union and a reader union, compare their types and return
    a read/write compatible union.

    A compatible read/write union has all of the writer's union schemas in the
    reader's schema.
    '''
    union = []
    for w_type in writer:
        for r_type in reader:
            try:
                merged = resolve(w_type, r_type)
                union.append(merged)
                break
            except SchemaResolutionException:
                # keep trying until we iterate through all read types
                continue
        else:
            # none of the read types matched the write type, this is an error
            raise SchemaResolutionException("Schema in writer's union not present in reader's union.")
    return union


promotable = ['int', 'long', 'float', 'double']


def resolve(writer, reader):
    '''Take a writer and a reader schema and return a meta schema that
    translates the writer's schema to the reader's schema.

    This handles skipping missing fields and default fills by creating
    non-standard 'types' for reader creation. These non-standard types are
    never surfaced out since they're not standard avro types but just used
    as an implementation detail for generating a write-compantible reader.'''
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
        if reader_type == "fixed":
            return resolve_fixed(writer, reader)
    else:
        # see if we've 'upgraded' to a union
        if reader_type == 'union':
            # if the writer type is in the reader's union
            # then just return the writer's schema
            if writer_type in [get_type(r) for r in reader]:
                type_index = [get_type(r) for r in reader].index(writer_type)
                return resolve(writer, reader[type_index])
            else:
                raise SchemaResolutionException("Writer schema not present in reader union")
        if writer_type in promotable and reader_type in promotable and promotable.index(writer_type) < promotable.index(reader_type):
            return writer
        raise SchemaResolutionException("Reader and Writer schemas are incompatible")
