import random
from string import ascii_letters
import json


min_long = -(1 << 63)
max_long = (1 << 63) - 1

def random_boolean():
    return True if random.random() > 0.5 else False

def random_double():
    return random.random() + random.randint(-64000, 64000)

def random_long():
    return random.randint(min_long, max_long)

def random_string():
    return ''.join(random.choice(ascii_letters) for i in range(random.randint(1, 30)))

def null_generator():
    return None


generators = {
    'boolean': random_boolean,
    'double': random_double,
    'long': random_long,
    'string': random_string,
}

sample_unions = (["null", "long"],
    ["null", "string"],
    ["null", "boolean"],
    ["null", "double"])



def generate_random_schema(field_count):
    return {"type": "record", "name": "benchmark", "fields": [{"name": "field{}".format(idx), "type": random.choice(sample_unions), "default": "null"} for idx in range(field_count)]}


def generate_records(schema, record_count):
    synthetic_fields = [(field['type'], [generators[field['type'][1]], null_generator]) for field in schema['fields']]
    for i in range(record_count):
        yield {"field{}".format(idx): random.choice(field[1])() for idx, field in enumerate(synthetic_fields)}


def generate_sample_records(record_count):
    with open('sample_schema.avsc', 'r') as avro_schema:
        schema = json.loads(avro_schema.read())
    return schema, generate_records(schema, record_count)


def generate_random_records(field_count, record_count):
    schema = generate_random_schema(field_count)
    return schema, generate_records(schema, record_count)

