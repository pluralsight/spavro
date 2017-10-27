# Copyright (C) 2017 Pluralsight LLC

import unittest
from spavro.schema_resolve import resolve
from spavro.exceptions import SchemaResolutionException


class TestResolver(unittest.TestCase):
    pass


pass_cases = (
("simple_null", "null", "null", "null"),
("simple_int", "int", "int", "int"),
("simple_long", "long", "long", "long"),
("simple_float", "float", "float", "float"),
("promote_int_to_long", "int", "long", "int"),
("promote_int_to_double", "int", "double", "int"),
("promote_float_to_double", "float", "double", "float"),
("promote_long_to_double", "long", "double", "long"),
("record_upgrade_to_union_and_default_field", {"fields": [{"default": "FOO",
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
                         "name": "Test"}],
    {'fields': [{"type": {"symbols": ["FOO", "BAR"],
                                              "type": "enum",
                                              "name": "F"},
                                     "name": "H"},
     {'type': {'type': 'default', 'value': 1234},
      'name': 'spork'}],
  'type': 'record',
  'name': 'Test'}),
("symbol_added_to_reader_enum",
 {"type": "enum", "name": "bigby", "symbols": ["A", "C"]},
 {"type": "enum", "name": "bigby", "symbols": ["A", "B", "C"]},
 {'symbols': ['A', 'C'], 'type': 'enum', 'name': 'bigby'}),
("array_items_upgraded_to_union",
    {"type": "array", "items": "string"},
    {"type": "array", "items": ["int", "string"]},
    {'items': 'string', 'type': 'array'})
)

exception_cases = (
("null_vs_int", "null", "int", SchemaResolutionException),
("boolean_vs_int", "boolean", "int", SchemaResolutionException),
("lower_precision_promote_long_int", "long", "int", SchemaResolutionException),
("lower_precision_promote_double_float", "double", "float", SchemaResolutionException),
("missing_symbol_in_read",
 {"type": "enum", "name": "bigby", "symbols": ["A", "C"]},
 {"type": "enum", "name": "bigby", "symbols": ["A", "B"]}, SchemaResolutionException),
("union_missing_write_schema",
    "int", ["string", "boolean"], SchemaResolutionException),
("record_names_dont_match",
    {"type": "record", "name": "my_name", "fields": [{"type": "int", "name": "A"}]},
    {"type": "record", "name": "not_my_name", "fields": [{"type": "int", "name": "A"}]},
    SchemaResolutionException),
("record_field_types_dont_match",
    {"type": "record", "name": "my_name", "fields": [{"type": "string", "name": "A"}]},
    {"type": "record", "name": "my_name", "fields": [{"type": "int", "name": "A"}]},
    SchemaResolutionException),
("record_new_field_no_default",
    {"type": "record", "name": "my_name", "fields": [{"type": "string", "name": "A"}]},
    {"type": "record", "name": "my_name", "fields": [{"type": "int", "name": "A"},
                                                     {"type": "int", "name": "B"}]},
    SchemaResolutionException)
)


def create_pass_case(writer, reader, expected):
    def resolve_write_reader(self):
        resolved = resolve(writer, reader)
        self.assertEqual(resolved, expected)
    return resolve_write_reader


def create_exception_case(writer, reader, exception):
    def resolve_write_reader(self):
        with self.assertRaises(exception) as context:
            resolved = resolve(writer, reader)
    return resolve_write_reader


def make_cases(cases):
    for name, writer, reader, expected in cases:
        test_method = create_pass_case(writer, reader, expected)
        test_method.__name__ = 'test_schema_resolution_{}'.format(name)
        setattr(TestResolver, test_method.__name__, test_method)


def make_exception_cases(cases):
    for name, writer, reader, expected in cases:
        test_method = create_exception_case(writer, reader, expected)
        test_method.__name__ = 'test_incompatible_schema_{}'.format(name)
        setattr(TestResolver, test_method.__name__, test_method)


make_cases(pass_cases)
make_exception_cases(exception_cases)
