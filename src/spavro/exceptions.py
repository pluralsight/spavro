# Modifications copyright (C) 2017 Pluralsight LLC

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
