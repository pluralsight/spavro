#! /usr/bin/env python

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
try:
  from setuptools import setup
except ImportError:
  from distutils.core import setup
from sys import version_info
from distutils.extension import Extension
try:
    from Cython.Build import cythonize
    USE_CYTHON = True
except ImportError:
    USE_CYTHON = False

install_requires = []
if version_info[:2] <= (2, 5):
    install_requires.append('simplejson >= 2.0.9')

ext = '.pyx' if USE_CYTHON else '.c'
extensions = [Extension("spavro.fast_binary", ["src/spavro/fast_binary" + ext])]

import sys
if USE_CYTHON:
    sys.stderr.write("CYTHONIZING...\n")
    extensions = cythonize(extensions)

# raise Exception("WHAT THE FRIG")
setup(
  name='spavro',
  version='2.0',
  packages=['spavro'],
  package_dir={'': 'src'},
  scripts=["./scripts/avro"],

  #include_package_data=True,
  package_data={'spavro': ['LICENSE', 'NOTICE']},

  # Project uses simplejson, so ensure that it gets installed or upgraded
  # on the target machine
  install_requires=install_requires,

  # fast avro code
  ext_modules=extensions,
  # metadata for upload to PyPI
  author='Michael Kowalchik',
  author_email='mikepk@pluralsight.com',
  description='Spavro is a (sp)eedier avro serialization and RPC framework.',
  license='Apache License 2.0',
  keywords='avro serialization rpc',
  url='http://avro.apache.org/',
  extras_require={
    'snappy': ['python-snappy'],
  },
)
