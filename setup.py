#! /usr/bin/env python

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

install_requires = ['six>=1.10.0']
if version_info[:2] <= (2, 5):
    install_requires.append('simplejson >= 2.0.9')

ext = '.pyx' if USE_CYTHON else '.c'
extensions = [Extension("spavro.fast_binary", sources=["src/spavro/fast_binary" + ext])]

if USE_CYTHON:
    extensions = cythonize(extensions)

setup(
  name='spavro',
  version='1.1.0',
  packages=['spavro'],
  package_dir={'': 'src'},
  # scripts=["./scripts/avro"],
  include_package_data=True,
  package_data={'spavro': ['LICENSE.txt', 'NOTICE.txt']},
  # Project uses simplejson, so ensure that it gets installed or upgraded
  # on the target machine
  install_requires=install_requires,
  # spavro C extensions
  ext_modules=extensions,
  # metadata for upload to PyPI
  author='Michael Kowalchik',
  author_email='mikepk@pluralsight.com',
  description='Spavro is a (sp)eedier avro library -- Spavro is a fork of the official Apache AVRO python 2 implementation with the goal of greatly improving data read deserialization and write serialization performance.',
  license='Apache License 2.0',
  keywords='avro serialization rpc data',
  url='http://github.com/pluralsight/spavro',
  extras_require={
    'snappy': ['python-snappy'],
  },
  classifiers=[
            "Development Status :: 4 - Beta",
            "Intended Audience :: Developers",
            "License :: OSI Approved :: Apache Software License",
            "Programming Language :: Python",
            "Programming Language :: Python :: 2.7",
            "Programming Language :: Python :: 3.5",
            "Topic :: Software Development :: Libraries",
            "Topic :: System :: Networking",
            "Operating System :: OS Independent",
        ],
  tests_require=['nose'],
  test_suite='nose.collector'
)
