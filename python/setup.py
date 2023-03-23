# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at

#   http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from setuptools import setup, find_packages, Extension
import os
from sedona import version

with open("README.md", "r") as fh:
    long_description = fh.read()

extension_args = {}

if os.getenv('ENABLE_ASAN'):
    extension_args = {
        'extra_compile_args': ["-fsanitize=address"],
        'extra_link_args': ["-fsanitize=address"]
    }

ext_modules = [
    Extension('sedona.utils.geomserde_speedup', sources=[
        'src/geomserde_speedup_module.c',
        'src/geomserde.c',
        'src/geom_buf.c',
        'src/geos_c_dyn.c'
    ], **extension_args)
]

setup(
    name='pineapple-spark',
    version=version,
    description='Pineapple is an extension of Apache Sedona for processing large-scale complex spatial queries',
    url='https://github.com/laila-abdelhafeez/pineapple',
    license="",
    author='Laila Abdelhafeez',
    author_email='labde005@ucr.edu',
    packages=find_packages(exclude=["*.tests", "*.tests.*", "tests.*", "tests"]),
    ext_modules=ext_modules,
    long_description=long_description,
    long_description_content_type="text/markdown",
    python_requires='>=3.6',
    install_requires=['attrs', "shapely>=1.7.0"],
    extras_require={"spark": ['pyspark>=2.3.0']},
    project_urls={
        'Documentation': 'https://https://github.com/laila-abdelhafeez/pineapple',
        'Source code': 'https://github.com/laila-abdelhafeez/pineapple',
        'Bug Reports': 'https://github.com/laila-abdelhafeez/pineapple'
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License"
    ]
)
