#!/usr/bin/env python

from os import path

from setuptools import setup, find_packages

this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='target-snowflake',
    url='https://github.com/datamill-co/target-snowflake',
    author='datamill',
    version="0.2.4",
    description='Singer.io target for loading data into Snowflake',
    long_description=long_description,
    long_description_content_type='text/markdown',
    classifiers=['Programming Language :: Python :: 3 :: Only'],
    py_modules=['target_snowflake'],
    install_requires=[
        'singer-python==5.9.0',
        'singer-target-postgres==0.2.4',
        'target-redshift==0.2.4',
        'botocore<1.13.0,>=1.12.253',
        'snowflake-connector-python==2.2.5'
    ],
    setup_requires=[
        "pytest-runner"
    ],
    extras_require={
        'tests': [
            "chance==0.110",
            "Faker==4.0.3",
            "pytest==5.4.1"
        ]},
    entry_points='''
      [console_scripts]
      target-snowflake=target_snowflake:cli
    ''',
    packages=find_packages()
)
