#!/usr/bin/env python

from setuptools import setup, find_packages
from codecs import open

with open('README.md', 'r', 'utf-8') as fd:
    long_description = fd.read()

setup(
    name='pbzlib',
    version='0.20191031',
    description='Library for serializing a list of protobuf objects',
    long_description=long_description,
    url='https://github.com/fabgeyer/pbzlib',
    author='fabgeyer',
    license='MIT',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Environment :: Console',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: BSD License',
    ],
    keywords='protobuf',
    packages=find_packages(),
    install_requires=['protobuf'],
)
