#!/usr/bin/env python

import sys
from distutils.core import setup

required = []

setup(
    name = 'splitfs',
    version = '0.0.1',
    description = 'Split/unsplit files transparently',
    author = 'Manfred Touron',
    author_email = 'm-splitfs@42.am',
    license='MIT',
    url = 'https://github.com/moul/splitfs',
    py_modules = ['splitfs.SplitFS'],
    scripts = ['splitfs-mount','splitfs-umount'],
    install_requires = ['python-fuse>=0.2']
    )
