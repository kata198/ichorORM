#!/usr/bin/env python
#
# Copyright (c) 2016-2018 Timothy Savannah under terms of LGPLv2.1
# You should have received a copy of this with this distribution as "LICENSE"
#


#vim: set ts=4 sw=4 expandtab

import os
import sys
from setuptools import setup


if __name__ == '__main__':
 

    dirName = os.path.dirname(__file__)
    if dirName and os.getcwd() != dirName:
        os.chdir(dirName)

    requires = ['psycopg2']

    summary = 'A python library for postgresql focused on performance and supporting ORM and query-building functionality'

    try:
        with open('README.rst', 'rt') as f:
            long_description = f.read()
    except Exception as e:
        sys.stderr.write('Exception when reading long description: %s\n' %(str(e),))
        long_description = summary

    setup(name='ichorORM',
            version='2.0.2',
            packages=['ichorORM'],
            author='Tim Savannah',
            author_email='kata198@gmail.com',
            maintainer='Tim Savannah',
            requires=requires,
            install_requires=requires,
            url='https://github.com/kata198/ichorORM',
            maintainer_email='kata198@gmail.com',
            description=summary,
            long_description=long_description,
            license='LGPLv2',
            keywords=['python', 'orm', 'postgres', 'postgresql', 'query', 'build', 'select', 'update', 'delete', 'model', 'psycopg', 'psycopg2'],
            classifiers=['Development Status :: 4 - Beta',
                         'Programming Language :: Python',
                         'License :: OSI Approved :: GNU Lesser General Public License v2 (LGPLv2)',
                         'Programming Language :: Python :: 2',
                          'Programming Language :: Python :: 2',
                          'Programming Language :: Python :: 2.7',
                          'Programming Language :: Python :: 3',
                          'Programming Language :: Python :: 3.3',
                          'Programming Language :: Python :: 3.4',
                          'Programming Language :: Python :: 3.5',
                          'Programming Language :: Python :: 3.6',
                          'Programming Language :: SQL',
                          'Topic :: Database',
                          'Topic :: Database :: Front-Ends',
                          'Topic :: Software Development :: Libraries :: Python Modules',
            ]
    )

