'''
    Copyright (c) 2016-2018 Timothy Savannah

    Licensed under the terms of the Lesser GNU Lesser General Public License version 2.1
    
      license can be found at https://raw.githubusercontent.com/kata198/ichorORM/master/LICENSE

    ichorORM - An ORM and query-builder for postgresql / psycopg2 with a focus on performance
'''

from .connection import setGlobalConnectionParams, getDatabaseConnection, DatabaseConnection, DatabaseConnectionFailure

from .model import DatabaseModel

__version__ = '1.0.2'
__version_tuple__ = ('1', '0', '2')
__version_int_tuple__ = (1, 0, 2)

__all__ = ('setGlobalConnectionParams', 'getDatabaseConnection', 'DatabaseConnection', 'DatabaseConnectionFailure', 'DatabaseModel')
