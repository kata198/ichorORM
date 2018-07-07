'''
    Copyright (c) 2016-2018 Timothy Savannah

    Licensed under the terms of the Lesser GNU Lesser General Public License version 2.1
    
      license can be found at https://raw.githubusercontent.com/kata198/ichorORM/master/LICENSE

    ichorORM - An ORM and query-builder for postgresql / psycopg2 with a focus on performance
'''

from .connection import setGlobalConnectionParams, getDatabaseConnection, DatabaseConnection, DatabaseConnectionFailure

from .model import DatabaseModel
from .special import SQL_NULL, QueryStr

from .query import SelectQuery, InsertQuery, UpdateQuery, DeleteQuery, SelectInnerJoinQuery, SelectGenericJoinQuery

__version__ = '2.0.1'
__version_tuple__ = ('2', '0', '1')
__version_int_tuple__ = (2, 0, 1)

__all__ = ('setGlobalConnectionParams', 'getDatabaseConnection', 'DatabaseConnection', 'DatabaseConnectionFailure', 'DatabaseModel')
