'''
    Copyright (c) 2016-2018 Timothy Savannah

    Licensed under the terms of the Lesser GNU Lesser General Public License version 2.1
    
      license can be found at https://raw.githubusercontent.com/kata198/ichorORM/master/LICENSE

    constants.py - Some constants
'''

__all__ = ('FETCH_ALL_FIELDS', 'WHERE_AND', 'WHERE_OR', 'WHERE_ALL_TYPES')

FETCH_ALL_FIELDS = 'ALL'

# Where Types
WHERE_AND = 'AND'
WHERE_OR  = 'OR'

WHERE_ALL_TYPES = ALL_WHERE_TYPES = ( WHERE_AND, WHERE_OR )

# Join Types
JOIN_INNER = 'INNER'
JOIN_LEFT  = 'LEFT'
JOIN_RIGHT = 'RIGHT'
JOIN_OUTER_FULL = 'OUTER FULL'

ALL_JOINS = (JOIN_INNER, JOIN_LEFT, JOIN_RIGHT, JOIN_OUTER_FULL)


class _SQL_NULL_TYPE(str):
    '''
        _SQL_NULL_TYPE - The type of the SQL_NULL singleton. Don't use this directly.
    '''

    def __new__(self):
        return str.__new__(self, 'NULL')

# SQL_NULL - Singleton represneting a NULL in SQL
SQL_NULL = _SQL_NULL_TYPE()

