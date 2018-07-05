'''
    Copyright (c) 2018 Timothy Savannah

    Licensed under the terms of the Lesser GNU Lesser General Public License version 2.1
    
      license can be found at https://raw.githubusercontent.com/kata198/ichorORM/master/LICENSE


    special - Some special types / values
'''
# vim: set ts=4 sw=4 st=4 expandtab:

__all__ = ('QueryStr', 'SQL_NULL' )

class QueryStr(str):
    '''
        QueryStr - A portion that should be treated like a raw string (Embedded sql),
          and not a quoted value
    '''
    pass

SQL_NULL = QueryStr('NULL')