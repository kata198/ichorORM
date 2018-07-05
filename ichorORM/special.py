'''
    Copyright (c) 2018 Timothy Savannah

    Licensed under the terms of the Lesser GNU Lesser General Public License version 2.1
    
      license can be found at https://raw.githubusercontent.com/kata198/ichorORM/master/LICENSE


    special - Some special types / values
'''
# vim: set ts=4 sw=4 st=4 expandtab:

__all__ = ('QueryStr', 'SQL_NULL', 'isQueryStr' )

class QueryStr(str):
    '''
        QueryStr - A portion that should be treated like a raw string (Embedded sql),
          and not a quoted value
    '''

    def __eq__(self, other):
        if not isQueryStr(other):
            return False
        return str.__eq__(self, other)

    def __ne__(self, other):
        return not QueryStr.__eq__(self, other)

SQL_NULL = QueryStr('NULL')


def isQueryStr(obj):
    return bool( issubclass(obj.__class__, QueryStr) )
