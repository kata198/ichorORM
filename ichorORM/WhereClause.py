'''    
    Copyright (c) 2016-2018 Timothy Savannah

    Licensed under the terms of the Lesser GNU Lesser General Public License version 2.1
    
      license can be found at https://raw.githubusercontent.com/kata198/ichorORM/master/LICENSE

    WhereClause - type to represent a WHERE clause and related
'''


from .constants import WHERE_AND, WHERE_OR


__all__ = ( 'WHERE_AND', 'WHERE_OR', 'WhereClause' )

class WhereClause(object):
    '''
        WhereClause - Construct a "Where" clause in one of several forms
            psycopg2 understands
    '''


    @staticmethod
    def fromFieldValuesDirect(fieldValues, joinBy=WHERE_AND):
        '''
            fromFieldValuesDirect - Get a WHERE clause joining fieldValues with AND/OR

                @param fieldValues <dict> fieldName : fieldValue

                @param joinBy <str> - WHERE_AND or WHERE_OR

                @return String of where clause assembled from above
        '''
        ret = []

        for fieldName, fieldValue in fieldValues.items():
            ret.append( "%s = '%s'" %(fieldName, fieldValue) )

        joinBy = " %s " %(joinBy, )

        return joinBy.join(ret)


    @staticmethod
    def fromFieldValuesParam(fieldValues, joinBy=WHERE_AND):
        '''
            fromFieldValuesParam - Get a WHERE clause ready for paramertized input

                @param fieldValues <dict> fieldName : fieldValue

                @param joinBy <str> WHERE_AND or WHERE_OR

                @return String ready to be used for parameterization
        '''
        ret = []

        for fieldName in fieldValues.keys():
            ret.append( fieldName + " = %s" )

        joinBy = " %s " %(joinBy, )

        return joinBy.join(ret)

    @staticmethod
    def fromFieldValuesDictParam(fieldValues, joinBy=WHERE_AND):
        '''
            fromFieldValuesDictParam - Get a WHERE clause for dict-param input

                @param fieldValues <dict> fieldName : fieldVaulue

                @param joinBy <str> WHERE_AND or WHERE_OR

                @return String ready to be used for dict params
        '''
        ret = []

        for fieldName in fieldValues.keys():
            ret.append( fieldName + " = %(" + fieldName + ")s" )

        joinBy = " %s " %(joinBy, )

        return joinBy.join(ret)

