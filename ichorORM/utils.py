'''    
    Copyright (c) 2016-2018 Timothy Savannah

    Licensed under the terms of the Lesser GNU Lesser General Public License version 2.1
    
      license can be found at https://raw.githubusercontent.com/kata198/ichorORM/master/LICENSE


    utils - Some general utility functions
'''

def convertFilterTypeToOperator(filterType):
    '''
        convertFilterTypeToOperator - Converts a filter type to a SQL operator

          @param filterType <str> - A filter type ( likely following "__" such as "fieldName__eq" would be "eq" )

          @reurn <str> - Matching SQL operator
    '''

    ft = filterType.lower()

    if not ft or ft in ('eq', '='):
        return '='
    elif ft in ('ne', '!=', '<>'):
        return '<>'
    elif ft in ('gt' '>'):
        return '>'
    elif ft in ('gte', 'ge', '>='):
        return '>='
    elif ft in ('lt', '<'):
        return '<'
    elif ft in ('lte', 'le', '<='):
        return '<='
    elif ft in ('in', 'isin', 'is in', 'is_in' ):
        return 'in'
    elif ft in ('notin', 'not_in', 'not in'):
        return 'not in'
    elif ft in ('like', ):
        return 'like'
    elif ft in ('notlike', 'not_like', 'not like'):
        return 'not like'
    elif ft in ('is', ):
        return 'is'
    elif ft in ('isnot', 'is_not', 'is not'):
        return 'is not'
    elif ft == 'between':
        return 'between'

    raise ValueError('Unknown filter type: %s' %(repr(filterType), ))

def isMultiOperator(operator):
    '''
        isMultiOperator - Check if the given operator takes multiple arguments (like a list, e.x. (1, 2, 3) )

          @param operator <str> - An operator (like from convertFilterTypeToOperator)

          @return <bool>
    '''
    operator = operator.lower()

    if operator in ('in', 'not in', 'between'):
        return True

    return False
