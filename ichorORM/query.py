'''
    Copyright (c) 2016-2018 Timothy Savannah

    Licensed under the terms of the Lesser GNU Lesser General Public License version 2.1
    
      license can be found at https://raw.githubusercontent.com/kata198/ichorORM/master/LICENSE


    query - A lot of abstraction and whatnot related to various SQL commands
'''
# vim: set ts=4 sw=4 st=4 expandtab:

import copy
import datetime
import re

from psycopg2.extensions import adapt as psycopg2_adapt

from .special import QueryStr, SQL_NULL, isQueryStr
from .constants import WHERE_AND, WHERE_OR, WHERE_ALL_TYPES, ALL_JOINS
from .utils import convertFilterTypeToOperator, isMultiOperator
from .objs import DictObj


from collections import OrderedDict

from . import getDatabaseConnection

__all__ = ('QueryStr', 'QueryBase', 'FilterType', 'isFilterType', 'FilterField', 'FilterJoin', 'FilterStage',
            'isSelectQuery', 'SelectQuery', 'SelectInnerJoinQuery', 'SelectGenericJoinQuery',
            'UpdateQuery', 'InsertQuery', 'DeleteQuery', 'SQL_NULL' )


class FilterType(object):
    '''
        FilterType - Base class of filters
    '''
    pass

def isFilterType(obj):
    '''
        isFilterType - Check if this is a filterable type (on a field, or a collection thereof)
    '''
    return issubclass(obj.__class__, FilterType)


class FilterField(FilterType):
    '''
        FilterField - A single condition on a field
    '''

    __slots__ = ('filterName', 'filterType', 'filterValue', 'operator')


    def __init__(self, filterName, filterType, filterValue, operator=None):
        '''
            __init__ - Create a FilterField (conditional)

                @param filterName <str> - Field name

                @param filterType <str> - Operation ( like "=" )

                @param filterValue <str/QueryStr/SelectQuery> - The value to match, or a query to embed to fetch value

                @param operator <str/None> default None - If provided, will use
                    
                    this as the operator, otherwise will calculate from #filterType
        '''

        self.filterName = filterName
        self.filterType = filterType
        self.filterValue = filterValue

        if operator is None:
            try:
                self.operator = convertFilterTypeToOperator(filterType)
            except ValueError as ve:
                raise ve
        else:
            self.operator = operator

        # Convert any = None/NULL to is NULL and any != None/NULL to is not NULL
        #   In postgresql a NULL field does not = NULL, but it is NULL.
        # This will prevent any unexpected filter failures when dealing with nulls
        if filterValue == SQL_NULL or filterValue == None:
            if self.operator == '=':
                self.operator = self.filterType = 'is'
            elif self.operator in ('<>', '!='):
                self.operator = self.filterType = 'is not'


    def getFilterValue(self):
        '''
            getFilterValue - Get the value of this filter

            @return <str> - Value of this filter
        '''
        return self.filterValue


    def toStr(self):
        '''
            toStr - Convert this into a single boolean expression for a SQL query.

              Recommended to use #toStrParam for quoting / injection reasons
        '''
        filterValue = self.filterValue

        if isQueryStr(filterValue):
            filterValue = filterValue
        elif isSelectQuery(filterValue):
            filterValue = filterValue.asQueryStr()
        else:
            # Convert complex types into proper representation
            filterValue = str(psycopg2_adapt(filterValue))

        return " %s %s %s " %(self.filterName, self.operator, filterValue)

    def toStrParam(self, paramName):
        '''
            toStrParam - Convert this into a single boolean expression for SQL query,
              with using a parameterized value

            @param paramName <str> - A unique name for this value

            @return tuple( <str>, <dict> ) - A tuple of the param str (for appending to SQL query), and
               a dict of the paramNames -> paramValues, for passing into parameterization
        '''
        params = {}

        ret = " %s %s " %(self.filterName, self.operator)

        filterValue = self.getFilterValue()

        if isQueryStr(filterValue):
            # Raw embedded SQL
            ret += filterValue + " "
        elif self.operator.lower() == 'between':
            if issubclass(filterValue.__class__, (tuple, list)) and len(filterValue) == 2:
                # Check if we are using "BETWEEN" operator and have a 2-element list/tuple
                slot2Name = paramName + '__item2_'
                thisItem = []

                # If we have a query str, put that directly in, otherwise param value
                if isQueryStr(filterValue[0]):
                    thisItem.append(filterValue[0])
                else:
                    slot1Name = paramName + '__item1_'

                    thisItem += ['%(', slot1Name, ')s ']
                    params[slot1Name] = filterValue[0]

                thisItem.append('  AND  ')

                if isQueryStr(filterValue[1]):
                    thisItem.append(filterValue[1])
                else:
                    slot2Name = paramName + '__item2_'

                    thisItem += ['%(', slot2Name, ')s ']
                    params[slot2Name] = filterValue[1]

                ret += ''.join(thisItem)

            elif isQueryStr(filterValue):
                # NOTE: This should never be reached, should be caught in first conditional
                ret += filterValue + " "
            else:
                # Unknown what we got here
                raise ValueError('Unexpected value for "BETWEEN" operator: <%s> %s.\nShould either be a 2-element array/tuple or a QueryStr.' %(str(type(filterValue)), repr(filterValue)))


        elif isSelectQuery(filterValue):
            (_queryStr, _selectParams) = filterValue.asQueryStrParams(paramPrefix=paramName)

            ret += " " + _queryStr
            params.update(_selectParams)
            
        elif not isMultiOperator(self.operator):
            # If not a multi operator, insert one parameterized value
            ret += ' %(' + paramName + ')s '
            params[paramName] = filterValue
        else:
            # Otherwise, we have to insert a list of parameterized values, like ( %(p1)s , %(p2)s )
            if not issubclass(filterValue.__class__, (list, tuple)):
                # If this is not a list but a string of a list, extract the items
                innerParenRE = re.match('^[ \t]*[(](?P<innerValues>[^)]+)[)]', filterValue)

                if not innerParenRE:
                    raise ValueError('Cannot parse value: %s' %(repr(filterValue), ))

                innerValues = innerParenRE.groupdict()['innerValues'].strip().split(',')
                innerValues2 = []
                for innerValue in innerValues:
                    innerValue = innerValue.strip()
                    if len(innerValue) >= 2:
                        if ( innerValue[0] == "'" and innerValue[-1] == "'" ) or \
                             (innerValue[0] == '"' and innerValue[-1] == '"'):
                                innerValue = innerValue[1:-1]
                    innerValues2.append(innerValue)
                innerValues = innerValues2
            else:
                # Otherwise, use the direct values provided
                innerValues = filterValue

            ret += '( '

            #multiNum = 0 #XXX: unused
            paramName += '_m'

            #vals = []    #XXX: unused

            numInnerValues = len(innerValues)

            for i in range(numInnerValues):
                innerValue = innerValues[i]

                nextParamName = paramName + str(i)

                params[nextParamName] = innerValue

                ret += ' %(' + nextParamName + ')s '
                if i + 1 != numInnerValues:
                    ret += ','


            ret += ' ) '

        return ( ret, params )


class FilterJoin(FilterField):
    '''
        FilterJoin - A filter expression which performs a join.

            Does NOT quote, both sides are expected to be fields.
    '''

    def toStr(self):
        '''
            toStr - Convert this into a single boolean expression for a SQL query
        '''
        return " %s %s %s " %(self.filterName, self.operator, self.filterValue)

    def toStrParam(self, paramName):
        '''
            toStrParam - Convert to a boolean expression for SQL query, parameterized

              for FilterJoin this is the same as toStr, params will always be blank

        '''

        myStr = self.toStr()

        return ( myStr, [] )


class FilterStage(FilterType):
    '''
        FilterStage - A filter stage, i.e. a grouping of conditions for the WHERE clause
    '''

    __slots__ = ('whereType', 'filters')

    def __init__(self, whereType=WHERE_AND, filters=None):
        '''
            __init__ - Create a FilterStage, defining a paticular "stage" of filtering

              Surrounded in parenthesis, with the various parts being joined by #whereType.

                @param whereType <str> , default "AND" [ WHERE_AND ] - This will join the various filter parts together

                @param filters <None/list <FilterField/FilterStage>> - None, or a list of either FilterField's or FilterStage's
        '''
        self.whereType = whereType

        if filters is None:
            filters = list()

        self.filters = filters


    def toStr(self):
        '''
            toStr - Convert this stage into a grouped boolean expression, including any nested stages.
        '''

        # If we are empty stage, return nothing
        if not self.filters:
            return ''

        whereType = self.whereType

        expressions = []
        for _filterEm in self.filters:
            if isSelectQuery(_filterEm):
                filterStr = _filterEm.asQueryStr()
            else:
                filterStr = _filterEm.toStr()
            
            filterStr = filterStr.strip()
            if filterStr:
                expressions.append(filterStr)

        if not expressions:
            return ''

        expressionsStr = '  (    %s   )  ' %( whereType.join(expressions), )

        return expressionsStr

    def toStrParam(self, paramPrefix):
        '''
            toStrParam - Convert to a parameterized string.

            @return tuple ( <str>, <dict> ) - The SQL string, and parameter dict
        '''

        if not self.filters:
            return ( '', [] )


        whereType = self.whereType

        expressions = []
        params = {}

        paramPrefix = paramPrefix + '_'

        paramNum = 0

        for _filterEm in self.filters:

            paramName = paramPrefix + str(paramNum)
            paramNum += 1

            if issubclass(_filterEm.__class__, FilterStage):
                (expression, filterParams) = _filterEm.toStrParam(paramName)
                if not expression:
                    continue
                params.update(filterParams)
            elif isSelectQuery(_filterEm):
                (expression, filterParams) = _filterEm.asQueryStrParams(paramPrefix=paramName)
                if not expression:
                    continue
                params.update(filterParams)
            else:
                (expression, filterParams) = _filterEm.toStrParam(paramName)
                if not expression:
                    continue
                params.update(filterParams)
#                params[paramName] = _filterEm.getFilterValue()

            expressions.append(expression)

        innerParenStr = whereType.join(expressions)
        if not innerParenStr.strip():
            expressionsStr = ''
            params = []
        else:
            expressionsStr = '  (   %s   )  ' %( whereType.join(expressions), )

        return ( expressionsStr, params )


    def __len__(self):
        return len(self.filters)


    def addFilter(self, _filter):
        '''
            addFilter/append/add - Add a filter.

            @param _filter < FilterField > - A FilterField object which has a field name, comparison operation, and matching value
        '''

        if not isFilterType(_filter):
            raise ValueError('Attempted to add non filter type < %s >:  %s' %(_filter.__class__.__name__, repr(_filter)))

        self.filters.append(_filter)

    add = addFilter

    append = addFilter

    def addStage(self, whereType=WHERE_AND):
        '''
            addStage - Add a sub stage to this query (like to AND together sets of multi-field OR filters)

            @param whereType <str> - AND or OR
        '''
        newStage = FilterStage(whereType)

        self.addFilter(newStage)

        return newStage

    def addCondition(self, filterName, filterType, filterValue):
        '''
            addCondition - Adds a filter, except takes the parameters of a FilterField as arguments

              @return - The FilterField that was created and added

            @see FilterField
        '''

        filterField = FilterField(filterName, filterType, filterValue)

        self.addFilter(filterField)

        return filterField

    def addJoin(self, leftName, filterType, rightName):
        '''
            addJoin - Adds a FilterJoin, which is a condition which results in an inner join

              @return - The FilterJoin that was created and added

            @see FilterField
            @see FilterJoin
        '''

        filterJoin = FilterJoin(leftName, filterType, rightName)

        self.addFilter(filterJoin)

        return filterJoin

    def removeFilter(self, _filter):
        '''
            removeFilter - Remove a filter from the list of filters.

                @param _filter <FilterType obj> - The filter to remove
                
                @return <None/FilterType obj> - The filter removed, or None if not found.
        '''
        ret = None
        try:
            ret = self.filters.remove(_filter)
        except:
            pass

        return ret

    '''
        remove - Alias for removeFilter

            @see removeFilter
    '''
    remove = removeFilter

    def pop(self, idx):
        '''
            pop - Remove and return a filter by index

                @param idx <int> - The index to pop

                @return <FilterType obj> - The filter popped
        '''
        mySize = len(self)
        if mySize == 0 or idx >= mySize:
            raise IndexError('Index out of range!')

        if idx < 0:
            # Flip it around

            if abs(idx) >= mySize:
                raise IndexError('Index out of range!')

            idx = mySize + idx

        return self.filters.pop(idx)

    def index(self, val):
        '''
            index - Get the index of the first match of a given filter

                @param val <FilterType obj> - Filter to search for

                @return <int> - First index of given filter
        '''
        return self.filters.index(val)

    def rindex(self, val):
        '''
            rindex - Get the index of the last match of a given filter

                @param val <FilterType obj> - Filter to search for

                @return <int> - Last index of given filter
        '''
        return self.filters.rindex(val)



class QueryBase(object):
    '''
        QueryBase - Base object for constructing a query.

            Should be extended and not used directly
    '''

    def __init__(self, model, filterStages=None):
        '''
            __init__ - Create a QueryBase object

                @param model <DatabaseModel> - The DatabaseModel to use for this query

                @param filterStages <None/list<FilterType objs>> Default None - 
                
                    You may past a list of initial filter stages to use in the WHERE portion of the query, or leave as None and add them indivdiualy with #addStage (preferred).

                    This list will be copied, but the stages themselves will not
                      (so if you add a condition to one of these stages, that will carry over,
                       but if you append to the list you provided here that will not.
                       Use #addStage instead.
                      )

        '''
        self.model = model

        # Ensure model is init and valid
        if model:
            self.model._setupModel()

        self.filterStages = []

        if filterStages:
            # Copy only the list here, but keep reference to objects
            for filterStage in filterStages:
                # TODO: Confirm that FilterType is good enough, maybe should enforce FilterStage?
                if not isFilterType(filterStage):
                    raise ValueError('Non-subclass of FilterType provided as a FilterStage. Type was: ' + 
                        filterStage.__class__.__name__
                    )
                self.filterStages.append(filterStage)


    def getModel(self):
        '''
            getModel - Gets the model <DatabaseModel> associated with this Query.
            
                NOTE: Not all queries have the same meaning here.
                  For example, a SelectInnerJoinQuery
                    has no "model" set but a list of "models" ( @see #getModels ).
                  On a SelectGenericJoinQuery this returns the "primary" model, while #getModels returns all models.

                  @return <DatabaseModel/None> - The "primary" model associated with this query if applicable
        '''
        return self.model


    def getModels(self):
        '''
            getModels - Gets a list of the models <DatabaseModel> associated with this Query.

                NOTE: The meaning of the query comes into play here.
                  For example, an InsertQuery would return a list of [ self.getModel() ].
                  A SelectGenericJoinQuery would return all the applicable models

                  @return list<DatabaseModel> - A list of alll models associated with this query
        '''
        # Generic impl - some queries may need to override this.

        # If we have a "self.models" attribute, return that
        if hasattr(self, 'models'):
            return self.models

        # Otherwise, return a list containing just the primary model, or empty list as last resort.
        model = self.getModel()
        if model:
            return [model]
        return []


    def addStage(self, _filter=WHERE_AND):
        '''
            addStage - Add a "stage" to the WHERE condition. A stage is a collection of conditional expressions.

              @param _filter:
                <str>  -  Default WHERE_AND - WHERE_AND or WHERE_OR , this specifies the relation of the various
                         conditionals in this stage,
                         i.e. if it should be ( conA AND conB AND conC )   or  ( conA OR conB OR conC )

                <FilterStage> - Adds a FilterStage directly

               @return - The FilterStage created by calling this method (just add to it, and it will automatically be linked),
                        or if a FilterStage was passed it will be returned
        '''

        if issubclass(_filter.__class__, FilterStage):
            #self.filterStages.append( (_filter.whereType, _filter) )
            self.filterStages.append( _filter )
            return _filter
        elif _filter in WHERE_ALL_TYPES:
            stage = FilterStage(whereType=_filter)
            #self.filterStages.append( (stage.whereType, stage) )
            self.filterStages.append( stage )
            return stage
        else:
            raise ValueError("Not a FilterStage or WHERE type:  %s:  %s" %(_filter.__class__.__name__, repr(_filter)))

    def getWhereClause(self, whereJoin=WHERE_AND):
        '''
            getWhereClause - Gets the "WHERE" portion (including the string 'WHERE'), including all stages and sub-stages

              @param whereJoin - If there are multiple top-level filter stages on this Query, this is how they should be related to eachother,
                via AND or OR.

                Generally, it makes more sense to just add a single filter stage at the top level, and append additional stages onto that.

             @return <str> - The WHERE clause, or empty string if no conditions are present
        '''
        if not self.filterStages:
            return ''

        filterStr = [fs.toStr() for fs in self.filterStages]

        # Remove empty stages
        filterStr = [x for x in filterStr if x.strip()]

        clauses = whereJoin.join(filterStr)
        if not clauses.strip():
            return ''

        return 'WHERE  ' + clauses

    def getWhereClauseParams(self, whereJoin=WHERE_AND, paramPrefix=''):
        '''
            getWhereClauseParams - Gets the "WHERE" portion (including the string 'WHERE'), parameterized and the parameters

                @param whereJoin <WHERE_AND/WHERE_OR> - Either 'AND' or 'OR', what will join the various conditions in this clause

                @param paramPrefix <str> Default '' - If not empty string, paramPrefix + "_" will be prepended to all parameters

              @see getWhereClause
        '''
        if not self.filterStages:
            return ( '', [] )

        if paramPrefix:
            paramPrefix = paramPrefix + '_' + 'wh_stg'
        else:
            paramPrefix = 'wh_stg'
            
        stageNum = 0

        stagesStrLst = []
        stagesParamValues = {}

        for fs in self.filterStages:

            stageParamPrefix = paramPrefix + str(stageNum)
            stageNum += 1

            (stageStr, stageParams) = fs.toStrParam(stageParamPrefix)

            stagesStrLst.append(stageStr)
            stagesParamValues.update(stageParams)

        clauses = whereJoin.join(stagesStrLst)
        if not clauses.strip():
            return ( '', [] )

        return ( 'WHERE  ' + clauses , stagesParamValues )

    def getSql(self):
        '''
            getSql - Get the SQL for this query

            @return <str> - The SQL string
        '''
        raise NotImplementedError('Must implement getSql. Type %s does not.' %(self.__class__.__name__, ))

    def execute(self, dbConn=None, doCommit=True):
        '''
            execute - Perform the action of this query, regardless of type
                        (useful for batching transactions into lists and iterating through)

                      @param dbConn <DatabaseConnection/None> Default None -
                                The postgresql connection to use, or None to
                                create a new one from global settings.

                                Must be defined if doCommit=False (so you can commit later)
        '''
        raise NotImplementedError('Must implement execute. Type %s does not.' %(self.__class__.__name__, ))


    def getTableName(self):
        '''
            getTableName - Get the name of the table associated with this model

              @return <str> - Table Name
        '''
        return self.model.TABLE_NAME

    def getAllFieldNames(self):
        '''
            getAllFieldNames - Get a list of all the fields on this table

              @return list<str> - List of all fields on model
        '''
        return self.model.FIELDS

def isSelectQuery(obj):
    '''
        isSelectQuery - Checks if passed object inherits from SelectQuery
    '''
    return bool( issubclass(obj.__class__, SelectQuery) )

class SelectQuery(QueryBase):
    '''
        SelectQuery - A Query designed for "SELECT".
    '''

    def __init__(self, model, selectFields='ALL', filterStages=None, orderByField=None, orderByDir='', limitNum=None):
        '''
            __init__ - Create a SelectQuery

                @param model <DatabaseModel> - The model associated with this query

                @param selectFields <list/'ALL'> Defalult 'ALL' - The fields to select. Default 'ALL' will return all fields on this model

                @param filterStages list<FilterStage> - A list of initial filter stages (can and should just be added later)

                        TODO: Refactor and remove "filterStages" from constructor. I thought maybe for copying it would be useful,
                            but forget it.

                @param orderByField - If provided, the first "orderBy" for this query. Can be appended to or removed later. Should be a field name.
                @param orderByDir <str> - If provided, the direction the "order by" will follow.

                @param limitNum <None/int> default None, if provided integer, will return no more than N rows
        '''
        QueryBase.__init__(self, model, filterStages)

        # If "*" is provided, replace with "ALL"
        self.selectFields = None
        self.setSelectFields(selectFields)

        self.orderBys = []

        if orderByField:
            self.addOrderBy( orderByField, orderByDir )

        self.limitNum = limitNum

    
    def clearOrderBy(self):
        '''
            clearOrderBy - Clears the "ORDER BY" portion of this query
        '''
        self.orderBys.clear()


    def setLimitNum(self, limitNum):
        '''
            setLimitNum - Set the limit num (max # of records returned)

                @param limitNum <int/None> - Provide the maximum number of records to return, or None for no limit.
        '''
        self.limitNum = limitNum


    def addOrderBy(self, fieldName, orderByDir=''):
        '''
            addOrderBy - Add an additional "ORDER BY"
        '''

        if orderByDir not in ('', 'ASC', 'DESC'):
            raise ValueError('Unknown direction for order by:  %s' %(repr(orderByDir), ))

        orderBy = ( fieldName, orderByDir )


        self.orderBys.append( orderBy )

    def removeOrderBy(self, fieldName):
        '''
            removeOrderBy - Remove the 'ORDER BY' for a given field

                @param  fieldName <str> - Name of field ordered by

                @return None or tuple ( fieldName<str>, orderByDirection<str> ) if found
        '''

        for i in range(len(self.orderBys)):

            ( orderFieldName, orderByDir ) = self.orderBys[i]

            if orderFieldName == fieldName:
                return self.orderBys.pop(i)

        return None

    def setSelectFields(self, selectFields):
        '''
            setSelectFields - Set the fields to select

            @param selectFields list<str> - Field names
        '''
        # If "*" is provided, replace with "ALL"
        if selectFields == '*' or (issubclass(selectFields.__class__, (tuple, list)) and len(selectFields) == 1 and selectFields[0] == '*'):
            selectFields = 'ALL'

        self.selectFields = selectFields

    def getFields(self):
        '''
            getFields - Get a list of the fields to select (will unroll the special, "ALL")
        '''
        if self.selectFields in ('ALL', '*'):
            selectFields = self.getAllFieldNames()
        else:
            selectFields = self._resolveSpecialFields()

        return selectFields


    def _resolveSpecialFields(self):
        '''
            _resolveSpecialFields - Resolves and validates special fields ( like TABLE_NAME.* )

                @return list<str> - A list of fields to select
        '''
        allModels = self.getModels()

        selectFieldsByTable = None
        selectFields = []

        for selectField in self.selectFields:
            if selectField.endswith('.*'):
                selectFieldSplit = selectField.split('.')
                if len(selectFieldSplit) != 2:
                    raise ValueError('Unknown select field: ' + selectField)

                tableName = selectFieldSplit[0]

                if selectFieldsByTable is None:

                    selectFieldsByTable = {}
                    for model in allModels:
                        selectFieldsByTable[model.TABLE_NAME] = model.FIELDS

                if tableName not in selectFieldsByTable:
                    raise ValueError('Unknown table "%s" in select field: %s. Available tables are: %s' %(tableName, selectField, repr(list(selectFieldsByTable.keys())) ) )

                selectFields += [ "%s.%s" %(tableName, fieldName) for fieldName in selectFieldsByTable[tableName] ]
            else:
                selectFields.append(selectField)

        return selectFields


    def getFieldsStr(self):
        '''
            getFieldsStr - Get comma-separated names of fields fit for going into a query
        '''
        selectFields = self.getFields()

        return ', '.join(selectFields)

    def getOrderByStr(self):
        '''
            getOrderByStr - Gets the ORDER BY portion (or empty string if unset) of the query
        '''
        if not self.orderBys:
            return ''

        orderByFieldsArr = []

        for orderByField, orderByDir in self.orderBys:
            orderByFieldsArr.append( '%s %s' %(orderByField, orderByDir))

        return ' ORDER BY ' + ', '.join(orderByFieldsArr)


    def getLimitStr(self):
        '''
            getLimitStr - Get the LIMIT N portion of the query string
        '''
        if not self.limitNum:
            return ''

        return ' LIMIT ' + str(self.limitNum)


    def getSql(self):
        '''
            getSql - Gets the sql command to execute
        '''
        whereClause = self.getWhereClause()
        orderByClause = self.getOrderByStr()
        limitClause = self.getLimitStr()

        sql = """SELECT  %s  FROM  %s  %s  %s  %s"""  %( self.getFieldsStr(), self.getTableName(), whereClause, orderByClause, limitClause )

        return sql

    def getSqlParameterizedValues(self, paramPrefix=''):
        '''
            getSqlParameterizedValues - Get the sql command parameterized

                @param paramPrefix <str> Default '' - If provided, will prefix params with paramPrefix + "_"

                @return tuple< sql<str>, whereParams <list<FilterStage obj>> >
        '''

        (whereClause, whereParams) = self.getWhereClauseParams(paramPrefix=paramPrefix)
        orderByClause = self.getOrderByStr()
        limitClause = self.getLimitStr()

        sql = """SELECT  %s  FROM  %s  %s  %s  %s""" %( self.getFieldsStr(), self.getTableName(), whereClause, orderByClause, limitClause )

        return (sql, whereParams)

    def executeGetRows(self, parameterized=True, dbConn=None):
        '''
            executeGetRows - Execute and return the raw data from postgres in rows of columns

                @param paramertized <bool> Default True - Whether to use parameterized query

                @param dbConn <DatabaseConnection/None> - If None, start a new connection using the
                                             global connection parameters.
                                             Otherwise, use this provided connection

            @return list<list<str>> - Rows of columns
        '''

        if not dbConn:
            dbConn = getDatabaseConnection()

        if parameterized:
            ( sql, params ) = self.getSqlParameterizedValues()
            rows = dbConn.doSelectParams(sql, params)
        else:
            sql = self.getSql()
            rows = dbConn.doSelect(sql)

        return rows

    def execute(self, dbConn=None, doCommit=True):
        '''
            execute - Execute this action, generic method.

                    For SelectQuery, will call executeGetRows

                @param dbConn <DatabaseConnection/None> Default None - If None, start a new connection
                        using the global connection settings. Otherwise, use given connection.

                @param doCommit <bool> Default True - Ignored on SelectQuery
        '''
        return self.executeGetRows(dbConn=dbConn)

    def executeGetObjs(self, parameterized=True, dbConn=None):
        '''
            executeGetObjs - Execute and transform the returned data into a series of objects, one per row returned.

                @param paramertized <bool> Default True - Whether to use parameterized query

                @param dbConn <DatabaseConnection/None> - If None, start a new connection using the
                                             global connection parameters.
                                             Otherwise, use this provided connection

            @return list<model object> - A list of constructed model objects with the fields from this query filled
        '''

        rows = self.executeGetRows(parameterized=parameterized, dbConn=dbConn)
        if not rows:
            return []

        ret = []

        Model = self.model
        fields = self.getFields()

        for row in rows:
            fieldMap = { fields[i] : row[i] for i in range(len(fields)) }
            ret.append( Model(**fieldMap) )

        return ret

    def asQueryStr(self):
        '''
            asQueryStr - Return this SELECT as an embedded group
                    (for use in setting field values based on selected values and similar endeavours

                  @return <QueryStr> -
                                A QueryStr representing this SELECT, with values inline
        '''

        
        retQS = QueryStr()
        retParams = {}

        ( selectSqlStr, retParams ) = self.getSqlParameterizedValues()

        selectSqlStr = selectSqlStr.strip()

        # If this select is not grouped, make it into a group
        if not selectSqlStr.startswith('('):
            selectSqlStr = ''.join(['( ', selectSqlStr, ' )'])

        for paramName, paramValue in retParams.items():
            selectSqlStr = selectSqlStr.replace('%(' + paramName + ')s', str(psycopg2_adapt(paramValue)) )

        retQS = QueryStr( selectSqlStr )
        return retQS

    toStr = asQueryStr

    def asQueryStrParams(self, paramPrefix=''):
        '''
            asQueryStrParams - Return this SELECT as an embedded group, paramertized version
                    (for use in setting field values based on selected values and similar endeavours

                  @param paramPrefix <str> - If provided, will prefix the parameters with this string + '_'

                  @return tuple( <QueryStr>, <dict> ) - A tuple containing:
                                0 - A QueryStr representing this SELECT
                                1 - A dict containing any params (to be added to param list)
        '''

        
        retQS = QueryStr()
        retParams = {}

        ( selectSqlStr, retParams ) = self.getSqlParameterizedValues(paramPrefix=paramPrefix)

        # If this select is not grouped, make it into a group
        if not selectSqlStr.startswith('('):
            selectSqlStr = ''.join(['( ', selectSqlStr, ' )'])

        retQS = QueryStr( selectSqlStr )
        return ( retQS, retParams )

    def toStrParam(self, prefix):
        (selectSqlStr, retParams) = self.asQueryStrParams(paramPrefix=prefix)

        return (selectSqlStr, retParams)


class SelectInnerJoinQuery(SelectQuery):
    '''
        SelectInnerJoinQuery - A SELECT query on multiple tables which supports inner join
    '''


    def __init__(self, models, selectFields='ALL', orderByField=None, orderByDir='', limitNum=None):
        '''
            __init__ - Create a SelectInnerJoinQuery

                @param models - list<DatabaseModel> - List of models to use

                @param selectFields <'ALL' or list<str>> - Default ALL for all fields on all joined models, or a list of fields to select (prefix with table name, like MyTable.myField)
                    

                    Use MyModel.TABLE_NAME + '.*' to select all fields on "MyModel"

                @param orderByField <None/str> Default None - Order by this field, if provided

                @param orderByDir <str> Default '' - ASC or DESC for ascneding/descending

                @param limitNum <int/None> default None - If provided, will return MAX this many results
        '''
        SelectQuery.__init__(self, None, selectFields=selectFields, orderByField=orderByField, orderByDir=orderByDir, limitNum=limitNum)

        self.models = models
        for model in models:
            model._setupModel()

    def getTableName(self):
        '''
            getTableName - Not implemented on SelectInnerJoinQuery
        '''
        raise NotImplementedError('SelectInnerJoinQuery does not deal with a single table. Use getTableNames instead.')

    def getAllFieldNames(self):
        '''
            getAllFieldNames - Not implemented on SelectInnerJoinQuery
        '''
        raise NotImplementedError('SelectInnerJoinQuery does not deal with a single table. Use getAllFieldNamesByTable instead.')

    def getTableNames(self):
        '''
            getTableNames - Get a list of associated table names
        
                @return list<str> - List of table names
        '''
        return [ model.TABLE_NAME for model in self.models ]

    def getTableNamesStr(self):
        '''
            getTableNamesStr - Get a comma-joined string of associated table names

                @return <str> - comma-joined string of associated table names
        '''
        return ', '.join(self.getTableNames())

    def getAllFieldNamesByTable(self):
        '''
            getAllFieldNamesByTable - Get a map of table name : fields for all tables

                @return dict <str> : list<str> - Table name as key, all fields as value
        '''
        return { model.TABLE_NAME : model.FIELDS for model in self.models }

    def getAllFieldNamesIncludingTable(self):
        '''
            getAllFieldNamesIncludingTable - Get a list of all fields prefixed with table name,

                i.e.  tableName.fieldName for each table and all fields

                @return list<str> - List of tableName.fieldName for all tables and fields
        '''
        ret = []
        for tableName, tableFields in self.getAllFieldNamesByTable().items():
            ret += [ "%s.%s" %(tableName, fieldName) for fieldName in tableFields ]

        return ret


    def getFields(self):
        '''
            getFields - Get a string of the fields to use in SELECT

                @return list<str> - List of fields to select
        '''
        if self.selectFields in ('ALL', '*'):
            selectFields = self.getAllFieldNamesIncludingTable()
        else:
            selectFields = self._resolveSpecialFields()

        return selectFields


    def getSql(self):
        '''
            getSql - Gets the sql command to execute
        '''
        whereClause = self.getWhereClause()
        orderByClause = self.getOrderByStr()
        limitClause = self.getLimitStr()

        sql = """SELECT  %s  FROM  %s  %s  %s  %s"""  %( self.getFieldsStr(), self.getTableNamesStr(), whereClause, orderByClause, limitClause )

        #print ( "\n\nSQL:\n%s\n\n" %(sql, ))

        return sql


    def getSqlParameterizedValues(self, paramPrefix=''):
        '''
            getSqlParameterizedValues - Gets the SQL command to execute using parameterized values

                @param paramPrefix <str> Default '' - If provided, will prefix params with paramPrefix + "_"

                @return tuple ( sql<str>, whereParams list<str> )
        '''

        (whereClause, whereParams) = self.getWhereClauseParams(paramPrefix=paramPrefix)
        orderByClause = self.getOrderByStr()
        limitClause = self.getLimitStr()

        sql = """SELECT  %s  FROM  %s  %s  %s  %s""" %( self.getFieldsStr(), self.getTableNamesStr(), whereClause, orderByClause, limitClause )

        return (sql, whereParams)


    def executeGetObjs(self, parameterized=True, dbConn=None):
        '''
            executeGetObjs - Not supported for SelectInnerJoinQuery
        '''
        raise NotImplementedError('SelectInnerJoinQuery does not support executeGetObjs. Use executeGetRows, executeGetMapping, or executeGetDictObjs instead.')

    def executeGetMapping(self, parameterized=True, dbConn=None):
        '''
            executeGetMapping - Execute this query, and return the results as
                a list of rows, where each list contains a map (OrderedDict) of the field name -> value

                @param paramertized <bool> Default True - Whether to use parameterized query

                @param dbConn <DatabaseConnection/None> - If None, start a new connection using the
                                             global connection parameters.
                                             Otherwise, use this provided connection

            @return list<dict> - List of rows, each row as a dict with named columns
        '''

        ret = []

        rows = self.executeGetRows(parameterized=parameterized, dbConn=dbConn)

        if not rows:
            return ret

        fields = self.getFields()

        for row in rows:
            # Use an OrderedDict versus a regular dict + comprehension
            #   to ensure returned field order is same as selected
            thisRowMap = OrderedDict()

            for i in range(len(fields)):
                fieldName = fields[i]

                thisRowMap[fieldName] = row[i]

            ret.append(thisRowMap)

        return ret

    def executeGetDictObjs(self, parameterized=True, dbConn=None):
        '''
            executeGetDictObjs - Execute this query, and map results to DictObjs by table.

                I.e. if you have a table "MyTable" and a field "MyTable.MyField", you can access like:

                    rows = executeGetDictObjs()

                    result1 = rows[0]

                    myTableMyField = result1.MyTable.MyField

                @param paramertized <bool> Default True - Whether to use parameterized query

                @param dbConn <DatabaseConnection/None> - If None, start a new connection using the
                                             global connection parameters.
                                             Otherwise, use this provided connection

                @return list<DictObjs> - List of rows, a DictObj for each row.
        '''

        ret = []

        rows = self.executeGetRows(parameterized=parameterized, dbConn=dbConn)

        if not rows:
            return ret

        fields = self.getFields()
        tableNames = self.getTableNames()

        newRowDictObj = lambda : DictObj(**{ tableName : DictObj() for tableName in tableNames })

        extractTableNameRE = re.compile('^(?P<table_name>[^\.]+)[.](?P<field_name>.+)$')

        for row in rows:

            thisRowDictObj = newRowDictObj()

            for i in range(len(fields)):

                fieldName = fields[i]

                matchObj = extractTableNameRE.match(fieldName)

                if not matchObj:
                    raise ValueError('Field name  %s  is not in the form  TABLE.FIELD' %(repr(fieldName), ))

                groupDict = matchObj.groupdict()

                thisRowDictObj[ groupDict['table_name'] ][ groupDict['field_name'] ] = row[i]

            ret.append(thisRowDictObj)

        return ret


class SelectGenericJoinQuery(SelectQuery):
    '''
        SelectInnerJoinQuery - A SELECT query on multiple tables which supports inner join
    '''


    def __init__(self, primaryModel, selectFields='ALL', orderByField=None, orderByDir='', limitNum=None):
        '''
            __init__ - Create a SelectGenericJoinQuery

                @param primaryModel - <DatabaseModel> - Primary Database model

                @param selectFields <'ALL' or list<str>> - Default ALL for all fields on all joined models, or a list of fields to select (prefix with table name, like MyTable.myField).

                    Use MyModel.TABLE_NAME + '.*' to select all fields on "MyModel"

                @param orderByField <None/str> - Default None, if provided ORDER BY this field

                @param orderByDir <str> Default '', ASC or DESC for direction

                @param limitNum <None/int> default None - If provided, return max this many records
        '''
        SelectQuery.__init__(self, primaryModel, selectFields=selectFields, orderByField=orderByField, orderByDir=orderByDir, limitNum=limitNum)

        self.models = [ self.model ]

        self.joins = []


    def joinModel(self, model, joinType, conditionGrouping=WHERE_AND):
        '''
            joinModel - Join to another model, using a join type and given condition grouping

                @param model <DatabaseModel> - Database model to use

                @param joinType <str> - A join type (see JOIN_* in constants.py)

                @param conditionGrouping <str> default AND, either WHERE_AND or WHERE_OR

                @return - The stage that makes up the conditional on the join.
        '''
        joinType = joinType.upper()


#        if joinType not in ALL_JOINS:
#            raise ValueError('Unknown join type: "%s". Must be one of: %s' %(joinType, ' '.join(['"%s"' %(joinType, ) for joinType in ALL_JOINS] ) )
#            )

        joinStage = FilterStage(conditionGrouping)

        self.joins.append( (model, joinType, joinStage) )

        if model not in self.models:
            self.models.append(model)

        return joinStage

    def getJoinClausesParams(self, paramPrefix=''):
        '''
            getJoinClausesParams - Get the "join" clauses with paramertized parameters

                @param paramPrefix <str> Default '' - If not empty string, paramPrefix + "_" will be prepended to all parameters

        '''
        if not self.joins:
            return ( '', [] )

        innerJoinStrs = []
        innerJoinParams = {}

        stageNum = 0

        if paramPrefix:
            paramPrefix = paramPrefix + '_' + 'join_stg'
        else:
            paramPrefix = 'join_stg'

        #stagesStrLst = []  # XXX: unused
        #stagesParamValues = {} # XXX: unused

        for model, joinType, joinStage in self.joins:

            stageParamPrefix = paramPrefix + str(stageNum)
            stageNum += 1

            (stageStr, stageParams) = joinStage.toStrParam(stageParamPrefix)

            innerJoinStr = "\n\t%s JOIN  %s  ON  %s\n" %(joinType, model.TABLE_NAME, stageStr)

            innerJoinStrs.append(innerJoinStr)
            innerJoinParams.update(stageParams)

        return ( ''.join(innerJoinStrs) , innerJoinParams )


    def getTableNames(self):
        '''
            getTableNames - Get a list of all table names in this query
            
                @return list<str> - A list of table names
        '''
        return [ model.TABLE_NAME for model in self.models ]

    def getTableNamesStr(self):
        '''
            getTableNamesStr - Get a comma-joined string of table names in this query

                @return <str> - comma-joined string of table names
        '''
        return ', '.join(self.getTableNames())

    def getAllFieldNames(self):
        '''
            getAllFieldNames - Get a list of all field names on the primary table

                @return list<str> - TABLE_NAME.fieldName for each field
        '''
        allFieldNames = SelectQuery.getAllFieldNames(self)
        return [ "%s.%s" %(self.model.TABLE_NAME, fieldName) for fieldName in allFieldNames ]
        #raise NotImplementedError('SelectInnerJoinQuery does not deal with a single table. Use getAllFieldNamesByTable instead.')

    def getAllFieldNamesByTable(self):
        '''
            getAllFieldNamesByTable - Gets a dict of table name to list of all fields for that model

                @return dict table name<str> -> list<str> fields
        '''
        return { model.TABLE_NAME : model.FIELDS for model in self.models }

    def getAllFieldNamesIncludingTable(self):
        '''
            getAllFieldNamesIncludingTable - Get a list of all the field names prefixed with "$tableName."

                @return list<str> - List of all table names and fields
        '''
        ret = []
        for tableName, tableFields in self.getAllFieldNamesByTable().items():
            ret += [ "%s.%s" %(tableName, fieldName) for fieldName in tableFields ]

        return ret


    def getFields(self):
        '''
            getFields - Gets the fields to SELECT

                @return list<str> - List of field names
        '''
        if self.selectFields in ('ALL', '*'):
            selectFields = self.getAllFieldNamesIncludingTable()
        else:
            selectFields = self._resolveSpecialFields()

        return selectFields

    def getJoinClauses(self):
        '''
            getJoinClauses - Get a string of all the JOIN clauses with other tables

                @return <str> - Join clause
        '''
        ret = []

        for model, joinType, joinStage in self.joins:
            ret.append("\n\t%s JOIN  %s  ON  %s\n" %(joinType, model.TABLE_NAME, joinStage.toStr()))

        return ''.join(ret)


    def getSql(self):
        '''
            getSql - Gets the sql command to execute
        '''
        joinClauses = self.getJoinClauses()
        whereClause = self.getWhereClause()
        orderByClause = self.getOrderByStr()
        limitClause = self.getLimitStr()

        sql = """SELECT  %s

 FROM  %s
 %s
 %s  %s  %s"""  %( self.getFieldsStr(), self.getTableName(), joinClauses, whereClause, orderByClause, limitClause )

        #print ( "\n\nSQL:\n%s\n\n" %(sql, ))

        return sql


    def getSqlParameterizedValues(self, paramPrefix=''):
        '''
            getSqlParameterizedValues - Get the SQL with parameterized values

                @param paramPrefix <str> Default '' - If provided, will prefix params with paramPrefix + "_"

                @return tuple( sql<str>, params<list<str>>)
        '''

        (whereClause, whereParams) = self.getWhereClauseParams(paramPrefix=paramPrefix)

        (joinClauses, joinParams) = self.getJoinClausesParams(paramPrefix=paramPrefix)

        orderByClause = self.getOrderByStr()
        limitClause = self.getLimitStr()

        sql = """SELECT  %s

 FROM  %s
 %s
 %s  %s  %s""" %( self.getFieldsStr(), self.getTableName(), joinClauses, whereClause, orderByClause, limitClause )

        params = copy.copy(joinParams)
        params.update(whereParams)

        return (sql, params)


    def executeGetObjs(self, parameterized=True, dbConn=None):
        '''
            executeGetObjs - Not supported on SelectGenericJoinQuery
        '''
        raise NotImplementedError('SelectInnerJoinQuery does not support executeGetObjs. Use executeGetRows, executeGetMapping, or executeGetDictObjs instead.')

    def executeGetMapping(self, parameterized=True, dbConn=None):
        '''
            executeGetMapping - Execute this query, and return the results as
                a list of rows, where each list contains a map (OrderedDict) of the field name -> value

                @param paramertized <bool> Default True - Whether to use parameterized query

                @param dbConn <DatabaseConnection/None> - If None, start a new connection using the
                                             global connection parameters.
                                             Otherwise, use this provided connection

            @return list<dict> - List of rows, each row as a dict with named columns
        '''

        ret = []

        rows = self.executeGetRows(parameterized=parameterized, dbConn=dbConn)

        if not rows:
            return ret

        fields = self.getFields()

        for row in rows:
            # Use an OrderedDict versus a regular dict + comprehension
            #   to ensure returned field order is same as selected
            thisRowMap = OrderedDict()

            for i in range(len(fields)):
                fieldName = fields[i]

                thisRowMap[fieldName] = row[i]

            ret.append(thisRowMap)

        return ret

    def executeGetDictObjs(self, parameterized=True, dbConn=None):
        '''
            executeGetDictObjs - Execute this query, and map results to DictObjs by table.

                I.e. if you have a table "MyTable" and a field "MyTable.MyField", you can access like:

                    rows = executeGetDictObjs()

                    result1 = rows[0]

                    myTableMyField = result1.MyTable.MyField

                @param paramertized <bool> Default True - Whether to use parameterized query

                @param dbConn <DatabaseConnection/None> - If None, start a new connection using the
                                             global connection parameters.
                                             Otherwise, use this provided connection


                @return list<DictObjs> - List of rows, a DictObj for each row.
        '''

        ret = []

        rows = self.executeGetRows(parameterized=parameterized, dbConn=dbConn)

        if not rows:
            return ret

        fields = self.getFields()
        tableNames = self.getTableNames()

        newRowDictObj = lambda : DictObj(**{ tableName : DictObj() for tableName in tableNames })

        extractTableNameRE = re.compile('^(?P<table_name>[^\.]+)[.](?P<field_name>.+)$')

        for row in rows:

            thisRowDictObj = newRowDictObj()

            for i in range(len(fields)):

                fieldName = fields[i]

                matchObj = extractTableNameRE.match(fieldName)

                if not matchObj:
                    raise ValueError('Field name  %s  is not in the form  TABLE.FIELD' %(repr(fieldName), ))

                groupDict = matchObj.groupdict()

                thisRowDictObj[ groupDict['table_name'] ][ groupDict['field_name'] ] = row[i]

            ret.append(thisRowDictObj)

        return ret



class DeleteQuery(QueryBase):
    '''
        DeleteQuery - Perform a delete
    '''

    def __init__(self, model, filterStages=None):
        '''
            __init__ - Create a DeleteQuery

              @param model - The model class
        '''
        QueryBase.__init__(self, model, filterStages)


    def getSql(self):
        '''
            getSql - Get the SQL to execute
        '''

        whereClause = self.getWhereClause()

        sql = """DELETE FROM  %s  %s """ %(self.getTableName(), whereClause)

        return sql

    def getSqlParameterizedValues(self, paramPrefix=''):
        '''
            getSqlParameterizedValues - Get SQL with parameterized values

                @param paramPrefix <str> Default '' - If provided, will prefix params with paramPrefix + "_"

        '''

        (whereClause, whereParams) = self.getWhereClauseParams(paramPrefix=paramPrefix)

        sql = """DELETE FROM  %s  %s """ %(self.getTableName(), whereClause)

        return (sql, whereParams)


    def executeDeleteRaw(self, dbConn=None, doCommit=True):
        '''
            executeDelete - Perform the delete. (non-parameterized)

                will NOT execute anything if conditionals are not set, to prevent accidently wiping the entire DB.

              @param dbConn <DatabaseConnection/None> Default None - Connect to use, like for transaction.

                    If None, default settings will be used with a new connection

              @param doCommit <bool> default True - If True will perform the delete right away.

                    If False, you must manually commit the transaction and a #dbConn is required
        '''
        if not doCommit and not dbConn:
            raise ValueError('doCommit=False but a dbConn not specified!')

        whereClause = self.getWhereClause()

        if not whereClause:
            raise ValueError('Error: Tried to delete the entire tablespace of  %s  (no where clause).' %(self.getTableName(), ))

        sql = self.getSql()

        if not dbConn:
            dbConn = getDatabaseConnection()

        dbConn.executeSql(sql)

        if doCommit:
            dbConn.commit()

    # TODO: implement "forceDeleteAll" flag
    def executeDelete(self, dbConn=None, doCommit=True):
        '''
            executeDelete - Perform the delete. (parameterized)

                will NOT execute anything if conditionals are not set, to prevent accidently wiping the entire DB.

              @param dbConn <DatabaseConnection> - Connect to use, like for transaction

              @param doCommit <bool> default True - If True will perform the delete right away.

                    If False, you must manually commit the transaction and a #dbConn is required
        '''

        if not doCommit and not dbConn:
            raise ValueError('doCommit=False but a dbConn not specified!')

        whereClause = self.getWhereClause()

        if not whereClause:
            raise ValueError('Error: Tried to delete the entire tablespace of  %s  (no where clause).' %(self.getTableName(), ))

        (sql, whereParams) = self.getSqlParameterizedValues()

        if not dbConn:
            dbConn = getDatabaseConnection(isTransactionMode=True)

        dbConn.executeSqlParams(sql, whereParams)

        if doCommit:
            dbConn.commit()


    def execute(self, dbConn=None, doCommit=True):
        '''
            execute - Execute this action, generic method.

                    For SelectQuery, will call executeDelete

                @param dbConn <DatabaseConnection/None> Default None - If None, start a new connection
                        using the global connection settings. Otherwise, use given connection.

                @param doCommit <bool> Default True - If True, will commit right away. If False, you must commit.

                    A #dbConn must be specified if doCommit=False
        '''
        return self.executeDelete(dbConn=dbConn, doCommit=doCommit)



class UpdateQuery(QueryBase):
    '''
        UpdateQuery - Perform an update on a model
    '''


    def __init__(self, model, newFieldValues=None, filterStages=None):
        '''
            __init__ - Create an update query

              @param model - The model class

              @param newFieldValues <dict/None> Default None.
 
                    If provided, will use these as initial field values for update.
                    Providing this is the same as calling #setFieldValues(newFieldValues)

                    You can set the value of specific fields using either the #setFieldValue or #setFieldValues
                        methods after constructing the object.

              @param filterStages <None/list<FilterStage>> Default None - Provide a list of
                    
                    filter stages to use. A copy of this list will be made internally
        '''
        QueryBase.__init__(self, model, filterStages)

        self.newFieldValues = {}
        if newFieldValues:
            # Copy values but not the reference
            self.newFieldValues.update(newFieldValues)


    def setFieldValue(self, fieldName, newValue):
        '''
            setFieldValue - Update a field to a new value

                @param fieldName <str> - The field name (should be in FIELDS array on model class)

                @param newValue <???> - The new value for the field. This can be a string, integer, datetime object, etc. 
                      depending on the schema for this field  
        '''

        self.newFieldValues[fieldName] = newValue

    def setFieldValues(self, fieldNameToValueMap):
        '''
            setFieldValues - Sets one or more field -> value associations for the insert operation.
                
                "Bulk mode"

                @param fieldNameToValueMap dict<str : ???> - A map of field name -> field value
        '''
        self.newFieldValues.update(fieldNameToValueMap)

    @property
    def hasAnyUpdates(self):
        '''
            hasAnyUpdates - Property reflecting whether any fields have been set thus far
                
                in this update query.

                @return <bool> - True if any fields have been configured to be updated, otherwise False
        '''
        return bool(self.newFieldValues)

    def getSetFieldsStr(self):
        '''
            getSetFieldsStr - Get the  X = "VALUE" , Y = "OTHER" portion of the SQL query
        '''

        ret = []

        useNewFieldValues = self.newFieldValues

        for fieldName, newValue in useNewFieldValues.items():

            if isSelectQuery(newValue):
                newValue = newValue.asQueryStr()

            if isQueryStr(newValue):
                newValueStr = newValue
            else:
                newValueStr = str(psycopg2_adapt(newValue))

            ret.append( " %s = %s " %(fieldName, newValueStr) )

        return ' , '.join(ret)

    def getSetFieldParamsAndValues(self):
        '''
            getSetFieldParamsAndValues - For parameterized values,

                This returns a tuple of two values, the first is the paramertized marker to be used in the query,
                  the second is a list of values which should be passed alongside
        '''
        retParams = []
        retValues = {}

        useNewFieldValues = self.newFieldValues

        argNum = 0

        for fieldName, fieldValue in useNewFieldValues.items():

            identifier = 'arg' + str(argNum)
            argNum += 1

            if isSelectQuery(fieldValue):
                (fieldValue, extraRetParams) = fieldValue.asQueryStrParams(paramPrefix=identifier)
                retValues.update(extraRetParams)
                
            if isQueryStr(fieldValue):
                retParams.append( fieldName + ' = ' + str(fieldValue) + " " )
            else:
                retParams.append( fieldName + '= %(' + identifier + ')s ' )
                retValues[identifier] = fieldValue

        return (retParams, retValues)


    def getSql(self):
        '''
            getSql - Get sql command to execute
        '''

        whereClause = self.getWhereClause()
        setFieldsStr = self.getSetFieldsStr()

        sql = """UPDATE  %s  SET  %s   %s"""  %( self.getTableName(), setFieldsStr, whereClause )

        return sql

    def getSqlParameterizedValues(self, paramPrefix=''):
        '''
            getSqlParameterizedValues - Get the SQL to execute, parameterized version

              @param paramPrefix <str> Default '' - If provided, will prefix params with paramPrefix + "_"
        '''

        paramValues = {}

        ( whereClause, whereParams ) = self.getWhereClauseParams(paramPrefix=paramPrefix)

        paramValues.update(whereParams)

        (setFieldParams, setFieldParamValues) = self.getSetFieldParamsAndValues()

        paramValues.update(setFieldParamValues)

        sql = """UPDATE  %s  SET  %s   %s"""  %( self.getTableName(), ', '.join(setFieldParams), whereClause )

        return (sql, paramValues)


    def executeUpdateRawValues(self, dbConn=None, doCommit=False):
        '''
            executeUpdate - Update some records

              @param dbConn <None/DatabaseConnection> - If None, will get a new connection with autocmommit.

              @param doCommit <bool> Default True - If True, will commit right away. If False, you must commit.

                Nay be passed a transaction-connection, to do update within a transaction
        '''
        if not self.hasAnyUpdates:
            return

        if not doCommit and not dbConn:
            raise ValueError('doCommit=False but a dbConn not specified!')

        sql = self.getSql()

        if not dbConn:
            dbConn = getDatabaseConnection()

        dbConn.executeSql(sql)


    def executeUpdate(self, dbConn=None, doCommit=True):
        '''
            executeUpdate - Upate records (parameterized)

                May potentially use an existing DatabaseConnection (for transaction)

            @param dbConn <None/DatabaseConnection> - If None, will use a fresh connection and auto-commit.
               Otherwise, will use the provided connection (which may be linked to a transaction

            @param doCommit <bool> default True - Whether to commit immediately

        '''
        if not self.hasAnyUpdates:
            return

        if not doCommit and not dbConn:
            raise ValueError('doCommit=False but a dbConn not specified!')

        (sqlParam, paramValues) = self.getSqlParameterizedValues()

        if not dbConn:
            dbConn = getDatabaseConnection(isTransactionMode=True)

        dbConn.executeSqlParams(sqlParam, paramValues)

        if doCommit:
            dbConn.commit()


    def execute(self, dbConn=None, doCommit=True):
        '''
            execute - Execute this action, generic method.

                    For SelectQuery, will call executeUpdate

                @param dbConn <DatabaseConnection/None> Default None - If None, start a new connection
                        using the global connection settings. Otherwise, use given connection.

                @param doCommit <bool> Default True - If True, will commit right away. If False, you must commit.

                    A #dbConn must be specified if doCommit=False
        '''
        return self.executeUpdate(dbConn=dbConn, doCommit=doCommit)


class InsertQuery(QueryBase):
    '''
        InsertQuery - A query builder class for doing inserts
    '''

    def __init__(self, model, initialFieldValues=None, filterStages=None):
        '''
            __init__ - Create an insert query

              @param model <DatabaseModel> - The model to use

              @param initialFieldValues <None / dict< str : ??? > > Default None - 
                
                If provided, must be a map to fieldName : fieldValue, and this will become the
                 initial set of fields to be set on the inserted object.

                 Providing this is the same as calling #setFieldValues(initialFieldValues)
        '''
        QueryBase.__init__(self, model, filterStages)

        self.fieldValues = {}
        if initialFieldValues:
            self.setFieldValues(initialFieldValues)


    def setFieldValue(self, fieldName, newValue):
        '''
            setFieldValue - Set a field to a value to be inserted

                @param fieldName <str> - The name of the field to set (should be in FIELDS array)

                @param newValue <???> - The value to insert. Can be a str, number, datetime object, etc. depending on schema
        '''
        self.fieldValues[fieldName] = newValue


    def setFieldValues(self, fieldNameToValueMap):
        '''
            setFieldValues - Set multiple field values

                @param fieldNameToValueMap dict<str : ???> - A map of field name -> field value
                    
                    This will define the values of the fields upon insert
        '''
        self.fieldValues.update(fieldNameToValueMap)


    def getTableFieldParamsAndValues(self):
        '''
            getTableFieldParamsAndValues - For parameterized values,

                This returns a tuple of two values, the first is the paramertized marker to be used in the query,
                  the second is a list of values which should be passed alongside
        '''
        retParams = []
        retValues = {}

        useSetFieldValues = self.fieldValues

        for fieldName, fieldValue in useSetFieldValues.items():
            if isQueryStr(fieldValue):
                retParams.append(fieldValue)
            elif isSelectQuery(fieldValue):
                (selParams, selValues) = fieldValue.asQueryStrParams(paramPrefix=fieldName + '_')
                
                retParams += selParams
                retValues.update(selValues)
            else:
                retParams.append( ' %(' + fieldName + ')s ' )
                retValues[fieldName] = fieldValue

        return (retParams, retValues)

    def getTableFields(self):
        '''
            getTableFields - Get a list of the fields that are going to be set
        '''
        return list(self.fieldValues.keys())

    def getTableFieldsStr(self):
        '''
            getTableFieldsStr - Get the portion following the table name in an INSERT query which specifies
                                    the fields that will be set
        '''

        if not self.fieldValues:
            # Should not be valid for an insert.. think about this
            return ''

        return ' ( %s ) ' %(', '.join( list(self.fieldValues.keys()) ), )

    def getInsertValuesStr(self):
        '''
            getInsertValuesStr - Get the portion following VALUES with values directly within (not parameterized)


        '''

        if not self.fieldValues:
            # Should not be valid for an insert.. think about this
            return ''

        useSetFieldValues = self.fieldValues


        return ' ( %s ) ' %( ', '.join( [ not isQueryStr(val) and repr(val) or str(val) for val in useSetFieldValues.values() ] ), )


    def getSql(self):
        '''
            getSql - Get the SQL to execute, non-parameterized

            @see getSqlParameterizedValues for parameterized version
        '''

        tableFieldsStr = self.getTableFieldsStr()
        insertValuesStr = self.getInsertValuesStr()
        whereClause = self.getWhereClause()

        sql = """INSERT INTO  %s %s  VALUES %s %s"""  %( self.getTableName(), tableFieldsStr, insertValuesStr, whereClause )

        return sql

    def getSqlParameterizedValues(self):
        '''
            getSqlParameterizedValues - Get the SQL to execute, parameterized version

        '''

        tableFieldsStr = self.getTableFieldsStr()
        tableFieldParams, tableFieldValues = self.getTableFieldParamsAndValues()

        sql = """INSERT INTO  %s %s  VALUES ( %s ) """  %( self.getTableName(), tableFieldsStr, ', '.join(tableFieldParams) )

        return (sql, tableFieldValues)


    def executeInsertRawValues(self, dbConn=None, doCommit=True):
        '''
            executeInsertRawValues - Insert records  (non-parameterized)

              May potentially use an existing DatabaseConnection (for transaction)

            @param dbConn <None/DatabaseConnection> - If None, will use a fresh connection and auto-commit.
               Otherwise, will use the provided connection (which may be linked to a transaction

            @param doCommit <bool> default True - Whether to commit immediately


            @see executeInsertParameterized for the parameterized version.
        '''
        sql = self.getSql()

        if not dbConn:
            dbConn = getDatabaseConnection(isTransactionMode=True)

        # TODO: Can probably use doInsert here to return the ID?
        dbConn.executeSql(sql)

        if doCommit:
            dbConn.commit()

    def executeInsert(self, dbConn=None, doCommit=True, returnPk=True):
        '''
            executeInsert - Insert records (parameterized)

                May potentially use an existing DatabaseConnection (for transaction)

            @param dbConn <None/DatabaseConnection> - If None, will use a fresh connection and auto-commit.
               Otherwise, will use the provided connection (which may be linked to a transaction

            @param doCommit <bool> default True - Whether to commit immediately


            @see executeInsert for the non-parameterized version.
        '''

        sqlParam, paramValues = self.getSqlParameterizedValues()

        if not doCommit and not dbConn:
            raise ValueError('doCommit=False but a dbConn not specified!')

        if not dbConn:
            dbConn = getDatabaseConnection(isTransactionMode=True)

        #  OLD RULE!! NO LONGER NEEDED, WILL DRAW VALUE DIRECTLY FROM SEQUENCE EVEN DURING TRANSACTION
        #if returnPk is True and doCommit is False:
        #    raise ValueError('Cannot have both doCommit=False and returnPk=True')

        if returnPk is True:
            pks = dbConn.doInsert(sqlParam, (paramValues, ), autoCommit=False, returnPk=True)
        else:
            dbConn.executeSqlParams(sqlParam, paramValues)

        if doCommit:
            dbConn.commit()

        if returnPk:
            return pks[0]


    def execute(self, dbConn=None, doCommit=True):
        '''
            execute - Execute this action, generic method.

                    For SelectQuery, will call executeInsert

                @param dbConn <DatabaseConnection/None> Default None - If None, start a new connection
                        using the global connection settings. Otherwise, use given connection.

                @param doCommit <bool> Default True - If True, will commit right away. If False, you must commit.

                    A #dbConn must be specified if doCommit=False
        '''
        return self.executeInsert(dbConn=dbConn, doCommit=doCommit)


# vim: set ts=4 sw=4 st=4 expandtab:
