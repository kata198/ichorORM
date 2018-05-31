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

from .constants import WHERE_AND, WHERE_OR, WHERE_ALL_TYPES, ALL_JOINS, SQL_NULL
from .utils import convertFilterTypeToOperator, isMultiOperator
from .objs import DictObj


from collections import OrderedDict

from . import getDatabaseConnection

# TODO: Better handle stringing of potential filter values (like to addCondition),
#         Currently we have QueryStr as a special type and SQL_NULL as a special singleton
# TODO: Support "in" <iterable> for filter addCondition
class QueryStr(str):
    '''
        QueryStr - A portion that should be treated like a raw string (Embedded sql),
          and not a quoted value
    '''
    pass


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

                @param filterValue <str> - The value to match

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

        if isinstance(filterValue, QueryStr):
            filterValue = filterValue
        elif filterValue == SQL_NULL:
            filterValue = 'NULL'
        else:
            filterValue = ''.join(["'", filterValue, "'"])

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

        if isinstance(filterValue, QueryStr):
            # Raw embedded SQL
            ret += filterValue + " "
        elif filterValue is SQL_NULL:
            # A raw NULL
            ret += "NULL "

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

        expressions = [ _filterEm.toStr() for _filterEm in self.filters ]
        expressions = [ x for x in expressions if x.strip() ]

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

                @param filterStages <None/list<FilterType objs>> - Use this list of WHERE filter stages, or None to start
                    
                    with an empty list. Call #addStage to add a stage to the WHERE
        '''
        self.model = model

        if filterStages is None:
            filterStages = []

        self.filterStages = filterStages


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

    def getWhereClauseParams(self, whereJoin=WHERE_AND):
        '''
            getWhereClauseParams - Gets the "WHERE" portion (including the string 'WHERE'), parameterized and the parameters

              @see getWhereClause
        '''
        if not self.filterStages:
            return ( '', [] )

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


    @staticmethod
    def replaceSpecialValues(fieldValues):
        '''
            replaceSpecialValues - Replace special values (like NOW() ) with a fixed value.

                They are supported as values in parameterized input, BUT they won't make it back onto the object
                  without some sort of special RETURNING clause (which isn't implemented.)

                May be changed in the future.
        '''
        # TODO: Special values like "NOW()" are supported, but they won't get set back
        #        on the model. Maybe we need like a RETURNING clause? For now, just unroll it on the client.
        for fieldName in fieldValues.keys():
            if fieldValues[fieldName] in ('NOW()', 'current_timestamp'):
                # NOTE: Right now we are able to translate these direct values.
                #         If in the future we need to support things like  " NOW() + interval '1 day' "
                #           we will need to parse with regex and work it like that.
                fieldValues[fieldName] = datetime.datetime.now()


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
        self.orderBys.clear()


    def setLimitNum(self, limitNum):
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
        if self.selectFields == 'ALL':
            selectFields = self.getAllFieldNames()
        else:
            selectFields = self.selectFields

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

    def getSqlParameterizedValues(self):
        '''
            getSqlParameterizedValues - Get the sql command parameterized

                @return tuple< sql<str>, whereParams <list<FilterStage obj>> >
        '''

        (whereClause, whereParams) = self.getWhereClauseParams()
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


class SelectInnerJoinQuery(SelectQuery):
    '''
        SelectInnerJoinQuery - A SELECT query on multiple tables which supports inner join
    '''


    def __init__(self, models, selectFields='ALL', orderByField=None, orderByDir='', limitNum=None):
        '''
            __init__ - Create a SelectInnerJoinQuery

                @param models - list<DatabaseModel> - List of models to use

                @param selectFields < 'ALL' or list< fieldName<str>> Default "ALL" - ALL to do * or a list of field names to select.
                    
                    Should be prefixed with table name, like "tableName.fieldName"

                @param orderByField <None/str> Default None - Order by this field, if provided

                @param orderByDir <str> Default '' - ASC or DESC for ascneding/descending

                @param limitNum <int/None> default None - If provided, will return MAX this many results
        '''
        SelectQuery.__init__(self, None, selectFields=selectFields, orderByField=orderByField, orderByDir=orderByDir, limitNum=limitNum)

        self.models = models

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
        if self.selectFields == 'ALL':
            selectFields = self.getAllFieldNamesIncludingTable()
        else:
            selectFields = self.selectFields

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


    def getSqlParameterizedValues(self):
        '''
            getSqlParameterizedValues - Gets the SQL command to execute using parameterized values

                @return tuple ( sql<str>, whereParams list<str> )
        '''

        (whereClause, whereParams) = self.getWhereClauseParams()
        orderByClause = self.getOrderByStr()
        limitClause = self.getLimitStr()

        sql = """SELECT  %s  FROM  %s  %s  %s  %s""" %( self.getFieldsStr(), self.getTableNamesStr(), whereClause, orderByClause, limitClause )

        return (sql, whereParams)


    def executeGetObjs(self, parameterized=True, dbConn=None):
        '''
            executeGetObjs - Not supported for SelectInnerJoinQuery
        '''
        raise NotImplementedError('SelectInnerJoinQuery does not support executeGetObjs. Use executeGetRows or executeGetMapping instead.')

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

                @param selectFields <'ALL' or list<str>> - Default ALL for all fields, or a list of fields to SELECT on primary table

                @param orderByField <None/str> - Default None, if provided ORDER BY this field

                @param orderByDir <str> Default '', ASC or DESC for direction

                @param limitNum <None/int> default None - If provided, return max this many records
        '''
        SelectQuery.__init__(self, None, selectFields=selectFields, orderByField=orderByField, orderByDir=orderByDir, limitNum=limitNum)

        self.model = primaryModel
        self.models = [ self.model ]

        self.joins = []


    def joinModel(self, model, joinType, conditionGrouping=WHERE_AND):
        '''
            joinModel - Join to another model, using a join type and given condition grouping

                @param model <DatabaseModel> - Database model to use

                @param joinType <str> - A join type (see JOIN_* in constants.py)

                @param conditionGrouping <str> default AND, either WHERE_AND or WHERE_OR
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

    def getJoinClausesParams(self):
        '''
            getJoinClausesParams - Get the "join" clauses with paramertized parameters

        '''
        if not self.joins:
            return ( '', [] )

        innerJoinStrs = []
        innerJoinParams = {}

        stageNum = 0

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
        if self.selectFields == 'ALL':
            selectFields = self.getAllFieldNames()
        else:
            selectFields = self.selectFields

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


    def getSqlParameterizedValues(self):
        '''
            getSqlParameterizedValues - Get the SQL with parameterized values

                @return tuple( sql<str>, params<list<str>>)
        '''

        (whereClause, whereParams) = self.getWhereClauseParams()

        (joinClauses, joinParams) = self.getJoinClausesParams()

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
            executeGetObjs - Not supported on SelectInnerJoinQuery
        '''
        raise NotImplementedError('SelectInnerJoinQuery does not support executeGetObjs. Use executeGetRows or executeGetMapping instead.')

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

    def getSqlParameterizedValues(self):
        '''
            getSqlParameterizedValues - Get SQL with parameterized values
        '''

        (whereClause, whereParams) = self.getWhereClauseParams()

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

              @param newFieldValues <dict/None> - Either a dict of field name : newValue, or None to set later
        '''
        QueryBase.__init__(self, model, filterStages)

        if newFieldValues is None:
            newFieldValues = {}

        self.newFieldValues = newFieldValues


    def setFieldValue(self, fieldName, newValue):
        '''
            setFieldValue - Update a field to a new value
        '''
        self.newFieldValues[fieldName] = newValue

    def setNewFieldValues(self, newFieldValues):
        '''
            setNewFieldValues - Set the dict of all field updates
        '''
        self.newFieldValues = newFieldValues

    @property
    def hasAnyUpdates(self):
        return bool(self.newFieldValues)

    def getSetFieldsStr(self, replaceSpecialValues=True):
        '''
            getSetFieldsStr - Get the  X = "VALUE" , Y = "OTHER" portion of the SQL query
        '''

        ret = []

        if replaceSpecialValues:
            useNewFieldValues = copy.deepcopy(self.newFieldValues)

            self.replaceSpecialValues(useNewFieldValues)
        else:
            useNewFieldValues = self.newFieldValues

        for fieldName, newValue in useNewFieldValues.items():
            if isinstance(newValue, QueryStr):
                newValueStr = newValue
            elif newValue == SQL_NULL:
                newValueStr = 'NULL'
            else:
                newValueStr = "'%s'" %(newValue, )

            ret.append( " %s = %s " %(fieldName, newValueStr) )

        return ' , '.join(ret)

    def getSetFieldParamsAndValues(self, replaceSpecialValues=True):
        '''
            getSetFieldParamsAndValues - For parameterized values,

              @param replaceSpecialValues <bool, default True> - If True, will replace special values ( like NOW() )
                with their calculated value. @see QueryBase.replaceSpecialValues for more info.

                This returns a tuple of two values, the first is the paramertized marker to be used in the query,
                  the second is a list of values which should be passed alongside
        '''
        retParams = []
        retValues = {}

        if replaceSpecialValues:
            useNewFieldValues = copy.deepcopy(self.newFieldValues)

            self.replaceSpecialValues(useNewFieldValues)
        else:
            useNewFieldValues = self.newFieldValues

        argNum = 0

        for fieldName, fieldValue in useNewFieldValues.items():

            identifier = 'arg' + str(argNum)
            argNum += 1

            if isinstance(fieldValue, QueryStr):
                retParams.append( fieldName + ' = ' + fieldValue + " " )
            elif fieldValue == SQL_NULL:
                retParams.append( fieldName + " = NULL " )
            else:
                retParams.append( fieldName + '= %(' + identifier + ')s ' )
                retValues[identifier] = fieldValue

        return (retParams, retValues)


    def getSql(self, replaceSpecialValues=True):
        '''
            getSql - Get sql command to execute
        '''

        whereClause = self.getWhereClause()
        setFieldsStr = self.getSetFieldsStr(replaceSpecialValues=replaceSpecialValues)

        sql = """UPDATE  %s  SET  %s   %s"""  %( self.getTableName(), setFieldsStr, whereClause )

        return sql

    def getSqlParameterizedValues(self, replaceSpecialValues=True):
        '''
            getSqlParameterizedValues - Get the SQL to execute, parameterized version

              @param replaceSpecialValues <bool, default True> - If True, will replace special values ( like NOW() )
                with their calculated value. @see QueryBase.replaceSpecialValues for more info.
        '''

        paramValues = {}

        ( whereClause, whereParams ) = self.getWhereClauseParams()

        paramValues.update(whereParams)

        (setFieldParams, setFieldParamValues) = self.getSetFieldParamsAndValues(replaceSpecialValues=replaceSpecialValues)

        paramValues.update(setFieldParamValues)

        sql = """UPDATE  %s  SET  %s   %s"""  %( self.getTableName(), ', '.join(setFieldParams), whereClause )

        return (sql, paramValues)


    def executeUpdateRawValues(self, dbConn=None, doCommit=False, replaceSpecialValues=True):
        '''
            executeUpdate - Update some records

              @param dbConn <None/DatabaseConnection> - If None, will get a new connection with autocmommit.

              @param doCommit <bool> Default True - If True, will commit right away. If False, you must commit.

              @param replaceSpecialValues <bool> - True to replace special values before sending to SQL

                Nay be passed a transaction-connection, to do update within a transaction
        '''
        if not self.hasAnyUpdates:
            return

        if not doCommit and not dbConn:
            raise ValueError('doCommit=False but a dbConn not specified!')

        sql = self.getSql(replaceSpecialValues=replaceSpecialValues)

        if not dbConn:
            dbConn = getDatabaseConnection()

        dbConn.executeSql(sql)


    def executeUpdate(self, dbConn=None, doCommit=True, replaceSpecialValues=True):
        '''
            executeUpdate - Upate records (parameterized)

                May potentially use an existing DatabaseConnection (for transaction)

            @param dbConn <None/DatabaseConnection> - If None, will use a fresh connection and auto-commit.
               Otherwise, will use the provided connection (which may be linked to a transaction

            @param doCommit <bool> default True - Whether to commit immediately

            @param replaceSpecialValues <bool, default True> - If True, will replace special values ( like NOW() )
              with their calculated value. @see QueryBase.replaceSpecialValues for more info.


        '''
        if not self.hasAnyUpdates:
            return

        if not doCommit and not dbConn:
            raise ValueError('doCommit=False but a dbConn not specified!')

        (sqlParam, paramValues) = self.getSqlParameterizedValues(replaceSpecialValues=replaceSpecialValues)

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
        InsertQuery - A query for doing inserts
    '''

    def __init__(self, model, setFieldValues=None, filterStages=None):
        '''
            __init__ - Create an insert query

              @param model - The model to use

              @param setFieldValues - A dict of fieldName : fieldValue, or None to set later
        '''
        QueryBase.__init__(self, model, filterStages)

        if setFieldValues is None:
            setFieldValues = {}

        self.setFieldValues = setFieldValues

    def setFieldValue(self, fieldName, newValue):
        '''
            setFieldValue - Set a field to a value to be inserted
        '''
        self.setFieldValues[fieldName] = newValue

    def setNewFieldValues(self, setFieldValues):
        '''
            setNewFieldValues - Set the mapping of field names to values
        '''
        self.setFieldValues = setFieldValues

    def getTableFieldParamsAndValues(self, replaceSpecialValues=True):
        '''
            getTableFieldParamsAndValues - For parameterized values,

              @param replaceSpecialValues <bool, default True> - If True, will replace special values ( like NOW() )
                with their calculated value. @see QueryBase.replaceSpecialValues for more info.

                This returns a tuple of two values, the first is the paramertized marker to be used in the query,
                  the second is a list of values which should be passed alongside
        '''
        retParams = []
        retValues = {}

        if replaceSpecialValues:
            useSetFieldValues = copy.deepcopy(self.setFieldValues)

            self.replaceSpecialValues(useSetFieldValues)
        else:
            useSetFieldValues = self.setFieldValues

        for fieldName, fieldValue in useSetFieldValues.items():
            if isinstance(fieldValue, QueryStr):
                retParams.append(fieldValue)
            elif fieldValue == SQL_NULL:
                retParams.append('NULL')
            else:
                retParams.append( ' %(' + fieldName + ')s ' )
                retValues[fieldName] = fieldValue

        return (retParams, retValues)

    def getTableFields(self):
        '''
            getTableFields - Get a list of the fields that are going to be set
        '''
        return list(self.setFieldValues.keys())

    def getTableFieldsStr(self):
        '''
            getTableFieldsStr - Get the portion following the table name in an INSERT query which specifies
                                    the fields that will be set
        '''

        if not self.setFieldValues:
            # Should not be valid for an insert.. think about this
            return ''

        return ' ( %s ) ' %(', '.join( list(self.setFieldValues.keys()) ), )

    def getInsertValuesStr(self, replaceSpecialValues=True):
        '''
            getInsertValuesStr - Get the portion following VALUES with values directly within (not parameterized)


              @param replaceSpecialValues <bool, default True> - If True, will replace special values ( like NOW() )
                with their calculated value. @see QueryBase.replaceSpecialValues for more info.

        '''

        if not self.setFieldValues:
            # Should not be valid for an insert.. think about this
            return ''

        if replaceSpecialValues:
            useSetFieldValues = copy.deepcopy(self.setFieldValues)

            self.replaceSpecialValues(useSetFieldValues)
        else:
            useSetFieldValues = self.setFieldValues


        return ' ( %s ) ' %( ', '.join( [ not isinstance(val, QueryStr) and repr(val) or str(val) for val in useSetFieldValues.values() ] ), )


    def getSql(self, replaceSpecialValues=True):
        '''
            getSql - Get the SQL to execute, non-parameterized

              @param replaceSpecialValues <bool, default True> - If True, will replace special values ( like NOW() )
                with their calculated value. @see QueryBase.replaceSpecialValues for more info.

            @see getSqlParameterizedValues for parameterized version
        '''

        tableFieldsStr = self.getTableFieldsStr()
        insertValuesStr = self.getInsertValuesStr(replaceSpecialValues=replaceSpecialValues)
        whereClause = self.getWhereClause()

        sql = """INSERT INTO  %s %s  VALUES %s %s"""  %( self.getTableName(), tableFieldsStr, insertValuesStr, whereClause )

        return sql

    def getSqlParameterizedValues(self, replaceSpecialValues=True):
        '''
            getSqlParameterizedValues - Get the SQL to execute, parameterized version

              @param replaceSpecialValues <bool, default True> - If True, will replace special values ( like NOW() )
                with their calculated value. @see QueryBase.replaceSpecialValues for more info.
        '''

        tableFieldsStr = self.getTableFieldsStr()
        tableFieldParams, tableFieldValues = self.getTableFieldParamsAndValues(replaceSpecialValues)

        sql = """INSERT INTO  %s %s  VALUES ( %s ) """  %( self.getTableName(), tableFieldsStr, ', '.join(tableFieldParams) )

        return (sql, tableFieldValues)


    def executeInsertRawValues(self, dbConn=None, doCommit=True, replaceSpecialValues=True):
        '''
            executeInsertRawValues - Insert records  (non-parameterized)

              May potentially use an existing DatabaseConnection (for transaction)

            @param dbConn <None/DatabaseConnection> - If None, will use a fresh connection and auto-commit.
               Otherwise, will use the provided connection (which may be linked to a transaction

            @param doCommit <bool> default True - Whether to commit immediately

              @param replaceSpecialValues <bool, default True> - If True, will replace special values ( like NOW() )
                with their calculated value. @see QueryBase.replaceSpecialValues for more info.

            @see executeInsertParameterized for the parameterized version.
        '''
        sql = self.getSql(replaceSpecialValues=replaceSpecialValues)

        if not dbConn:
            dbConn = getDatabaseConnection(isTransactionMode=True)

        # TODO: Can probably use doInsert here to return the ID?
        dbConn.executeSql(sql)

        if doCommit:
            dbConn.commit()

    def executeInsert(self, dbConn=None, doCommit=True, replaceSpecialValues=True, returnPk=True):
        '''
            executeInsert - Insert records (parameterized)

                May potentially use an existing DatabaseConnection (for transaction)

            @param dbConn <None/DatabaseConnection> - If None, will use a fresh connection and auto-commit.
               Otherwise, will use the provided connection (which may be linked to a transaction

            @param doCommit <bool> default True - Whether to commit immediately

            @param replaceSpecialValues <bool, default True> - If True, will replace special values ( like NOW() )
              with their calculated value. @see QueryBase.replaceSpecialValues for more info.


            @see executeInsert for the non-parameterized version.
        '''

        sqlParam, paramValues = self.getSqlParameterizedValues(replaceSpecialValues=replaceSpecialValues)

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
