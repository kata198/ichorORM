'''
    Copyright (c) 2016-2018 Timothy Savannah

    Licensed under the terms of the Lesser GNU Lesser General Public License version 2.1
    
      license can be found at https://raw.githubusercontent.com/kata198/ichorORM/master/LICENSE

    model.py - ORM model base
'''

import copy

from .constants import FETCH_ALL_FIELDS, WHERE_AND, WHERE_OR, ALL_WHERE_TYPES, SQL_NULL
from . import DatabaseConnection, getDatabaseConnection

from .query import InsertQuery, UpdateQuery, SelectQuery, DeleteQuery

from .WhereClause import WhereClause

__all__ = ('DatabaseModel', )

class DatabaseModel(object):
    '''
        DatabaseModel - Models should extend this
    '''

    # TABLE_NAME - Set to the Postgresql table name
    TABLE_NAME = None

    # FIELDS - List of fields on this table
    FIELDS = []

    # REQUIRED_FIELDS - List of fields that are also required (NOT NULL)
    REQUIRED_FIELDS = []

    # DEFAULT_FIELD_VALUES - Field names to default values (when not specified, these will be used)
    DEFAULT_FIELD_VALUES = {}

    # SERIAL_PRIMARY_KEY - Set to True to indicate that the #PRIMARY_KEY is a serial sequence
    SERIAL_PRIMARY_KEY = True

    # PRIMARY_KEY - The name of the primary key on this table.
    #   It is HIGHLY recommended that every table have an "id serial primary key" for
    #     simplified/streamlined ORM usage
    PRIMARY_KEY = 'id'

    def __init__(self, **kwargs):
        '''
            __init__ - Create an object of this type.

              Arguments are in form of fieldName=fieldValue

        '''
        FIELDS = self.FIELDS
        DEFAULT_FIELD_VALUES = self.DEFAULT_FIELD_VALUES

        # TODO: Add some basic support for his some
        if not self.SERIAL_PRIMARY_KEY:
            raise NotImplementedError('Models without a serial sequenced primary key are not currently supported')

        # Get the field values passed to constructor.
        for fieldName in FIELDS:
            # Alias "id" field as "_id"
            if fieldName == 'id':
                if '_id' in kwargs:
                    fieldName = 'id'
                    value = kwargs['_id']
                else:
                    value = kwargs.get('id', None)
            else:
                defaultValue = DEFAULT_FIELD_VALUES.get(fieldName, None)

                value = kwargs.get(fieldName, defaultValue)

            setattr(self, fieldName, value)

    # TODO: rename to includePk?
    def asDict(self, includePk=True):
        '''
            asDict - Return a dict representation of this model
        '''
        primaryKeyName = self.PRIMARY_KEY
        ret = { fieldName : getattr(self, fieldName, None) for fieldName in self.FIELDS }
        if includePk is not True and primaryKeyName in self.FIELDS:
            ret.pop(primaryKeyName)

        return ret


    @classmethod
    def createAndSave(cls, replaceSpecialValues=True, dbConn=None, **kwargs):
        '''
            createAndSave - Creates an object of this type and saves it.

            Parameters are the fieldName=fieldValue for this model.

            At least all entries in REQUIRED_FIELDS must be present!


            @param replaceSpecialValues <bool, default True> - If True, will replace special values ( like NOW() )
              with their calculated value. @see QueryBase.replaceSpecialValues for more info.


            @param dbConn <None/DatabaseConnection> Default None- A specific DatabaseConnection to use,
                if None generate a new connection with global settings


            @return - An object of this model's type
        '''

        for reqField in cls.REQUIRED_FIELDS:
            if reqField not in kwargs and reqField not in cls.DEFAULT_FIELD_VALUES:
                raise ValueError('%s missing required field: %s' %(cls.__name__, repr(reqField)) )

        setDict = kwargs

        for defaultField in cls.DEFAULT_FIELD_VALUES.keys():
            if defaultField not in setDict:
                setDict[defaultField] = cls.DEFAULT_FIELD_VALUES[defaultField]

        if replaceSpecialValues is True:
            useSetDict = copy.deepcopy(setDict)

            InsertQuery.replaceSpecialValues(useSetDict)
        else:
            useSetDict = setDict


        q = InsertQuery(cls, setFieldValues=useSetDict)

        # Set replaceSpecialValues to False here, as we already handled it above.
        _pk = q.executeInsert(doCommit=True, replaceSpecialValues=False, dbConn=dbConn)

        useSetDict[cls.PRIMARY_KEY] = _pk

        return cls( **useSetDict )


    def insertObject(self, dbConn=None, doCommit=True):
        '''
            insertObject - Inserts current object

                @param dbConn <None/DatabaseConnection> Default None- A specific DatabaseConnection to use,
                    if None generate a new connection with global settings

                @param doCommit <bool> default True - If True, will commit upon insert.
                    If False, you must call dbConn.commitTransaction yourself when ready.
                    Primary key is set either way.
                    If doCommit is False, dbConn must be specified (obviously, so you can commit later)

              Will raise exception if object is already saved, or a REQUIRED_FIELDS is not present.
        '''
        primaryKeyName = self.PRIMARY_KEY

        if primaryKeyName in self.FIELDS and getattr(self, primaryKeyName, None) != None:
            raise ValueError('Object already saved [ %s = %s ]:   < %s >' %(primaryKeyName, getattr(self, primaryKeyName), repr(self)))

        if not doCommit and not dbConn:
            raise ValueError('When doCommit=False, dbConn must be specified. Try connection.getDatabaseConnection()')

        setDict = {}
        for fieldName in self.FIELDS:
            fieldValue = getattr(self, fieldName, None)
            if fieldValue is not None:
                setDict[fieldName] = fieldValue

        for reqField in self.REQUIRED_FIELDS:
            if reqField not in setDict:
                raise ValueError('%s missing required field: %s' %(self.__class__.__name__, repr(reqField)) )

        q = InsertQuery(self.__class__, setFieldValues=setDict)

        # returnPk works here whether in a transaction or not because of isolation level,
        #   the select from sequence is executed outside of the transaction
        _pk = q.executeInsert(doCommit=doCommit, dbConn=dbConn, returnPk=True)

        setattr(self, self.PRIMARY_KEY, _pk)

        return self

    # TODO: Support transaction here?
    def updateObject(self, updateFieldNames, dbConn=None, doCommit=True):
        '''
            updateObject - Performs an UPDATE on a given list of field names, based on value held on current object.

                @param updateFieldNames < list<str> > - A list of field names to update.

                @param dbConn <None/DatabaseConnection> Default None- A specific DatabaseConnection to use,
                    if None generate a new connection with global settings

                @param doCommit <bool> default True - If True, will commit upon insert.
                    If False, you must call dbConn.commitTransaction yourself when ready.
                    Primary key is set either way.
                    If doCommit is False, dbConn must be specified (obviously, so you can commit later)


              Will raise exception if current object is not saved.
        '''
        primaryKeyName = self.PRIMARY_KEY

        if primaryKeyName in self.FIELDS and not getattr(self, primaryKeyName, None):
            raise ValueError('Asked to update but object is not saved:  < %s >' %(repr(self), ))

        if not doCommit and not dbConn:
            raise ValueError('When doCommit=False, dbConn must be specified. Try connection.getDatabaseConnection()')

        newFieldValues = { fieldName : getattr(self, fieldName) for fieldName in updateFieldNames }

        q = UpdateQuery(self.__class__, newFieldValues)

        where = q.addStage()

        where.addCondition(primaryKeyName, '=', getattr(self, primaryKeyName))

        q.executeUpdate(dbConn=dbConn, doCommit=doCommit)


    @classmethod
    def get(cls, _pk, dbConn=None):
        '''
            get - Gets a single object of this model type by primary key (id)

            @param _pk <str/int> - Value of primary key"

            @param dbConn <None/DatabaseConnection> Default None- A specific DatabaseConnection to use,
                        if None generate a new connection with global settings

            @return object of this type with all fields populated
        '''

        primaryKeyName = cls.PRIMARY_KEY

        _pk = str(_pk)

        q = SelectQuery(cls, selectFields='ALL', limitNum=1)

        where = q.addStage()

        where.addCondition(primaryKeyName, '=', _pk)

        objs = q.executeGetObjs(dbConn=dbConn)

        if len(objs) != 1:
            raise KeyError('No such %s object [ %s ] with %s=%s' %(cls.__name__, cls.TABLE_NAME, primaryKeyName, _pk) )

        return objs[0]


    def delete(self, dbConn=None):
        '''
            delete - Delete current object.
                Should generally NOT be used, instead things should be "archived" to the best of ability.

            Will clear the primary key field on this object.

            If object is already deleted, this will return None

            @param dbConn <None/DatabaseConnection> Default None- A specific DatabaseConnection to use,
                        if None generate a new connection with global settings

            @return - Old ID
        '''
        primaryKeyName = self.PRIMARY_KEY

        _pk = getattr(self, primaryKeyName, None)
        if not _pk:
            return None

        q = DeleteQuery(self)

        where = q.addStage()

        where.addCondition(primaryKeyName, '=', _pk)

        q.executeDelete(dbConn=dbConn)

        setattr(self, primaryKeyName, None)

        return _pk

    @classmethod
    def filter(cls, whereType=WHERE_AND, dbConn=None, **kwargs):
        '''
            filter - Filter and return objects of this type

              @param whereType <WHERE_AND/WHERE_OR> - Whether filter criteria should be AND or OR'd together

              @param dbConn <DatabaseConnection/None> default None- If present and not None, 
                  will use this as the postgres connection.
                  Otherwise, will start a new connection based on global settings

              Optionals:

                @param orderByField - If present, results will be ordered using this field

                @param orderByDir - If present, ordered results will follow this direction


              All other parameters should be in the form  "fieldName=value" for equality comparison,
                otherwise fieldName should end with __OPERATION, e.x.   fieldName__ne=value for not-equals,
                fieldName__like="Start%End" for like, etc.

              @return list<objs> - List of objects of this model type
        '''
        if whereType not in ALL_WHERE_TYPES:
            raise ValueError('Unknown where type: %s.   Possible types:  %s.' %(repr(whereType), repr(ALL_WHERE_TYPES)))

        if 'orderByField' in kwargs:
            orderByField = kwargs.pop('orderByField')
        else:
            orderByField = None

        if 'orderByDir' in kwargs:
            orderByDir = kwargs.pop('orderByDir')
        else:
            orderByDir = ''

        q = SelectQuery(cls, orderByField=orderByField, orderByDir=orderByDir)

        where = q.addStage(whereType)

        for fieldName, fieldValue in kwargs.items():
            if '__' in fieldName:
                try:
                    fieldName, operation = fieldName.split('__')
                except:
                    raise ValueError('Unknown filter param: "%s". double-underscore should be followed by an operation, e.x. __ne' %(fieldName, ))
            else:
                operation = '='

            if fieldValue is None or fieldValue is SQL_NULL:
                if operation in ('eq', '='):
                    operation = 'is'
                elif operation == 'ne':
                    operation = 'is not'
            where.addCondition(fieldName, operation, fieldValue)

        objs = q.executeGetObjs(dbConn=dbConn)
        return objs

    @classmethod
    def all(cls, orderByField=None, orderByDir='', dbConn=None):
        '''
            all - Get all objects associated with this model

                @param orderByField <None/str> Default None, if provided the list returned
                    will be ordered by this sql field

                @param orderByDir <str> Default empty string, if provided the list will be ordered
                    in this direction (DESC or ASC)

                @param dbConn <None/DatabaseConnection> Default None- A specific DatabaseConnection to use,
                    if None generate a new connection with global settings

                @return list<DatabaseModel> - A list of all objects in the database for this model
        '''

        q = SelectQuery(cls, orderByField=orderByField, orderByDir=orderByDir)

        objs = q.executeGetObjs(dbConn=dbConn)
        return objs


    def __repr__(self):
        '''
            __repr__ - Get object representation of this instance

                @return <str> - A descriptive string of this object
        '''
        primaryKeyName = self.PRIMARY_KEY

        objDict = self.asDict(includePk=True)
        fieldValues = []

        # Ensure PRIMARY KEY ("id") field always comes first
        if primaryKeyName in objDict:
            fieldValues.append('%s=%s' %( primaryKeyName, repr(objDict.pop(primaryKeyName)), ) )

        fieldValues += [ "%s=%s" %( fieldName, repr(fieldValue) ) for fieldName, fieldValue in objDict.items() ]

        return "%s( %s )" % \
              ( self.__class__.__name__, \
                ' , '.join(fieldValues) \
        )

# vim: set ts=4 sw=4 expandtab :
