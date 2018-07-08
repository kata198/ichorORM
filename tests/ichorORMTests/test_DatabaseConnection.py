#!/usr/bin/env GoodTests.py
'''
    Test of the DatabaseConnection class
'''

import subprocess
import sys


import LocalConfig


import ichorORM
from ichorORM import getDatabaseConnection


class TestDatabaseConnection(object):
    '''
        A test for the DatabaseConnection class
    '''

    def setup_class(self):
        '''
            setup_class - Called at the beginning of the test
                
                Sets configuration based on LocalConfig
        '''
        LocalConfig.ensureTestSetup()


    def test_configApplied(self):
        '''
            test_configApplied - Test that the config from LocalConfig was applied properly
        '''

        gotException = False
        try:
           dbConn = ichorORM.getDatabaseConnection()
        except Exception as e:
           gotException = e

        assert gotException is False , 'Got exception calling getDatabaseConnection:  %s  %s' %( str(type(gotException)), str(gotException) )

         
        assert dbConn.host == LocalConfig._CONFIG_HOSTNAME , 'Expected LocalConfig._CONFIG_HOSTNAME to be set on database connection. Expected %s but got %s' %( repr(LocalConfig._CONFIG_HOSTNAME), repr(dbConn.host) )

        assert dbConn.port == LocalConfig._CONFIG_PORT , 'Expected LocalConfig._CONFIG_PORT to be set on database connection. Expected %s but got %s' %( repr(LocalConfig._CONFIG_PORT), repr(dbConn.port) )

        assert dbConn.user == LocalConfig._CONFIG_USERNAME , 'Expected LocalConfig._CONFIG_USERNAME to be set on database connection. Expected %s but got %s' %( repr(LocalConfig._CONFIG_USERNAME), repr(dbConn.user) )

        assert dbConn.dbname == LocalConfig._CONFIG_DBNAME , 'Expected LocalConfig._CONFIG_DBNAME to be set on database connection. Expected %s but got %s' %( repr(LocalConfig._CONFIG_DBNAME), repr(dbConn.dbname) )

        assert dbConn.password == LocalConfig._CONFIG_PASSWORD , 'Expected LocalConfig._CONFIG_PASSWORD to be set on database connection. Expected %s but got %s' %( repr(LocalConfig._CONFIG_PASSWORD), repr(dbConn.password) )


    def test_getDatabaseConnection(self):
        '''
            Test the getDatabaseConnection method more in depth
        '''
        ALT_HOST = 'X_MY_HOST'
        ALT_PORT = 9999
        ALT_DB_NAME = 'X_MY_DB'
        ALT_USER = 'X_USER'
        ALT_PASS = 'X_PASS'

        dbConnGlobal = ichorORM.getDatabaseConnection()

        dbConnAlt = ichorORM.getDatabaseConnection(host=ALT_HOST, port=ALT_PORT, dbname=ALT_DB_NAME, user=ALT_USER, password=ALT_PASS)

        assert dbConnAlt.host == ALT_HOST , 'Expected ALT_HOST to be set on database connection. Expected %s but got %s' %( repr(ALT_HOST), repr(dbConnAlt.host) )

        assert dbConnAlt.port == ALT_PORT , 'Expected ALT_PORT to be set on database connection. Expected %s but got %s' %( repr(ALT_PORT), repr(dbConnAlt.port) )

        assert dbConnAlt.user == ALT_USER , 'Expected ALT_USER to be set on database connection. Expected %s but got %s' %( repr(ALT_USER), repr(dbConnAlt.user) )

        assert dbConnAlt.dbname == ALT_DB_NAME , 'Expected ALT_DB_NAME to be set on database connection. Expected %s but got %s' %( repr(ALT_DB_NAME), repr(dbConnAlt.dbname) )

        assert dbConnAlt.password == ALT_PASS , 'Expected ALT_PASS to be set on database connection. Expected %s but got %s' %( repr(ALT_PASS), repr(dbConnAlt.password) )


        # Now, try a couple fields and see that we properly inherit global

        for attrName, attrValue in [ ('host', ALT_HOST), ('port', ALT_PORT), ('user', ALT_USER), ('dbname', ALT_DB_NAME), ('password', ALT_PASS) ]:
            args = { attrName : attrValue }

            dbConnAlt = ichorORM.getDatabaseConnection(**args)

            
            assert getattr(dbConnAlt, attrName) == attrValue , 'Expected to set %s=%s but object had value %s.' %( attrName, repr(attrValue), repr(getattr(dbConnAlt, attrName)) )

            for connField in ( 'host', 'port', 'user', 'dbname', 'password' ):
                if connField == attrName:
                    # Skip if this is the field we modified
                    continue
    
                globalVal = getattr(dbConnGlobal, connField)
                altVal = getattr(dbConnAlt, connField)

                assert globalVal == altVal , 'Expected that when changing field %s but not %s we would inherit global value for %s. Expected %s but got %s' %( attrName, connField, connField, repr(globalVal), repr(altVal) ) 


    def test_getCursor(self):
        '''
            test_getCursor - Test if we can obtain a cursor with global connection params
        '''

        dbConn = ichorORM.getDatabaseConnection()

        dbCursor = dbConn.getCursor()

        assert dbCursor , 'Expected to be able to get a cursor, but did not.'


    def _dropTestTable(self):
        dbConn = ichorORM.getDatabaseConnection()

        dbConn.executeSql('DROP TABLE ichor_test_conn_table')

    def _createTestTable(self, dropIfExists=True):
        '''
            _createTestTable - Create a table for this test

                @param dropIfExists <bool> default True - If True, will attempt to drop table first (if exists)
        '''
        if dropIfExists:
            try:
                self._dropTestTable()
            except:
                pass

        dbConn = ichorORM.getDatabaseConnection()
        dbConn.executeSql('CREATE TABLE ichor_test_conn_table(id serial PRIMARY KEY, name varchar(255) NOT NULL, value varchar(255) NOT NULL, extra_data text NULL)')


    def test_executeSql(self):
        '''
            test_executeSql - Test executing SQL commands
        '''

        dbConn = ichorORM.getDatabaseConnection()

        gotException = False
        try:
            self._createTestTable(dropIfExists=True)
        except Exception as e:
            gotException = e

        assert gotException is False , 'Expected to be able to create a table with executeSql. Got exception %s:  %s' %(str(type(gotException)), str(gotException))

        def _insertIntoTestTable(dbConn, name, value, extra_data=None):
            '''
                _insertIntoTestTable - Insert data into test table

                    @param name <str> - Name

                    @param value <str> - value

                    @param extra_data <None/str> default None - Extra_data field
            '''
            if extra_data == None:
                extraDataVal = 'NULL'
            else:
                extraDataVal = "'%s'" %(extra_data, )

            query = '''INSERT INTO ichor_test_conn_table(name, value, extra_data) VALUES ('%s', '%s', %s)''' %(name, value, extraDataVal)
            dbConn.executeSql(query)
        

        gotException = False
        try:
            _insertIntoTestTable(dbConn, 'one', 'Hello', None)
            _insertIntoTestTable(dbConn, 'two', 'Goodbye', None)
            _insertIntoTestTable(dbConn, 'three', 'Goodbye', 'Some extra data')
        except Exception as e:
            gotException = e

        assert gotException is False , 'Got exception inserting test data using executeSql: %s  %s' %(str(type(gotException)), str(gotException))


    def test_executeSqlError(self):
        '''
            test_executeSqlError - Test that errors are correctly raised with executeSql
        '''
        dbConn = getDatabaseConnection()

        gotException = False
        try:
            dbConn.executeSql('SELECT booooh FIGGLE whop')
        except Exception as e:
            gotException = e

        assert gotException is not False , 'Expected to get exception from executeSql with nonsense query, but did not.'

   
    def test_doSelect(self):
        '''
            test_doSelect - Test the doSelect method
        '''
        # Perform inserts and such here
        try:
            self.test_executeSql()
        except AssertionError as e:
            raise AssertionError('Unable to run test_doSelect because test_executeSql failed.')

        dbConn = getDatabaseConnection()

        query = 'SELECT id, name, value, extra_data FROM ichor_test_conn_table'

        results = dbConn.doSelect(query)

        assert results , 'Expected to get results from select query, but did not.'

        assert len(results) == 3 , 'Expected to get 3 rows from doSelect, but got %d.  %s' %( len(results), repr(results))

        foundOne = False
        foundTwo = False
        foundThree = False

            #_insertIntoTestTable(dbConn, 'one', 'Hello', None)
            #_insertIntoTestTable(dbConn, 'two', 'Goodbye', None)
            #_insertIntoTestTable(dbConn, 'three', 'Goodbye', 'Some extra data')
        for result in results:
            assert len(result) == 4  , 'Expected 4 columns in result (id, name, value, extra_data) but got %d. %s' %(len(result), repr(result))

            (fetchedId, fetchedName, fetchedValue, fetchedExtraData) = result

            if fetchedName == 'one':
                foundOne = True
                assert fetchedValue == 'Hello' , 'Got mixed up data. Expected name="one" to have value="Hello". Row was: ' + repr(result)
                assert fetchedExtraData is None , 'Expected NULL result to have value of None. Got: %s' %(repr(fetchedExtraData), )

            elif fetchedName == 'two':
                foundTwo = True
                assert fetchedValue == 'Goodbye' , 'Got mixed up data. Expected name="two" to have value="Goodbye". Row was: ' + repr(result)
                assert fetchedExtraData is None , 'Expected NULL result to have value of None. Got: %s' %(repr(fetchedExtraData), )

            elif fetchedName == 'three':
                foundThree = True
                assert fetchedValue == 'Goodbye' , 'Got mixed up data. Expected name="three" to have value="Goodbye". Row was: ' + repr(result)
                assert fetchedExtraData == 'Some extra data' , 'Expected extra data on name="three" to be "Some extra data" but got %s' %(repr(fetchedExtraData), )
            else:
                raise AssertionError("Got unknown row: %s" %(repr(result), ))

        assert foundOne , 'Did not find name="one"'
        assert foundTwo , 'Did not find name="two"'
        assert foundThree , 'Did not find name="three"'


    def test_executeSqlParams(self):
        '''
            test_executeSqlParams - Test executing SQL commands with params
        '''

        dbConn = ichorORM.getDatabaseConnection()

        gotException = False
        try:
            self._createTestTable(dropIfExists=True)
        except Exception as e:
            gotException = e

        assert gotException is False , 'Expected to be able to create a table with executeSql. Got exception %s:  %s' %(str(type(gotException)), str(gotException))

        def _insertIntoTestTable(dbConn, name, value, extra_data=None):
            '''
                _insertIntoTestTable - Insert data into test table

                    @param name <str> - Name

                    @param value <str> - value

                    @param extra_data <None/str> default None - Extra_data field
            '''
            query = '''INSERT INTO ichor_test_conn_table(name, value, extra_data) VALUES (%(name)s, %(value)s, %(extra_data)s)''' 
            dbConn.executeSqlParams(query, {'name' : name, 'value' : value, 'extra_data' : extra_data })
        

        gotException = False
        try:
            nextOne = 'one'
            _insertIntoTestTable(dbConn, 'one', 'Hello', None)
            nextOne = 'two'
            _insertIntoTestTable(dbConn, 'two', 'Goodbye', None)
            nextOne = 'three'
            _insertIntoTestTable(dbConn, 'three', 'Goodbye', 'Some extra data')
        except Exception as e:
            gotException = e

        assert gotException is False , 'Got exception inserting test data name=%s using executeSqlParams: %s  %s' %(repr(nextOne), str(type(gotException)), str(gotException))


    def test_executeSqlParamsError(self):
        '''
            test_executeSqlParamsError - Test that errors are correctly raised with executeSqlParams
        '''
        dbConn = getDatabaseConnection()

        gotException = False
        try:
            dbConn.executeSqlParams('SELECT booooh FIGGLE whop')
        except Exception as e:
            gotException = e

        assert gotException is not False , 'Expected to get exception from executeSqlParams with nonsense query, but did not.'

        gotException = False
        try:
            query = '''INSERT INTO ichor_test_conn_table(name, value, extra_data) VALUES (%(name)s, %(value)s, %(extra_data)s)''' 
            dbConn.executeSqlParams(query, {'nameX' : name, 'value' : value, 'extra_data' : extra_data })
        except Exception as e:
            gotException = e

        assert gotException is not False , 'Expected to get exception from executeSqlParams with a param that does not match expected values.'

   
    def test_doSelectFromParams(self):
        '''
            test_doSelectFromParams - Test the doSelect method (using the params insert)
        '''
        # Perform inserts and such here
        try:
            self.test_executeSqlParams()
        except AssertionError as e:
            raise AssertionError('Unable to run test_doSelectFromParams because test_executeSqlParams failed.')

        dbConn = getDatabaseConnection()

        query = 'SELECT id, name, value, extra_data FROM ichor_test_conn_table'

        results = dbConn.doSelect(query)

        assert results , 'Expected to get results from select query, but did not.'

        assert len(results) == 3 , 'Expected to get 3 rows from doSelect, but got %d.  %s' %( len(results), repr(results))

        foundOne = False
        foundTwo = False
        foundThree = False

            #_insertIntoTestTable(dbConn, 'one', 'Hello', None)
            #_insertIntoTestTable(dbConn, 'two', 'Goodbye', None)
            #_insertIntoTestTable(dbConn, 'three', 'Goodbye', 'Some extra data')
        for result in results:
            assert len(result) == 4  , 'Expected 4 columns in result (id, name, value, extra_data) but got %d. %s' %(len(result), repr(result))

            (fetchedId, fetchedName, fetchedValue, fetchedExtraData) = result

            if fetchedName == 'one':
                foundOne = True
                assert fetchedValue == 'Hello' , 'Got mixed up data. Expected name="one" to have value="Hello". Row was: ' + repr(result)
                assert fetchedExtraData is None , 'Expected NULL result to have value of None. Got: %s' %(repr(fetchedExtraData), )

            elif fetchedName == 'two':
                foundTwo = True
                assert fetchedValue == 'Goodbye' , 'Got mixed up data. Expected name="two" to have value="Goodbye". Row was: ' + repr(result)
                assert fetchedExtraData is None , 'Expected NULL result to have value of None. Got: %s' %(repr(fetchedExtraData), )

            elif fetchedName == 'three':
                foundThree = True
                assert fetchedValue == 'Goodbye' , 'Got mixed up data. Expected name="three" to have value="Goodbye". Row was: ' + repr(result)
                assert fetchedExtraData == 'Some extra data' , 'Expected extra data on name="three" to be "Some extra data" but got %s' %(repr(fetchedExtraData), )
            else:
                raise AssertionError("Got unknown row: %s" %(repr(result), ))

        assert foundOne , 'Did not find name="one"'
        assert foundTwo , 'Did not find name="two"'
        assert foundThree , 'Did not find name="three"'


    def test_doInsert(self):
        '''
            test_doInsert - Test insert via doInsert
        '''

        self.nameToPk = {}

        dbConn = ichorORM.getDatabaseConnection()

        gotException = False
        try:
            self._createTestTable(dropIfExists=True)
        except Exception as e:
            gotException = e

        assert gotException is False , 'Expected to be able to create a table with executeSql. Got exception %s:  %s' %(str(type(gotException)), str(gotException))


        # Insert with one valueDict
        queryParams = '''INSERT INTO ichor_test_conn_table(name, value, extra_data) VALUES (%(name)s, %(value)s, %(extra_data)s)''' 
        gotException = False
        try:
            pks = dbConn.doInsert(queryParams, valueDicts=[{'name' : 'one', 'value' : 'Hello', 'extra_data' : None }], returnPk=True)
        except Exception as e:
            gotException = e

        assert gotException is False , 'Got unexpected exception from doInsert: %s  %s' %(str(type(gotException)), str(gotException))
        assert pks and len(pks) == 1 and pks[0] , 'Expected to get primary key back from doInsert, but did not.'
        self.nameToPk['one'] = pks[0]
        
        # insert with inline values and no valueDict
        queryInline = '''INSERT INTO ichor_test_conn_table(name, value, extra_data) VALUES ('two', 'Goodbye', NULL)''' 
        gotException = False
        try:
            pks = dbConn.doInsert(queryInline, returnPk=True)
        except Exception as e:
            gotException = e

        assert gotException is False , 'Got unexpected exception from doInsert: %s  %s' %(str(type(gotException)), str(gotException))
        assert pks and len(pks) == 1 and pks[0] , 'Expected to get primary key back from doInsert, but did not.'
        self.nameToPk['two'] = pks[0]
        
        # insert multiple valueDicts
        valueDicts = [ 
            { 'name' : 'three', 'value' : 'Goodbye', 'extra_data' : 'Some extra data' },
            { 'name' : 'four', 'value' : 'Cheese', 'extra_data' : 'Yummy yum yum'},
        ]
        gotException = False
        try:
            pks = dbConn.doInsert(queryParams, valueDicts=valueDicts, returnPk=True)
        except Exception as e:
            gotException = e

        assert gotException is False , 'Got unexpected exception from doInsert: %s  %s' %(str(type(gotException)), str(gotException))
        assert pks , 'Expected to get primary keys back from doInsert, but did not.'
        assert len(pks) == 2 , 'Expected to get 2 primary keys back from insert with 2 rows, but got %d  %s' %(len(pks), repr(pks))

        assert len([x for x in pks if x]) == 2, 'Expected to have primary keys set, but got empty/missing values. Got: ' + repr(pks)
        
        self.nameToPk['three'] = pks[0]
        self.nameToPk['four'] = pks[1]
        # TODO: Test returnPk = False


    def test_doInsertError(self):
        '''
            test_doInsertError
        '''
        dbConn = getDatabaseConnection()

        gotException = False
        try:
            dbConn.doInsert("INSERT blargiety(id, value) VALUES ('1', '2')")
        except Exception as e:
            gotException = e

        assert gotException is not False , 'Expected to get exception from doInsert with nonsense query, but did not.'

        gotException = False
        try:
            query = '''INSERT INTO ichor_test_conn_table(name, value, extra_data) VALUES (%(name)s, %(value)s, %(extra_data)s)''' 
            dbConn.doInsert(query, valueDicts=[{'nameX' : name, 'value' : value, 'extra_data' : extra_data }])
        except Exception as e:
            gotException = e

        assert gotException is not False , 'Expected to get exception from doInsert with a param that does not match expected values.'


    def test_doSelectFromDoInsert(self):
        '''
            test_doSelectFromDoInsert - Test the doSelect method (using the doInsert insert)
        '''
        # Perform inserts and such here
        try:
            self.test_doInsert()
        except AssertionError as e:
            raise AssertionError('Unable to run test_doSelectFromDoInsert because test_doInsert() failed.')

        dbConn = getDatabaseConnection()

        query = 'SELECT id, name, value, extra_data FROM ichor_test_conn_table'

        results = dbConn.doSelect(query)

        assert results , 'Expected to get results from select query, but did not.'

        assert len(results) == 4 , 'Expected to get 4 rows from doSelect, but got %d.  %s' %( len(results), repr(results))

        foundOne = False
        foundTwo = False
        foundThree = False
        foundFour = False

        for result in results:
            assert len(result) == 4  , 'Expected 4 columns in result (id, name, value, extra_data) but got %d. %s' %(len(result), repr(result))

            (fetchedId, fetchedName, fetchedValue, fetchedExtraData) = result

            if fetchedName == 'one':
                foundOne = True
                assert fetchedValue == 'Hello' , 'Got mixed up data. Expected name="one" to have value="Hello". Row was: ' + repr(result)
                assert fetchedExtraData is None , 'Expected NULL result to have value of None. Got: %s' %(repr(fetchedExtraData), )

                assert fetchedId == self.nameToPk['one'] , 'Expected pk for name="one" to match the one returned by doInsert. Got %s but expected %s' %( repr(fetchedId), repr(self.nameToPk['one']))

            elif fetchedName == 'two':
                foundTwo = True
                assert fetchedValue == 'Goodbye' , 'Got mixed up data. Expected name="two" to have value="Goodbye". Row was: ' + repr(result)
                assert fetchedExtraData is None , 'Expected NULL result to have value of None. Got: %s' %(repr(fetchedExtraData), )

                assert fetchedId == self.nameToPk['two'] , 'Expected pk for name="two" to match the two returned by doInsert. Got %s but expected %s' %( repr(fetchedId), repr(self.nameToPk['two']))

            elif fetchedName == 'three':
                foundThree = True
                assert fetchedValue == 'Goodbye' , 'Got mixed up data. Expected name="three" to have value="Goodbye". Row was: ' + repr(result)
                assert fetchedExtraData == 'Some extra data' , 'Expected extra data on name="three" to be "Some extra data" but got %s' %(repr(fetchedExtraData), )

                assert fetchedId == self.nameToPk['three'] , 'Expected pk for name="three" to match the three returned by doInsert. Got %s but expected %s' %( repr(fetchedId), repr(self.nameToPk['three']))

            elif fetchedName == 'four':
                foundFour = True
                assert fetchedValue == 'Cheese' , 'Got mixed up data. Expected name="four" to have value="Cheese". Row was: ' + repr(result)
                assert fetchedExtraData == 'Yummy yum yum' , 'Expected extra data on name="four" to be "Yummy yum yum" but got %s' %(repr(fetchedExtraData), )

                assert fetchedId == self.nameToPk['four'] , 'Expected pk for name="four" to match the four returned by doInsert. Got %s but expected %s' %( repr(fetchedId), repr(self.nameToPk['four']))

            else:
                raise AssertionError("Got unknown row: %s" %(repr(result), ))

        assert foundOne , 'Did not find name="one"'
        assert foundTwo , 'Did not find name="two"'
        assert foundThree , 'Did not find name="three"'
        assert foundFour , 'Did not find name="four"'


    def test_doInsertTransaction(self):
        '''
            test_doInsertTransaction - Test insert via doInsert using a transaction
        '''
        self.nameToPk = {}

        dbConn = ichorORM.getDatabaseConnection(isTransactionMode=True)

        gotException = False
        try:
            self._createTestTable(dropIfExists=True)
        except Exception as e:
            gotException = e

        assert gotException is False , 'Expected to be able to create a table with executeSql. Got exception %s:  %s' %(str(type(gotException)), str(gotException))


        # Insert with one valueDict
        queryParams = '''INSERT INTO ichor_test_conn_table(name, value, extra_data) VALUES (%(name)s, %(value)s, %(extra_data)s)''' 
        gotException = False
        try:
            pks = dbConn.doInsert(queryParams, valueDicts=[{'name' : 'one', 'value' : 'Hello', 'extra_data' : None }], returnPk=True, doCommit=False)
        except Exception as e:
            gotException = e

        assert gotException is False , 'Got unexpected exception from doInsert with transaction: %s  %s' %(str(type(gotException)), str(gotException))
        assert pks and len(pks) == 1 and pks[0] , 'Expected to get primary key back from doInsert, but did not.'
        self.nameToPk['one'] = pks[0]
        
        # insert with inline values and no valueDict
        queryInline = '''INSERT INTO ichor_test_conn_table(name, value, extra_data) VALUES ('two', 'Goodbye', NULL)''' 
        gotException = False
        try:
            pks = dbConn.doInsert(queryInline, returnPk=True, doCommit=False)
        except Exception as e:
            gotException = e

        assert gotException is False , 'Got unexpected exception from doInsert with transaction: %s  %s' %(str(type(gotException)), str(gotException))
        assert pks and len(pks) == 1 and pks[0] , 'Expected to get primary key back from doInsert, but did not.'
        self.nameToPk['two'] = pks[0]
        
        # insert multiple valueDicts
        valueDicts = [ 
            { 'name' : 'three', 'value' : 'Goodbye', 'extra_data' : 'Some extra data' },
            { 'name' : 'four', 'value' : 'Cheese', 'extra_data' : 'Yummy yum yum'},
        ]
        gotException = False
        try:
            pks = dbConn.doInsert(queryParams, valueDicts=valueDicts, returnPk=True, doCommit=False)
        except Exception as e:
            gotException = e

        assert gotException is False , 'Got unexpected exception from doInsert with transaction: %s  %s' %(str(type(gotException)), str(gotException))
        assert pks , 'Expected to get primary keys back from doInsert with transaction, but did not.'
        assert len(pks) == 2 , 'Expected to get 2 primary keys back from insert with 2 rows, but got %d  %s' %(len(pks), repr(pks))

        assert len([x for x in pks if x]) == 2, 'Expected to have primary keys set, but got empty/missing values. Got: ' + repr(pks)
        self.nameToPk['three'] = pks[0]
        self.nameToPk['four'] = pks[1]

        # Verify that there are no values because we have not committed
        dbConn2 = getDatabaseConnection()

        countResults = dbConn2.doSelect('SELECT count(*) FROM ichor_test_conn_table')
        assert countResults , 'Did not get any return from SELECT count(*) query'

        assert len(countResults) == 1 , 'Expected count(*) query to return 1 row, but got %d.  %s' %(len(countResults), repr(countResults))
        
        assert countResults[0][0] == 0 , 'Expected no rows to be present before commit with doInsert using transaction. count(*) returned %s' %(repr(countResults), )

        # commit
        gotException = False
        try:
            dbConn.commit()
        except Exception as e:
            gotException = e
        # TODO: Also test error on commit

        assert gotException is False , 'Got an error trying to commit transaction: %s  %s' %(str(type(gotException)), str(gotException))

        # Count should now be 4
        countResults = dbConn2.doSelect('SELECT count(*) FROM ichor_test_conn_table')
        assert countResults , 'Did not get any return from SELECT count(*) query'

        assert len(countResults) == 1 , 'Expected count(*) query to return 1 row, but got %d.  %s' %(len(countResults), repr(countResults))
        
        assert countResults[0][0] == 4 , 'Expected 4 rows to be present after commit with doInsert using transaction. count(*) returned %s' %(repr(countResults), )


    def test_doSelectFromDoInsertTrans(self):
        '''
            test_doSelectFromDoInsertTrans - Test the doSelect method (using the doInsert insert with transaction)
        '''
        # Perform inserts and such here
        try:
            self.test_doInsertTransaction()
        except AssertionError as e:
            raise AssertionError('Unable to run test_doSelectFromDoInsertTrans because test_doInsertTransaction() failed.')

        dbConn = getDatabaseConnection()

        query = 'SELECT id, name, value, extra_data FROM ichor_test_conn_table'

        results = dbConn.doSelect(query)

        assert results , 'Expected to get results from select query, but did not.'

        assert len(results) == 4 , 'Expected to get 4 rows from doSelect, but got %d.  %s' %( len(results), repr(results))

        foundOne = False
        foundTwo = False
        foundThree = False
        foundFour = False

        for result in results:
            assert len(result) == 4  , 'Expected 4 columns in result (id, name, value, extra_data) but got %d. %s' %(len(result), repr(result))

            (fetchedId, fetchedName, fetchedValue, fetchedExtraData) = result

            if fetchedName == 'one':
                foundOne = True
                assert fetchedValue == 'Hello' , 'Got mixed up data. Expected name="one" to have value="Hello". Row was: ' + repr(result)
                assert fetchedExtraData is None , 'Expected NULL result to have value of None. Got: %s' %(repr(fetchedExtraData), )

                assert fetchedId == self.nameToPk['one'] , 'Expected pk for name="one" to match the one returned by doInsert. Got %s but expected %s' %( repr(fetchedId), repr(self.nameToPk['one']))

            elif fetchedName == 'two':
                foundTwo = True
                assert fetchedValue == 'Goodbye' , 'Got mixed up data. Expected name="two" to have value="Goodbye". Row was: ' + repr(result)
                assert fetchedExtraData is None , 'Expected NULL result to have value of None. Got: %s' %(repr(fetchedExtraData), )

                assert fetchedId == self.nameToPk['two'] , 'Expected pk for name="two" to match the two returned by doInsert. Got %s but expected %s' %( repr(fetchedId), repr(self.nameToPk['two']))

            elif fetchedName == 'three':
                foundThree = True
                assert fetchedValue == 'Goodbye' , 'Got mixed up data. Expected name="three" to have value="Goodbye". Row was: ' + repr(result)
                assert fetchedExtraData == 'Some extra data' , 'Expected extra data on name="three" to be "Some extra data" but got %s' %(repr(fetchedExtraData), )

                assert fetchedId == self.nameToPk['three'] , 'Expected pk for name="three" to match the three returned by doInsert. Got %s but expected %s' %( repr(fetchedId), repr(self.nameToPk['three']))

            elif fetchedName == 'four':
                foundFour = True
                assert fetchedValue == 'Cheese' , 'Got mixed up data. Expected name="four" to have value="Cheese". Row was: ' + repr(result)
                assert fetchedExtraData == 'Yummy yum yum' , 'Expected extra data on name="four" to be "Yummy yum yum" but got %s' %(repr(fetchedExtraData), )

                assert fetchedId == self.nameToPk['four'] , 'Expected pk for name="four" to match the four returned by doInsert. Got %s but expected %s' %( repr(fetchedId), repr(self.nameToPk['four']))

            else:
                raise AssertionError("Got unknown row: %s" %(repr(result), ))

        assert foundOne , 'Did not find name="one"'
        assert foundTwo , 'Did not find name="two"'
        assert foundThree , 'Did not find name="three"'
        assert foundFour , 'Did not find name="four"'


if __name__ == '__main__':
    sys.exit(subprocess.Popen('GoodTests.py -n1 "%s" %s' %(sys.argv[0], ' '.join(['"%s"' %(arg.replace('"', '\\"'), ) for arg in sys.argv[1:]]) ), shell=True).wait())

