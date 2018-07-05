#!/usr/bin/env GoodTests.py
'''
    test_DatabaseModel - Test various methods on a DatabaseModel
'''

import copy
import subprocess
import sys

import LocalConfig


import ichorORM

from ichorORM.model import DatabaseModel
from ichorORM.query import SelectQuery, QueryStr, SQL_NULL


class MyPersonModel(DatabaseModel):
    '''
        MyPersonModel - A model that could represent a person
    '''

    FIELDS = ['id', 'first_name', 'last_name', 'age', 'birth_day', 'birth_month']

    REQUIRED_FIELDS = ['first_name', 'last_name']

    TABLE_NAME = 'ichortest_my_person_model2'

    def getFullName(self):
        return "%s %s" %(self.first_name, self.last_name)


class TestDatabaseModel(object):
    '''
        Test class for DatabaseModel methods
    '''

    def setup_class(self):
        '''
            setup_class - ensure this test is setup.
                Executed prior to any of the tests in this class.
        '''
        LocalConfig.ensureTestSetup()

        dbConn = ichorORM.getDatabaseConnection()
        try:
            dbConn.executeSql("DELETE FROM " + MyPersonModel.TABLE_NAME)
        except:
            dbConn.executeSql("CREATE TABLE %s ( id serial primary key, first_name varchar(255) NOT NULL, last_name varchar(255) NOT NULL, age smallint, birth_day smallint, birth_month smallint )" %(MyPersonModel.TABLE_NAME, ))

        # NOTE: "id" will be set upon insert
        self.dataSet = [
            { "id" : None, "first_name" : 'John', 'last_name'  : 'Smith',  'age' : 43, 'birth_day' : 4, 'birth_month' : 11 },
            { "id" : None, "first_name" : 'John', 'last_name'  : 'Doe',    'age' : 38, 'birth_day' : 2, 'birth_month' : 12 },
            { "id" : None, "first_name" : 'Jane', 'last_name'  : 'Doe',    'age' : 25, 'birth_day' : 8, 'birth_month' : 5 },
            { "id" : None, "first_name" : 'Cathy', 'last_name' : 'Lawson', 'age' : 14, 'birth_day' : 6, 'birth_month' : 8 },
            { "id" : None, "first_name" : 'Tom',  'last_name'  : 'Brown',  'age' : 65, 'birth_day' : 2, 'birth_month' : 9 },
        ]

        self.nullDataSet = [
            { "id" : None, "first_name" : 'Henry', 'last_name'  : 'Thomson',    'age' : None, 'birth_day' : None, 'birth_month' : None },
            { "id" : None, "first_name" : 'Frank', 'last_name' : "L'ray", 'age' : None, 'birth_day' : None, 'birth_month' : None },
            { "id" : None, "first_name" : 'Bob',  'last_name'  : 'Bizzle',  'age' : None, 'birth_day' : None, 'birth_month' : None },
        ]


    def setup_method(self, meth):
        '''
            setup_method - Called before every method.
                
                @param meth <builtins.method> - The test method that is about to be executed
        '''

        self.fullDataSet = []

        dbConn = ichorORM.getDatabaseConnection(isTransactionMode=True)

        pks = dbConn.doInsert("INSERT INTO " + MyPersonModel.TABLE_NAME + " (first_name, last_name, age, birth_day, birth_month) VALUES ( %(first_name)s, %(last_name)s, %(age)s, %(birth_day)s, %(birth_month)s )", valueDicts=self.dataSet, autoCommit=False, returnPk=True)

        dbConn.commit()

        for i in range(len(self.dataSet)):
            self.dataSet[i]['id'] = pks[i]

        self.fullDataSet += self.dataSet

        if meth in (self.test_filterNull, ):
            pks = dbConn.doInsert("INSERT INTO " + MyPersonModel.TABLE_NAME + " (first_name, last_name, age, birth_day, birth_month) VALUES ( %(first_name)s, %(last_name)s, %(age)s, %(birth_day)s, %(birth_month)s )", valueDicts=self.nullDataSet, autoCommit=False, returnPk=True)

            dbConn.commit()

            for i in range(len(self.nullDataSet)):
                self.nullDataSet[i]['id'] = pks[i]
                self.fullDataSet.append( self.nullDataSet[i] )



        #dbConn.executeSql("INSERT INTO %s (first_name, last_name, age, birth_day, birth_month) VALUES ('John', 'Smith', 43, 4, 11)" %(MyPersonModel.TABLE_NAME, ))
        #dbConn.executeSql("INSERT INTO %s (first_name, last_name, age, birth_day, birth_month) VALUES ('John', 'Doe', 38, 2, 12)" %(MyPersonModel.TABLE_NAME, ))
        #dbConn.executeSql("INSERT INTO %s (first_name, last_name, age, birth_day, birth_month) VALUES ('Jane', 'Doe', 25, 8, 5)" %(MyPersonModel.TABLE_NAME, ))
        #dbConn.executeSql("INSERT INTO %s (first_name, last_name, age, birth_day, birth_month) VALUES ('Cathy', 'Lawson', 14, 6, 8)" %(MyPersonModel.TABLE_NAME, ))
        #dbConn.executeSql("INSERT INTO %s (first_name, last_name, age, birth_day, birth_month) VALUES ('Tom', 'Brown', 65, 2, 9)" %(MyPersonModel.TABLE_NAME, ))



    def teardown_method(self, meth):
        '''
            teardown_method - Called after every method.
                
                @param meth <builtins.method> - The test method that completed
        '''
        try:
            dbConn = ichorORM.getDatabaseConnection()
            dbConn.executeSql("DELETE FROM %s" %(MyPersonModel.TABLE_NAME, ))
        except Exception as e:
            pass

    def teardown_class(self):
        '''
            teardown_class - Destroy any data generated by this test.
                Ran after all tests have completed
        '''
        

        try:
            dbConn = ichorORM.getDatabaseConnection()
            dbConn.executeSql("DROP TABLE %s CASCADE" %(MyPersonModel.TABLE_NAME, ))
        except Exception as e:
            pass
            

    @staticmethod
    def _testPerson(personObj, expectedAge, expectedBirthDay, expectedBirthMonth):
        '''
            _testPerson - Test the attributes on a person against expected values
        '''
        fullName = "%s %s" %(personObj.first_name, personObj.last_name)
        assert personObj.age == expectedAge , "Expected %s to have age of %d. Got: %s" %(fullName, expectedAge, repr(personObj.age))
        assert personObj.birth_day == expectedBirthDay , 'Expected %s to have a birth day of %d. Got: %s' %(fullName, expectedBirthDay, repr(personObj.birth_day))
        assert personObj.birth_month == expectedBirthMonth , 'Expected %s to have a birth month of %d. Got: %s' %(fullName, expectedBirthMonth, repr(personObj.birth_month))


    def test_initFields(self):
        '''
            test_initFields - Test that the init method takes and sets fields
        '''

        personObj = MyPersonModel(age=38, first_name='Hello', last_name='World', birth_day=21, birth_month=1)

        assert personObj.first_name == 'Hello' , 'Expected init to set first_name field'
        assert personObj.last_name == 'World', 'Expected init to set last_name field'
        assert personObj.birth_day == 21 , 'Expected init to set birth_day field'
        assert personObj.birth_month == 1 , 'Expected init to set birth_month field'
        assert personObj.age == 38 , 'Expected init to set age field'


        personObj = MyPersonModel(first_name='Hello', last_name='World', birth_day=21)
        assert personObj.first_name == 'Hello' , 'Expected init to set first_name field'
        assert personObj.last_name == 'World', 'Expected init to set last_name field'
        assert personObj.birth_day == 21 , 'Expected init to set birth_day field'
        assert personObj.birth_month is None , 'Expected default field value of None for birth_month'
        assert personObj.age is None , 'Expected default field value of None for age'


    def test_defaultFieldValues(self):
        '''
            test_defaultFieldValues - Test that the default field values are used
        '''
        class Employee(DatabaseModel):
            
            FIELDS = ['name', 'age', 'occupation', 'hour_rate']

            TABLE_NAME = 'employee'

            DEFAULT_FIELD_VALUES = { 'occupation' : 'intern', 'hour_rate' : 8.50 }

        employee = Employee()

        assert employee.name is None , 'Expected default for name to be None'
        assert employee.age is None , 'Expected default for age to be None'
        assert employee.occupation == 'intern' , 'Expected default for occupation to be "intern". Got: ' + repr(employee.occupation)
        assert employee.hour_rate == 8.50 , 'Expected default for hour_rate to be 8.50'

        employee = Employee(name='Bob', occupation='Director')

        assert employee.name == 'Bob', 'Expected name to be set'
        assert employee.occupation == 'Director' , 'Expected provided "Director" to override default value "intern" but got: ' + repr(employee.occupation)
        assert employee.hour_rate == 8.50 , 'Expected default for hour_rate to be 8.50'


    def test_all(self):
        '''
            test_selectAllObjs - Test selecting all objects
        '''


        allPeople = MyPersonModel.all()

        foundJohnSmith = False
        foundJohnDoe = False
        foundJaneDoe = False
        foundCathyLawson = False
        foundTomBrown = False

        _testPerson = self._testPerson

        for person in allPeople:
            if person.first_name == 'John' and person.last_name == 'Smith':
                assert foundJohnSmith is False , 'John Smith in results twice.'
                foundJohnSmith = True
                _testPerson(person, 43, 4, 11)
            elif person.first_name == 'John' and person.last_name == 'Doe':
                assert foundJohnDoe is False, 'John Doe in results twice.'
                foundJohnDoe = True
                _testPerson(person, 38, 2, 12)
            elif person.first_name == 'Jane' and person.last_name == 'Doe':
                assert foundJaneDoe is False, 'Jane Doe in results twice.'
                foundJaneDoe = True
                _testPerson(person, 25, 8, 5)
            elif person.first_name == 'Cathy' and person.last_name == 'Lawson':
                assert foundCathyLawson is False , 'Cathy Lawson in results twice.'
                foundCathyLawson = True
                _testPerson(person, 14, 6, 8)
            elif person.first_name == 'Tom' and person.last_name == 'Brown':
                assert foundTomBrown is False, 'Tom Brown in results twice.'
                foundTomBrown = True
                _testPerson(person, 65, 2, 9)
            else:
                assert False , 'Got unexpected result in data set: ' + repr(person)

        assert foundJohnSmith , 'John Smith not in results.'
        assert foundJohnDoe , 'John Doe not in results.'
        assert foundJaneDoe , 'Jane Doe is not in results.'
        assert foundCathyLawson , 'Cathy Lawson is not in results.'
        assert foundTomBrown , 'Tom Brown is not in results.'


    def test_allOrderBy(self):
        '''
            test_allOrderBy - Test the orderBy arguments to the DatabaseModel.all call
        '''

        dataSetAgeAsc = list(sorted( copy.copy(self.dataSet), key = lambda dataSet : dataSet['age'] ))
        dataSetAgeDesc = list(reversed( sorted( copy.copy(self.dataSet), key = lambda dataSet : dataSet['age'] ) ))

        allAgeAsc = MyPersonModel.all(orderByField='age', orderByDir='ASC')
        assert allAgeAsc , 'Did not get any objects returned.'

        assert len(allAgeAsc) == len(self.dataSet) , 'Expected to get %d objects, but got %d. Objects: %s' %( len(self.dataSet), len(allAgeAsc), repr(allAgeAsc) )

        allAgeAscMaps = [ x.asDict(includePk=True) for x in allAgeAsc ]

        assert allAgeAscMaps == dataSetAgeAsc , 'Objects seem out of order. Expected: %s     Got:  %s' %( repr(dataSetAgeAsc), repr(allAgeAscMaps) )


        allAgeDesc = MyPersonModel.all(orderByField='age', orderByDir='DESC')
        assert allAgeDesc , 'Did not get any objects returned.'

        assert len(allAgeDesc) == len(self.dataSet) , 'Expected to get %d objects, but got %d. Objects: %s' %( len(self.dataSet), len(allAgeDesc), repr(allAgeDesc) )


        allAgeDescMaps = [ x.asDict(includePk=True) for x in allAgeDesc ]
        
        assert allAgeDescMaps == dataSetAgeDesc , 'Objects seem out of order. Expected: %s     Got:  %s' %( repr(dataSetAgeDesc), repr(allAgeDescMaps) )


    def test_filter(self):
        '''
            test_filter - Test the "filter" method
        '''

        
        _testPerson = self._testPerson

        matchedObjs = MyPersonModel.filter(birth_day=2)

        foundJohnDoe = False
        foundTomBrown = False

        assert len(matchedObjs) == 2 , 'Expected to get 2 objects returned. Got: ' + repr(matchedObjs)

        for personObj in matchedObjs:
            if personObj.first_name == 'John' and personObj.last_name == 'Doe':
                assert not foundJohnDoe , 'Found John Doe twice.'
                foundJohnDoe = True
                _testPerson(personObj, 38, 2, 12)
            elif personObj.first_name == 'Tom' and personObj.last_name == 'Brown':
                assert not foundTomBrown , 'Found Tom Brown twice.'
                foundTomBrown = True
                _testPerson(personObj, 65, 2, 9)
            else:
                assert False , 'Got unexpected result in data set: ' + repr(personObj)

        assert foundJohnDoe , 'John Doe was not in results'
        assert foundTomBrown , 'Tom Brown was not in results'


        allResults = MyPersonModel.filter(age__gt=35, birth_day=2)

        foundJohnDoe = False
        foundTomBrown = False

        for person in allResults:
            if person.first_name == 'John' and person.last_name == 'Doe':
                assert foundJohnDoe is False, 'John Doe in results twice.'
                foundJohnDoe = True
                _testPerson(person, 38, 2, 12)
            elif person.first_name == 'Tom' and person.last_name == 'Brown':
                assert foundTomBrown is False, 'Tom Brown in results twice.'
                foundTomBrown = True
                _testPerson(person, 65, 2, 9)
            else:
                assert False , 'Got unexpected result in data set: ' + repr(person)

        assert foundJohnDoe , 'Did not find expected John Doe entry in results.'
        assert foundTomBrown , 'Did not find expected Tom Brown entry in results.'


    def test_filterOrderBy(self):
        '''
            test_filterOrderBy - Test that the order by parameters work with DatabaseModel.filter
        '''

        expectedObjs = [ copy.copy(dataItem) for dataItem in self.dataSet if dataItem['first_name'] in ('John', 'Tom') ]
        expectedObjsAgeAsc = list(sorted( copy.copy(expectedObjs), key = lambda dataSet : dataSet['age'] ))
        expectedObjsAgeDesc = list(reversed( sorted( copy.copy(expectedObjs), key = lambda dataSet : dataSet['age'] ) ))

        # ASCending
        filterObjs = MyPersonModel.filter(first_name__in=('John', 'Tom'), orderByField='age', orderByDir='ASC')

        assert filterObjs , 'Did not get any results from filter.'

        assert len(filterObjs) == len(expectedObjs) , 'Got unexpected number of results. Expected %d but got %d.  Got: %s' %(len(expectedObjs), len(filterObjs), repr(filterObjs) )

        filterResultsDicts = [ obj.asDict(includePk=True) for obj in filterObjs ]

        assert filterResultsDicts == expectedObjsAgeAsc , 'Did not get filtered results in expected order. Expected:  %s  Got:  %s' %( repr(expectedObjsAgeAsc), repr(filterResultsDicts) )

        # DESCending
        filterObjs = MyPersonModel.filter(first_name__in=('John', 'Tom'), orderByField='age', orderByDir='DESC')

        assert filterObjs , 'Did not get any results from filter.'

        assert len(filterObjs) == len(expectedObjs) , 'Got unexpected number of results. Expected %d but got %d.  Got: %s' %(len(expectedObjs), len(filterObjs), repr(filterObjs) )

        filterResultsDicts = [ obj.asDict(includePk=True) for obj in filterObjs ]

        assert filterResultsDicts == expectedObjsAgeDesc , 'Did not get filtered results in expected order. Expected:  %s  Got:  %s' %( repr(expectedObjsAgeDesc), repr(filterResultsDicts) )



    def test_insertObject(self):
        '''
            test_insertObject - Test inserting an object
        '''
        
        newPerson = MyPersonModel(first_name='Timmy', last_name='Tooth', age=12, birth_day=28, birth_month=6)

        gotException = False
        try:
            myObj = newPerson.insertObject()
        except Exception as e:
            gotException = e

        assert gotException is False , 'Expected to not get exception doing insert. Got %s  %s' %(str(type(gotException)), str(gotException))

        assert myObj , 'Expected insertObject to return obj.'

        assert myObj is newPerson , 'Expected returned object to be same id as origin object'

        assert newPerson.id , 'Expected "id" field to be set after inserting.'

        findResults = MyPersonModel.filter(first_name='Timmy', last_name='Tooth')

        assert len(findResults) == 1 , 'Expected to get 1 result for "Timmy Tooth" after insert.'

        foundObj = findResults[0]

        assert newPerson.id == foundObj.id , 'Expected id to match. %s != %s' %(repr(myId), repr(foundObj.id))

        assert foundObj.asDict() == newPerson.asDict() , 'Expected fields to all match on inserted object and selected object'

        objMissingFields = MyPersonModel(first_name='Jim', age=49)

        gotValueError = False
        try:
            objMissingFields.insertObject()
        except ValueError:
            gotValueError = True

        assert gotValueError , 'Expected to get a ValueError missing a REQUIRED_FIELD'

    def test_updateObject(self):
        '''
            test_updateObject - Test updating an object
        '''
        newPerson = MyPersonModel(first_name='Tony', last_name='Tiger', age=33, birth_day=25, birth_month=7)

        gotException = False
        try:
            myObj = newPerson.insertObject()
        except Exception as e:
            gotException = e

        assert gotException is False , 'Expected to not get exception doing insert. Got %s  %s' %(str(type(gotException)), str(gotException))


        newPerson.age = 33
        newPerson.birth_day = 16
        newPerson.birth_month = 9 # Note, we will NOT call update on this field

        gotException = False
        try:
            newPerson.updateObject( ['age', 'birth_day' ] )
        except Exception as e:
            gotException = e

        assert gotException is False , 'Expected to not get exception doing update. Got %s  %s' %(str(type(gotException)), str(gotException))


        fetchedPersons = MyPersonModel.filter(first_name='Tony', last_name='Tiger')
        
        assert len(fetchedPersons) == 1 , 'Expected to only get 1 result for "Tony Tiger". Got: ' + repr(fetchedPersons)

        fetchedPerson = fetchedPersons[0]

        assert fetchedPerson.first_name == 'Tony' and fetchedPerson.last_name == 'Tiger' , 'Expected to get "Tony Tiger" but got ' + repr(fetchedPerson)

        assert fetchedPerson.age == 33 , 'Expected "age" field to get updated to 33, but got: ' + repr(fetchedPerson.age)
        assert fetchedPerson.birth_day == 16 , 'Expected "birth_day" field to get updated to 16, but got: ' + repr(fetchedPerson.birth_day)
        assert fetchedPerson.birth_month == 7 , 'Expected "birth_month" field to NOT get updated to 9 (was not in fields passed to updateObject), but got: ' + repr(fetchedPerson.birth_month)


    def test_get(self):
        '''
            test_get - Test the "get" method
        '''
        
        allObjs = MyPersonModel.all()

        for obj in allObjs:
            
            getObj = MyPersonModel.get( obj.id )

            assert getObj , 'Expected .get ( %d ) to return fetched object, but got: %s' %(obj.id, repr(getObj))
            assert getObj.asDict() == obj.asDict() , 'Expected .get to return idential object.\n%s   !=  %s\n' %( repr(obj), repr(getObj))


    def test_createAndSave(self):
        '''
            test_createAndSave - Test the "create and save" method
        '''

        newObj = MyPersonModel.createAndSave(first_name='Jimmy', last_name='Hoffa', age=82)

        assert newObj , 'Expected createAndSave to return an object'

        assert issubclass(newObj.__class__, MyPersonModel) , 'Expected object returned to be of model type'

        assert newObj.id , 'Expected id field to be set (meaning object was saved)'

        # Fetch the inserted obect
        objFetch = MyPersonModel.get(newObj.id)

        assert objFetch.asDict() == newObj.asDict() , 'Expected fetched object to contain the same field values as inserted object'

    def test_filterNull(self):
        '''
            test_filterNull - Test using NULL in filters
        '''
        nullAgeExpectedMaps = [ copy.copy(x) for x in self.fullDataSet if x['age'] is None ]
        notNullAgeExpectedMaps = [ copy.copy(x) for x in self.fullDataSet if x['age'] is not None ]
        #    { "id" : None, "first_name" : 'Henry', 'last_name'  : 'Thomson',    'age' : None, 'birth_day' : None, 'birth_month' : None },
        #    { "id" : None, "first_name" : 'Frank', 'last_name' : "L'ray", 'age' : None, 'birth_day' : None, 'birth_month' : None },
        #    { "id" : None, "first_name" : 'Bob',  'last_name'  : 'Bizzle',  'age' : None, 'birth_day' : None, 'birth_month' : None },
        #]
        def testResults(results, expectedResults, whichTestStr):
            assert results , 'Got no results for %s check' %(whichTestStr, )

            assert len(results) == len(expectedResults) , 'Expected %d results on "%s" check but got %d.  Results:  %s' %(len(expectedResults), whichTestStr, len(results), repr(results) )

            resultDicts = [result.asDict(includePk=True) for result in results]

            for resultDict in resultDicts:
                foundMatch = False
                for expectedResult in expectedResults:
                    if expectedResult == resultDict:
                        foundMatch = True
                        break

                assert foundMatch , '"%s" check did not match expected data set: ' %( whichTestStr, repr(resultDict), )

        # Test is=None
        nullAgeObjs = MyPersonModel.filter(age__is=None)
        testResults(nullAgeObjs, nullAgeExpectedMaps, 'age__is=None')

        # Test isnot=None
        notNullAgeObjs = MyPersonModel.filter(age__isnot=None)
        testResults(notNullAgeObjs, notNullAgeExpectedMaps, 'age__isnot=None')

        # Test that =None is converted to is None
        nullAgeObjs = MyPersonModel.filter(age=None)
        testResults(nullAgeObjs, nullAgeExpectedMaps, 'age=None')

        # Test that __ne=None is converted to isnot None
        notNullAgeObjs = MyPersonModel.filter(age__ne=None)
        testResults(notNullAgeObjs, notNullAgeExpectedMaps, 'age__ne=None')


        # Test with SQL_NULL
        nullAgeObjs = MyPersonModel.filter(age__is=SQL_NULL)
        testResults(nullAgeObjs, nullAgeExpectedMaps, 'age__is=SQL_NULL')

        # Test with = SQL_NULL (should be converted to is)
        nullAgeObjs = MyPersonModel.filter(age=SQL_NULL)
        testResults(nullAgeObjs, nullAgeExpectedMaps, 'age=SQL_NULL')

        # Test isnot SQL_NULL
        notNullAgeObjs = MyPersonModel.filter(age__isnot=SQL_NULL)
        testResults(notNullAgeObjs, notNullAgeExpectedMaps, 'age__isnot=SQL_NULL')

        # Test __ne= SQL_NULL (should be converted to is not)
        notNullAgeObjs = MyPersonModel.filter(age__ne=SQL_NULL)
        testResults(notNullAgeObjs, notNullAgeExpectedMaps, 'age__ne=SQL_NULL')

        # Test with QueryStr('NULL')
        nullAgeObjs = MyPersonModel.filter(age__is=QueryStr('NULL'))
        testResults(nullAgeObjs, nullAgeExpectedMaps, 'age__is=QueryStr("NULL")')

        # Test isnot SQL_NULL
        notNullAgeObjs = MyPersonModel.filter(age__isnot=QueryStr('NULL'))
        testResults(notNullAgeObjs, notNullAgeExpectedMaps, 'age__isnot=QueryStr("NULL)')

        
        # Test with = QueryStr('NULL') ( should be converted to is )
        nullAgeObjs = MyPersonModel.filter(age=QueryStr('NULL'))
        testResults(nullAgeObjs, nullAgeExpectedMaps, 'age=QueryStr("NULL")')


        # Test with __ne= QueryStr('NULL') ( should be converted to is )
        notNullAgeObjs = MyPersonModel.filter(age__ne=QueryStr('NULL'))
        testResults(notNullAgeObjs, notNullAgeExpectedMaps, 'age__ne==QueryStr("NULL)')


if __name__ == '__main__':
    sys.exit(subprocess.Popen('GoodTests.py -n1 "%s" %s' %(sys.argv[0], ' '.join(['"%s"' %(arg.replace('"', '\\"'), ) for arg in sys.argv[1:]]) ), shell=True).wait())
