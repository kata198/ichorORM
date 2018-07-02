#!/usr/bin/env GoodTests.py
'''
    test_DatabaseModel - Test various methods on a DatabaseModel
'''

import subprocess
import sys

import LocalConfig


import ichorORM

from ichorORM.model import DatabaseModel
from ichorORM.query import SelectQuery


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

        
        dbConn.executeSql("INSERT INTO %s (first_name, last_name, age, birth_day, birth_month) VALUES ('John', 'Smith', 43, 4, 11)" %(MyPersonModel.TABLE_NAME, ))
        dbConn.executeSql("INSERT INTO %s (first_name, last_name, age, birth_day, birth_month) VALUES ('John', 'Doe', 38, 2, 12)" %(MyPersonModel.TABLE_NAME, ))
        dbConn.executeSql("INSERT INTO %s (first_name, last_name, age, birth_day, birth_month) VALUES ('Jane', 'Doe', 25, 8, 5)" %(MyPersonModel.TABLE_NAME, ))
        dbConn.executeSql("INSERT INTO %s (first_name, last_name, age, birth_day, birth_month) VALUES ('Cathy', 'Lawson', 14, 6, 8)" %(MyPersonModel.TABLE_NAME, ))
        dbConn.executeSql("INSERT INTO %s (first_name, last_name, age, birth_day, birth_month) VALUES ('Tom', 'Brown', 65, 2, 9)" %(MyPersonModel.TABLE_NAME, ))

    def teardown_class(self):
        '''
            teardown_class - Destroy any data generated by this test.
                Ran after all tests have completed
        '''
        
        try:
            dbConn = ichorORM.getDatabaseConnection()
            dbConn.executeSql("DELETE FROM %s" %(MyPersonModel.TABLE_NAME, ))
        except Exception as e:
            pass

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

if __name__ == '__main__':
    sys.exit(subprocess.Popen('GoodTests.py -n1 "%s" %s' %(sys.argv[0], ' '.join(['"%s"' %(arg.replace('"', '\\"'), ) for arg in sys.argv[1:]]) ), shell=True).wait())
