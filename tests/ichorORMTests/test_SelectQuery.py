#!/usr/bin/env GoodTests.py
'''
    test_Select - General "Select" test
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

    TABLE_NAME = 'ichortest_my_person_model1'

    def getFullName(self):
        return "%s %s" %(self.first_name, self.last_name)


class TestSelectQuery(object):
    '''
        Test class for a SelectQuery
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


    def setup_method(self, meth):
        '''
            setup_method - Called before every method call
        '''

        # TODO: These tests were written before this pattern of test data was being used.
        #   Refactor the tests to replace the "magic numbers" to references to this test data
        if meth in (self.test_whereOr, self.test_whereAnd, self.test_selectAllObjs, self.test_SelectWithWhere, self.test_SelectSpecificFields, self.test_selectOrderBy, self.test_limitNum):

            self.dataSet = [
                { "id" : None, "first_name" : 'John', 'last_name'  : 'Smith',  'age' : 43, 'birth_day' : 4, 'birth_month' : 11 },
                { "id" : None, "first_name" : 'John', 'last_name'  : 'Doe',    'age' : 38, 'birth_day' : 2, 'birth_month' : 12 },
                { "id" : None, "first_name" : 'Jane', 'last_name'  : 'Doe',    'age' : 25, 'birth_day' : 8, 'birth_month' : 5 },
                { "id" : None, "first_name" : 'Cathy', 'last_name' : 'Lawson', 'age' : 14, 'birth_day' : 6, 'birth_month' : 8 },
                { "id" : None, "first_name" : 'Tom',  'last_name'  : 'Brown',  'age' : 65, 'birth_day' : 2, 'birth_month' : 9 },
            ]


            dbConn = ichorORM.getDatabaseConnection(isTransactionMode=True)
            pks = dbConn.doInsert("INSERT INTO " + MyPersonModel.TABLE_NAME + " (first_name, last_name, age, birth_day, birth_month) VALUES ( %(first_name)s, %(last_name)s, %(age)s, %(birth_day)s, %(birth_month)s )", valueDicts=self.dataSet, autoCommit=False, returnPk=True)

            dbConn.commit()

            for i in range(len(self.dataSet)):
                self.dataSet[i]['id'] = pks[i]


    def teardown_method(self, meth):
        '''
            teardown_method - Called after each method
        '''
        if meth in (self.test_whereOr, self.test_whereAnd, self.test_selectAllObjs, self.test_SelectWithWhere, self.test_SelectSpecificFields, self.test_selectOrderBy, self.test_limitNum):
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


    def test_selectAllObjs(self):
        '''
            test_selectAllObjs - Test selecting all fields and all objects of a model
        '''

        selQ = SelectQuery(MyPersonModel)

        allPeople = selQ.executeGetObjs()

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


    def test_SelectWithWhere(self):
        '''
            test_SelectWithWhere Test selecting all fields with a single WHERE conditional
        '''
        
        _testPerson = self._testPerson

        selQ = SelectQuery(MyPersonModel)

        selQWhere = selQ.addStage()
        selQWhere.addCondition('birth_day', '=', '2')

        matchedObjs = selQ.executeGetObjs()
         

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


    def test_SelectSpecificFields(self):
        '''
            test_SelectSpecificFields - Test selecting specific fields from our model
        '''
        _testPerson = self._testPerson

        selQ = SelectQuery(MyPersonModel, selectFields=['first_name', 'last_name', 'age'])

        allPeople = selQ.executeGetObjs()

        foundJohnSmith = False
        foundJohnDoe = False
        foundJaneDoe = False
        foundCathyLawson = False
        foundTomBrown = False

        # Check using "None" here as these fields should not have been fetched.
        for person in allPeople:
            if person.first_name == 'John' and person.last_name == 'Smith':
                assert foundJohnSmith is False , 'John Smith in results twice.'
                foundJohnSmith = True
                _testPerson(person, 43, None, None) 
            elif person.first_name == 'John' and person.last_name == 'Doe':
                assert foundJohnDoe is False, 'John Doe in results twice.'
                foundJohnDoe = True
                _testPerson(person, 38, None, None) 
            elif person.first_name == 'Jane' and person.last_name == 'Doe':
                assert foundJaneDoe is False, 'Jane Doe in results twice.'
                foundJaneDoe = True
                _testPerson(person, 25, None, None) 
            elif person.first_name == 'Cathy' and person.last_name == 'Lawson':
                assert foundCathyLawson is False , 'Cathy Lawson in results twice.'
                foundCathyLawson = True
                _testPerson(person, 14, None, None) 
            elif person.first_name == 'Tom' and person.last_name == 'Brown':
                assert foundTomBrown is False, 'Tom Brown in results twice.'
                foundTomBrown = True
                _testPerson(person, 65, None, None) 
            else:
                assert False , 'Got unexpected result in data set: ' + repr(person)

        assert foundJohnSmith , 'John Smith not in results.'
        assert foundJohnDoe , 'John Doe not in results.'
        assert foundJaneDoe , 'Jane Doe is not in results.'
        assert foundCathyLawson , 'Cathy Lawson is not in results.'
        assert foundTomBrown , 'Tom Brown is not in results.'


    def test_selectOrderBy(self):
        '''
            test_selectOrderBy - Test ordering data
        '''
        _testPerson = self._testPerson
        
        selQ = SelectQuery(MyPersonModel, orderByField='age', orderByDir='DESC')
        allPeople = selQ.executeGetObjs()

        assert len(allPeople) == 5 , 'Expected 5 results. Got %d: %s' %(len(allPeople), repr(allPeople))

        assert allPeople[0].getFullName() == 'Tom Brown', 'Expected Tom Brown to be 1st result ordered by age descending'
        _testPerson(allPeople[0], 65, 2, 9)

        assert allPeople[1].getFullName() == 'John Smith', 'Expected John Smith to be 2nd result ordered by age descending'
        _testPerson(allPeople[1], 43, 4, 11)

        assert allPeople[2].getFullName() == 'John Doe' , 'Expected John Doe to be 3rd result ordered by age descending'
        _testPerson(allPeople[2], 38, 2, 12)

        assert allPeople[3].getFullName() == 'Jane Doe', 'Expected Jane Doe to be 4th result ordered by age descending'
        _testPerson(allPeople[3], 25, 8, 5)

        assert allPeople[4].getFullName() == 'Cathy Lawson', 'Expected Cathy Lawson to be the 5th result ordered by age descending'
        _testPerson(allPeople[4], 14, 6, 8)


        selQ = SelectQuery(MyPersonModel, orderByField='age', orderByDir='ASC')
        allPeople = selQ.executeGetObjs()

        assert len(allPeople) == 5 , 'Expected 5 results. Got %d: %s' %(len(allPeople), repr(allPeople))

        assert allPeople[0].getFullName() == 'Cathy Lawson', 'Expected Cathy Lawson to be the 1st result ordered by age descending'
        _testPerson(allPeople[0], 14, 6, 8)

        assert allPeople[1].getFullName() == 'Jane Doe', 'Expected Jane Doe to be 2nd result ordered by age descending'
        _testPerson(allPeople[1], 25, 8, 5)

        assert allPeople[2].getFullName() == 'John Doe' , 'Expected John Doe to be 3rd result ordered by age descending'
        _testPerson(allPeople[2], 38, 2, 12)

        assert allPeople[3].getFullName() == 'John Smith', 'Expected John Smith to be 4th result ordered by age descending'
        _testPerson(allPeople[3], 43, 4, 11)

        assert allPeople[4].getFullName() == 'Tom Brown', 'Expected Tom Brown to be 5th result ordered by age descending'
        _testPerson(allPeople[4], 65, 2, 9)

    
    def test_limitNum(self):
        '''
            test_limitNum - Test a select with a LIMIT set
        '''
        _testPerson = self._testPerson

        selQ = SelectQuery(MyPersonModel, orderByField='age', orderByDir='DESC', limitNum=2)
        allResults = selQ.executeGetObjs()

        assert len(allResults) == 2 , 'Expected 2 results. Got %d: %s' %(len(allResults), repr(allResults))

        assert allResults[0].getFullName() == 'Tom Brown', 'Expected Tom Brown to be 1st result ordered by age descending'
        _testPerson(allResults[0], 65, 2, 9)

        assert allResults[1].getFullName() == 'John Smith', 'Expected John Smith to be 2nd result ordered by age descending'
        _testPerson(allResults[1], 43, 4, 11)


    def test_whereAnd(self):
        '''
            test_whereAnd - Test a query using two conditionals AND'd together
        '''
        _testPerson = self._testPerson

        selQ = SelectQuery(MyPersonModel)

        selQWhere = selQ.addStage('AND')
        selQWhere.addCondition('age', '>', 35)
        selQWhere.addCondition('birth_day', '=', 2)

        allResults = selQ.executeGetObjs()

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


    def test_whereOr(self):
        '''
            test_whereOr - Test a query with two conditionals OR'd together
        '''
        _testPerson = self._testPerson

        selQ = SelectQuery(MyPersonModel)

        selQWhere = selQ.addStage('OR')
        selQWhere.addCondition('birth_month', '=', 11)
        selQWhere.addCondition('birth_day', '=', 2)

        allResults = selQ.executeGetObjs()

        foundJohnSmith = False
        foundJohnDoe = False
        foundTomBrown = False

        for person in allResults:
            if person.first_name == 'John' and person.last_name == 'Smith':
                assert foundJohnSmith is False , 'John Smith in results twice.'
                foundJohnSmith = True
                _testPerson(person, 43, 4, 11)
            elif person.first_name == 'John' and person.last_name == 'Doe':
                assert foundJohnDoe is False, 'John Doe in results twice.'
                foundJohnDoe = True
                _testPerson(person, 38, 2, 12)
            elif person.first_name == 'Tom' and person.last_name == 'Brown':
                assert foundTomBrown is False, 'Tom Brown in results twice.'
                foundTomBrown = True
                _testPerson(person, 65, 2, 9)
            else:
                assert False , 'Got unexpected result in data set: ' + repr(person)

        assert foundJohnSmith , 'Did not find expected John Smith entry in results.'
        assert foundJohnDoe , 'Did not find expected John Doe entry in results.'
        assert foundTomBrown , 'Did not find expected Tom Brown entry in results.'


if __name__ == '__main__':
    sys.exit(subprocess.Popen('GoodTests.py -n1 "%s" %s' %(sys.argv[0], ' '.join(['"%s"' %(arg.replace('"', '\\"'), ) for arg in sys.argv[1:]]) ), shell=True).wait())
