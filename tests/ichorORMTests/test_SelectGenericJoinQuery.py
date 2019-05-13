#!/usr/bin/env GoodTests.py
'''
    test_SelectGenericJoinQuery - Test the SelectGenericJoinQuery
'''

import copy
import subprocess
import sys
import traceback
import uuid

import LocalConfig


import ichorORM

from ichorORM.model import DatabaseModel
from ichorORM.query import InsertQuery, SelectQuery, SelectGenericJoinQuery, QueryStr
from ichorORM.objs import DictObj
from ichorORM.constants import JOIN_INNER

from ichor_test_models.all import Person, Meal

class TestSelectGenericJoinQuery(object):
    '''
        Test class for a SelectGenericJoinQuery
    '''

    def setup_class(self):
        '''
            setup_class - ensure this test is setup.
                Executed prior to any of the tests in this class.
        '''
        LocalConfig.ensureTestSetup()

        self.datasetUid = str(uuid.uuid4())

    def _deleteDataset(self, tableName):
        '''
            _deleteDataset - Delete all records in a given table which have a field, "datasetuid",
                which is generated at the start of this test and is unique to this run-through

                @param tableName <str> - The name of the SQL table
        '''
        try:
            dbConn = ichorORM.getDatabaseConnection()
            dbConn.executeSql("DELETE FROM %s WHERE datasetUid = '%s'" %(tableName, self.datasetUid, ))
        except Exception as e:
            sys.stderr.write('Error deleting all %s objects with dataset uid "%s": %s  %s\n' %
                (tableName, self.datasetUid, str(type(e)), str(e) )
            )

    def _deleteGlobalDatasets(self):
        '''
            _deleteGlobalDatasets - Deletes all the global datasets matching this test's dataset uid

        '''
        # First, delete from Meal which refrences Person
        self._deleteDataset(Meal.TABLE_NAME)

        # Now can delete the Person from this dataset
        self._deleteDataset(Person.TABLE_NAME)


    def teardown_class(self):
        '''
            teardown_class - Destroy any data generated by this test.
                Ran after all tests have completed
        '''
        self._deleteGlobalDatasets()


    def setup_method(self, meth):
        '''
            setup_method - Called prior to each method to perform setup specific to it.

                @param meth <built-in method> - The method being tested (compare meth == self.someMethod)
        '''

        if meth in ( self.test_generalGetMapping, self.test_generalGetDictObjs, self.test_tableStarSelectFields ):

            # self.DEFAULT_PERSON_DATASET - A sample dataset of field -> value for Person model
            self.DEFAULT_PERSON_DATASET = [
                { "id" : None, "first_name" : "John", "last_name" : "Smith", "eye_color" : "blue",
                    'age' : 35, "birth_month" : 4, "birth_day" : 16 },
                { "id" : None, "first_name" : "John", "last_name" : "Doe", "eye_color" : "blue",
                    'age' : 22, "birth_month" : 4, "birth_day" : 26 },
                { "id" : None, "first_name" : "Jane", "last_name" : "Doe", "eye_color" : "green",
                    'age' : 19, "birth_month" : 6, "birth_day" : 24 },
                { "id" : None, "first_name" : "Bill", "last_name" : "Johnson", "eye_color" : "brown",
                    'age' : 19, "birth_month" : 1, "birth_day" : 30 },
                { "id" : None, "first_name" : "Ted", "last_name" : "Karma", "eye_color" : "green",
                    'age' : 29, "birth_month" : 4, "birth_day" : 16 },
            ]
            # Mark the dataset id
            for i in range(len(self.DEFAULT_PERSON_DATASET)):
                self.DEFAULT_PERSON_DATASET[i]['datasetuid'] = self.datasetUid

            dbConn = ichorORM.getDatabaseConnection(isTransactionMode=True)

            pks = dbConn.doInsert(query="INSERT INTO " + Person.TABLE_NAME + " (first_name, last_name, eye_color, age, birth_month, birth_day, datasetuid) VALUES ( %(first_name)s, %(last_name)s, %(eye_color)s, %(age)s, %(birth_month)s, %(birth_day)s, %(datasetuid)s )",
                    valueDicts=self.DEFAULT_PERSON_DATASET, doCommit=False)

            dbConn.commit()

            self.personIdToData = {}

            for i in range(len(pks)):
                self.DEFAULT_PERSON_DATASET[i]['id'] = pks[i]
                self.personIdToData[ pks[i] ] = self.DEFAULT_PERSON_DATASET[i]

    #FIELDS = ['id', 'food_group', 'item_name', 'price', 'id_person', 'datasetuid']

            def getMealForPerson(mealDict, personIdx):
                mealDict = copy.deepcopy(mealDict)
                mealDict['id_person'] = self.DEFAULT_PERSON_DATASET[personIdx]['id']

                return mealDict

            MEAL_ICE_CREAM = { \
                    "id" : None, "food_group" : "desert", "item_name" : "ice cream",
                    "price" : 3.99, "id_person" : None, "datasetuid" : None,
            }

            MEAL_PIZZA = { \
                    "id" : None, "food_group" : "junk", "item_name" : "pizza",
                    "price" : 9.99, "id_person" : None, "datasetuid" : None,
            }

            MEAL_MILK = { \
                    "id" : None, "food_group" : "dairy", "item_name" : "milk",
                    "price" : 4.20, "id_person" : None, "datasetuid" : None,
            }

            self.DEFAULT_MEAL_DATASET = [
                getMealForPerson(MEAL_ICE_CREAM, 0),
                getMealForPerson(MEAL_ICE_CREAM, 1),
                getMealForPerson(MEAL_ICE_CREAM, 2),
                getMealForPerson(MEAL_ICE_CREAM, 3),
                getMealForPerson(MEAL_ICE_CREAM, 4),
                getMealForPerson(MEAL_PIZZA, 2),
                getMealForPerson(MEAL_PIZZA, 4),
                getMealForPerson(MEAL_MILK, 0),
                getMealForPerson(MEAL_MILK, 1),
                getMealForPerson(MEAL_MILK, 2),
            ]
            for i in range(len(self.DEFAULT_MEAL_DATASET)):
                self.DEFAULT_MEAL_DATASET[i]['datasetuid'] = self.datasetUid

            dbConn = ichorORM.getDatabaseConnection(isTransactionMode=True)

            pks = dbConn.doInsert(query="INSERT INTO " + Meal.TABLE_NAME + " (food_group, item_name, price, id_person, datasetuid) VALUES ( %(food_group)s, %(item_name)s, %(price)s, %(id_person)s, %(datasetuid)s )",
                    valueDicts=self.DEFAULT_MEAL_DATASET, doCommit=False)

            dbConn.commit()

            self.mealIdToData = {}

            for i in range(len(pks)):
                self.DEFAULT_MEAL_DATASET[i]['id'] = pks[i]
                self.mealIdToData[ pks[i] ] = self.DEFAULT_MEAL_DATASET[i]



    def teardown_method(self, meth):
        '''
            teardown_method - Called after execution of each method to clean up

                @param meth <built-in method> - The method being tested (compare meth == self.someMethod)
        '''
        if meth in ( self.test_generalGetMapping, self.test_generalGetDictObjs, self.test_tableStarSelectFields):
            self._deleteGlobalDatasets()


    def test_generalGetMapping(self):
        '''
            test_generalGetMapping - Test general querying using SelectGenericJoinQuery
        '''

        selQ = SelectGenericJoinQuery( Person,  )

        selQWhere = selQ.addStage()
        selQWhere.addCondition(Person.TABLE_NAME + '.datasetuid', '=', self.datasetUid)

        joinWhere = selQ.joinModel( Meal, JOIN_INNER )

        joinWhere.addJoin(Meal.TABLE_NAME + '.id_person', '=', Person.TABLE_NAME + '.id' )

        resultMappings = selQ.executeGetMapping()

        assert resultMappings , 'Did not get any results from query.'

        # Should have 1 row per Meal, with Person fields duplicated therein
        assert len(resultMappings) == len( self.DEFAULT_MEAL_DATASET ) , 'Expected %d rows but got %d back. Got: %s' %( len(self.DEFAULT_MEAL_DATASET), len(resultMappings), repr(resultMappings) )


        # Check that all fields are correct
        for resultMapping in resultMappings:

            assert 'person.id' in resultMapping , 'Expected person.id to be a mapping in results. Keys are: %s' %( repr(list(resultMapping.keys())), )

            assert 'meal.id' in resultMapping, 'Expected meal.id to be a mapping in results.  Keys are: %s' %( repr(list(resultMapping.keys())), )

            assert resultMapping['meal.id_person'] == resultMapping['person.id'] , 'Expected meal.id_person [ %s ] to equal person.id [ %s ].' %( repr(resultMapping['meal.id_person']), repr(resultMapping['person.id']) )

            # Ok, general sanity check seems okay. So let's verify that every field is present and accounted for

            personId = resultMapping['person.id']
            mealId = resultMapping['meal.id']

            expectedPersonData = self.personIdToData[personId]
            expectedMealData = self.mealIdToData[mealId]

            # Check all person fields
            for personFieldName in Person.FIELDS:

                mapKey = Person.TABLE_NAME + '.' + personFieldName

                assert mapKey in resultMapping , 'Expected %s to be in mapping results, but it was not. Keys are: %s' %( mapKey, repr(list(resultMapping.keys())) )

                assert str(resultMapping[mapKey]) == str(expectedPersonData[personFieldName]) , 'Unexpected value on mapping %s. Got %s but expected %s' %( mapKey, repr(resultMapping[mapKey]), repr(expectedPersonData[personFieldName]) )

            # Check all meal fields
            for mealFieldName in Meal.FIELDS:

                mapKey = Meal.TABLE_NAME + '.' + mealFieldName

                assert mapKey in resultMapping , 'Expected %s to be in mapping results, but it was not. Keys are: %s' %( mapKey, repr(list(resultMapping.keys())) )

                assert str(resultMapping[mapKey]) == str(expectedMealData[mealFieldName]) , 'Unexpected value on mapping %s. Got %s but expected %s' %( mapKey, repr(resultMapping[mapKey]), repr(expectedMealData[mealFieldName]) )


    def test_generalGetDictObjs(self):
        '''
            test_generalGetDictObjs - Test general querying using SelectGenericJoinQuery
        '''
        selQ = SelectGenericJoinQuery( Person,  )

        selQWhere = selQ.addStage()
        selQWhere.addCondition(Person.TABLE_NAME + '.datasetuid', '=', self.datasetUid)

        joinWhere = selQ.joinModel( Meal, JOIN_INNER )

        joinWhere.addJoin(Meal.TABLE_NAME + '.id_person', '=', Person.TABLE_NAME + '.id' )

        resultDictObjs = selQ.executeGetDictObjs()

        assert resultDictObjs , 'Did not get any results from query.'

        # Should have 1 row per Meal, with Person fields duplicated therein
        assert len(resultDictObjs) == len( self.DEFAULT_MEAL_DATASET ) , 'Expected %d rows but got %d back. Got: %s' %( len(self.DEFAULT_MEAL_DATASET), len(resultDictObjs), repr(resultDictObjs) )


        # TODO: Add a test for DictObjs type

        # Check that all fields are correct
        for resultDictObj in resultDictObjs:

            assert issubclass(resultDictObj.__class__, DictObj) , 'Expected result to be of type DictObj. Got: ' + str(resultDictObj.__class__.__name__)

            assert hasattr(resultDictObj, Person.TABLE_NAME) , 'Expected a Person.TABLE_NAME attribute, but there is none.'
            assert Person.TABLE_NAME in resultDictObj , 'Expected a key of Person.TABLE_NAME, but it is not present.'

            assert hasattr(resultDictObj, Meal.TABLE_NAME) , 'Expected a Meal.TABLE_NAME attribute, but there is none.'
            assert Meal.TABLE_NAME in resultDictObj , 'Expected a key of Meal.TABLE_NAME, but it is not present.'

            personObj = getattr(resultDictObj, Person.TABLE_NAME) # Aka resultDictObj.person
            mealObj = getattr(resultDictObj, Meal.TABLE_NAME)     # Aka resultDictObj.meal

            assert 'id' in personObj , 'Expected person.id to be in results. Keys are: %s' %( repr(list(personObj.keys())), )
            assert 'id' in mealObj , 'Expected meal.id to be in results.  Keys are: %s' %( repr(list(mealObj.keys())), )

            assert mealObj.id_person == personObj.id , 'Expected meal.id_person [ %s ] to equal person.id [ %s ].' %( repr(mealObj.id_person), repr(personObj.id) )

            # Ok, general sanity check seems okay. So let's verify that every field is present and accounted for

            personId = personObj.id
            mealId = mealObj.id

            expectedPersonData = self.personIdToData[personId]
            expectedMealData = self.mealIdToData[mealId]

            # Check all person fields
            for personFieldName in Person.FIELDS:

                assert personFieldName in personObj , 'Expected .person to contain field %s but it did not. Fields are: %s' %( personFieldName, repr(list(personObj.keys())) )

                assert str(personObj[personFieldName]) == str(expectedPersonData[personFieldName]) , 'Unexpected value on person field %s. Got %s but expected %s' %( personFieldName, repr(personObj[personFieldName]), repr(expectedPersonData[personFieldName]) )


            # Check all meal fields
            for mealFieldName in Meal.FIELDS:

                assert mealFieldName in mealObj , 'Expected .meal to contain field %s but it did not. Fields are: %s' %( mealFieldName, repr(list(mealObj.keys())) )

                assert str(mealObj[mealFieldName]) == str(expectedMealData[mealFieldName]) , 'Unexpected value on meal field %s. Got %s but expected %s' %( mealFieldName, repr(mealObj[mealFieldName]), repr(expectedMealData[mealFieldName]) )


    def test_tableStarSelectFields(self):
        '''
            test_tableStarSelectFields - Test that TABLE_NAME + '.*' selects all fields on given table
        '''

        mealStarQ = SelectGenericJoinQuery( Person, selectFields=[ Person.TABLE_NAME + '.age', Meal.TABLE_NAME + '.*' ] )

        mealStarJoin = mealStarQ.joinModel( Meal, JOIN_INNER )

        selectFields = mealStarQ.getFields()

        assert len(selectFields) == len ( Meal.FIELDS ) + 1 , 'Expected [Person.age, Meal.*] to runroll into Person.age and all of Meal fields. Got %d fields but expected %d.  getFields returned: %s' %( len(selectFields), len(Meal.FIELDS) + 1, repr(selectFields))

        for mealField in Meal.FIELDS:

            combinedFieldName = Meal.TABLE_NAME + '.' + mealField

            assert combinedFieldName in selectFields , 'Expected Meal.* to include field %s but it did not. Fields are: %s' %( repr(mealField), repr(selectFields))

        personAgeField = Person.TABLE_NAME + '.age'

        assert personAgeField in selectFields , 'Missing person.age field'

        selQ = SelectGenericJoinQuery( Person, selectFields=[ Person.TABLE_NAME + '.*', Meal.TABLE_NAME + '.*'] )

        selQWhere = selQ.addStage()
        selQWhere.addCondition(Person.TABLE_NAME + '.datasetuid', '=', self.datasetUid)

        joinWhere = selQ.joinModel( Meal, JOIN_INNER )

        joinWhere.addJoin(Meal.TABLE_NAME + '.id_person', '=', Person.TABLE_NAME + '.id' )

        resultMappings = selQ.executeGetMapping()

        assert resultMappings , 'Did not get any results from query.'

        # Should have 1 row per Meal, with Person fields duplicated therein
        assert len(resultMappings) == len( self.DEFAULT_MEAL_DATASET ) , 'Expected %d rows but got %d back. Got: %s' %( len(self.DEFAULT_MEAL_DATASET), len(resultMappings), repr(resultMappings) )


        # Check that all fields are correct
        for resultMapping in resultMappings:

            assert 'person.id' in resultMapping , 'Expected person.id to be a mapping in results. Keys are: %s' %( repr(list(resultMapping.keys())), )

            assert 'meal.id' in resultMapping, 'Expected meal.id to be a mapping in results.  Keys are: %s' %( repr(list(resultMapping.keys())), )

            assert resultMapping['meal.id_person'] == resultMapping['person.id'] , 'Expected meal.id_person [ %s ] to equal person.id [ %s ].' %( repr(resultMapping['meal.id_person']), repr(resultMapping['person.id']) )

            # Ok, general sanity check seems okay. So let's verify that every field is present and accounted for

            personId = resultMapping['person.id']
            mealId = resultMapping['meal.id']

            expectedPersonData = self.personIdToData[personId]
            expectedMealData = self.mealIdToData[mealId]

            # Check all person fields
            for personFieldName in Person.FIELDS:

                mapKey = Person.TABLE_NAME + '.' + personFieldName

                assert mapKey in resultMapping , 'Expected %s to be in mapping results, but it was not. Keys are: %s' %( mapKey, repr(list(resultMapping.keys())) )

                assert str(resultMapping[mapKey]) == str(expectedPersonData[personFieldName]) , 'Unexpected value on mapping %s. Got %s but expected %s' %( mapKey, repr(resultMapping[mapKey]), repr(expectedPersonData[personFieldName]) )

            # Check all meal fields
            for mealFieldName in Meal.FIELDS:

                mapKey = Meal.TABLE_NAME + '.' + mealFieldName

                assert mapKey in resultMapping , 'Expected %s to be in mapping results, but it was not. Keys are: %s' %( mapKey, repr(list(resultMapping.keys())) )

                assert str(resultMapping[mapKey]) == str(expectedMealData[mealFieldName]) , 'Unexpected value on mapping %s. Got %s but expected %s' %( mapKey, repr(resultMapping[mapKey]), repr(expectedMealData[mealFieldName]) )




if __name__ == '__main__':
    sys.exit(subprocess.Popen('GoodTests.py -n1 "%s" %s' %(sys.argv[0], ' '.join(['"%s"' %(arg.replace('"', '\\"'), ) for arg in sys.argv[1:]]) ), shell=True).wait())
