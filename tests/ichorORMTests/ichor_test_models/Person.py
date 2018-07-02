
from ichorORM.model import DatabaseModel
from ichorORM.relations import OneToManyRelation

class Person(DatabaseModel):

    FIELDS = ['id', 'first_name', 'last_name', 'eye_color', 'age', 'birth_day', 'birth_month', 'datasetuid']

    REQUIRED_FIELDS = ['last_name']

    TABLE_NAME = 'person'


    # _CREATE_TABLE_SQL - Not a field to ichorORM, but used by the tests to create the tables
    _CREATE_TABLE_SQL = """CREATE TABLE person(id serial PRIMARY KEY, first_name varchar(255) NULL, last_name varchar(255) NOT NULL, eye_color varchar(64) NULL, age smallint NULL, birth_day smallint NULL, birth_month smallint NULL, datasetUid varchar(255) NULL)"""

    def getFullName(self):
        '''
            getFullName - Returns the full name (first and last)

              @return <str> - Full name
        '''
        firstName = self.first_name or ''
        lastName = self.last_name or ''

        # If only a first or last name, make sure we have no spaces
        #   otherwise make it "%s %s" %(firstName, lastName)
        ret = ' '.join([x for x in (firstName, lastName) if x])
        return ret

    @classmethod
    def getModelRelations(cls):
        
        from ichor_test_models.Meal import Meal

        mealRelation = OneToManyRelation('id', Meal, 'id_person')


        return {
            'meals' : mealRelation,
             Meal  : mealRelation
        }

