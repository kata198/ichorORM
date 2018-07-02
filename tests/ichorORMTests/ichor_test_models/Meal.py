
from ichorORM.model import DatabaseModel
from ichorORM.relations import OneToOneRelation

class Meal(DatabaseModel):

    FIELDS = ['id', 'food_group', 'item_name', 'price', 'id_person', 'datasetuid']

    REQUIRED_FIELDS = ['item_name', 'price', 'id_person']

    TABLE_NAME = 'meal'


    # _CREATE_TABLE_SQL - Not a field to ichorORM, but used by the tests to create the tables
    _CREATE_TABLE_SQL = """CREATE TABLE meal(id serial PRIMARY KEY, food_group varchar(128) NULL, item_name varchar(255) NOT NULL, price decimal NOT NULL, id_person integer REFERENCES Person(id) NOT NULL, datasetUid varchar(255) NULL)"""

    @classmethod
    def getModelRelations(cls):
        
        from ichor_test_models.Person import Person

        personRelation = OneToOneRelation('id_person', Person, 'id')

        return {
            'person' : personRelation,
            Person : personRelation
        }


#    def getMeals(self):
#        '''
#            getMeals - get a list of all the Meal objects
#                associated with this person.
#
#                @return list<ichor_test_models.Meal> - A list of related Meal objects
#        '''
#        if not self.id:
#            # Not yet saved, no relations
#            return []
#
#        from ichor_test_models.Meal import Meal
#
#        return Meal.filter(id_person=self.id)
