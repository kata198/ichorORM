
from ichorORM.model import DatabaseModel

class Meal(DatabaseModel):

    FIELDS = ['id', 'food_group', 'item_name', 'price', 'id_person', 'datasetUid']

    REQUIRED_FIELDS = ['item_name', 'price', 'id_person']

    TABLE_NAME = 'meal'


    # _CREATE_TABLE_SQL - Not a field to ichorORM, but used by the tests to create the tables
    _CREATE_TABLE_SQL = """CREATE TABLE meal(id serial PRIMARY KEY, food_group varchar(128) NULL, item_name varchar(255) NOT NULL, price decimal NOT NULL, id_person integer REFERENCES Person(id) NOT NULL, datasetUid varchar(255) NULL)"""

    def getPerson(self):
        '''
            getPerson - get the Person object associated with this meal.

                @return <ichor_test_models.Person/None> - 
                    The Person object related to this meal, or None
                     if none associated
        '''

        if not self.id_person:
            return None

        from ichor_test_models.Person import Person

        return Person.get(self.id_person)

    def getMeals(self):
        '''
            getMeals - get a list of all the Meal objects
                associated with this person.

                @return list<ichor_test_models.Meal> - A list of related Meal objects
        '''
        if not self.id:
            # Not yet saved, no relations
            return []

        from ichor_test_models.Meal import Meal

        return Meal.filter(id_person=self.id)
