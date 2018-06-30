
from ichorORM.model import DatabaseModel

class Meal(DatabaseModel):

    FIELDS = ['id', 'food_group', 'item_name', 'price', 'id_person', 'datasetUid']

    REQUIRED_FIELDS = ['item_name', 'price', 'id_person']

    TABLE_NAME = 'meal'


    # _CREATE_TABLE_SQL - Not a field to ichorORM, but used by the tests to create the tables
    _CREATE_TABLE_SQL = """CREATE TABLE meal(id serial PRIMARY KEY, food_group varchar(128) NULL, item_name varchar(255) NOT NULL, price decimal NOT NULL, id_person integer REFERENCES Person(id) NOT NULL, datasetUid varchar(255) NULL)"""

