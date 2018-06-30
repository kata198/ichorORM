
from ichorORM.model import DatabaseModel

class Person(DatabaseModel):

    FIELDS = ['id', 'first_name', 'last_name', 'eye_color', 'age', 'birth_day', 'birth_month', 'datasetUid']

    REQUIRED_FIELDS = ['last_name']

    TABLE_NAME = 'person'


    # _CREATE_TABLE_SQL - Not a field to ichorORM, but used by the tests to create the tables
    _CREATE_TABLE_SQL = """CREATE TABLE person(id serial PRIMARY KEY, first_name varchar(255) NULL, last_name varchar(255) NOT NULL, eye_color varchar(64) NULL, age smallint NULL, birth_day smallint NULL, birth_month smallint NULL, datasetUid varchar(255) NULL)"""

