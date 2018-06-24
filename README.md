# ichorORM
A python library for postgresql focused on performance and supporting ORM and query-building functionality. It supports transactions and autocommit mode.

ichorORM uses psycopg2 to interact with a configured SQL database and provides an easy and efficient abstraction layer.


Connecting to the Database
--------------------------

ichorORM provides two means of connection to the database.

**Global Connection**

The first is the "global" connection info. This is the default used for everything unless specified otherwise.

Set these fields via the *setGlobalConnectionParams* method

	setGlobalConnectionParams(host='localhost', dbname='my_db', user='Super User', password='cheese')


Fields can be omitted and they will not be sent. For example, if your ident specifies a default user to be your system user, or if you don't have a password for localhost accounts, etc. , then don't set those fields.

**Individual Connections**

While the global provides the defaults to use, you may also create standalone connections (for example, to copy data from one database and then connect and save to another).

You will also need to create and pass along a connection object when doing transactions.


The *getDatabaseConnection* method will return a new connection. If you don't provide any arguments, it will inherit the connection info from the global connection. Any arguments you do provide will override the associated global connection parameter for the returned connection.

	# Get a connection same settings as global connection (for transactions)
	dbConn = getDatabaseConnection() 

	# Get a connection using same settings but connect to a different database:
	dbConnBak = getDatabaseConnection(db_name='bak_my_db')



Models
------

This section will deal with your ORM models and the associated methods. Models are also used with the query builder, which is covered in the "Query Builder" section.


Your SQL tables should be represented by a DatabaseModel object ( ichorORM.DatabaseModel ).

Each table is expected to have a serial sequence primary key (generally called "id") for sanity and performance reasons.

You may find it useful to add relevant methods to this model object.


	from ichorORM import DatabaseModel

	class Person(DatabaseModel):

		# TABLE_NAME - This is the name of the corrosponding table in your database
		TABLE_NAME = 'Person'   

		# FIELDS - A list of all fields on this table (excluding the primary pk, "id" )
		FIELDS = [ 'first_name', 'last_name', 'age', 'gender', 'eye_color', 'ethnicity', 'title' ]

		# REQUIRED_FIELDS - A list of NOT NULL fields which will validate prior to 
		#        insertObject/createAndSave (cannot be None or ValueError raised)
		REQUIRED_FIELDS = [ 'last_name' ]

		# DEFAULT_FIELD_VALUES - A map for client-side defaults.
		#        When a new object is created, the fields are assigned these values
		#         if not explicitly set to something else
		DEFAULT_FIELD_VALUES = { 'title' : 'General Employee' }

		# PRIMARY_KEY - If your primary serial key is not 'id', name it here
		# PRIMARY_KEY = 'serial_num'


**Creating and Saving an entry**

All field names found in the 'FIELDS' array on your model can be set by passing as a kwarg to \_\_init\_\_.

They also become variable members of the object.

Any defaults found in DEFAULT\_FIELD\_VALUES will be applied here if a different value isn't explicitly set.

	personObj = Person(first_name='Tim', age=30, gender='male', eye_color='Hazel')

	print ( "%s is a %d year old %s whose job title is %s." %( personObj.first_name, personObj.age, personObj.gender, personObj.title ) )

The above code will output "Tim is a 30 year old male whose job title is General Employee."


To save this object, we call the *insertObject* method

	personObj.insertObj()

This will perform an INSERT of that person, and it will set the primary key on the personObj.

So if the next serial sequence item was 5, personObj.id would now == 5.

This commits the transaction right away. How to group multiple actions within a single transaction will be covered later.

.

You may also use the static method *createAndSave* to immediately save and return a given object:

	personObj = Person.createAndSave(first_name='Tim', age=30, gender='male', eye_color='Hazel')

This will have all the same field values, including primary key set, as the earlier insert method.

createAndSave also supports transactions which will be covered later.


**Updating an entry**

Any model that is saved can be updated simply by setting the appropriate field values and calling *updateObject* as seen below:

	personObj.last_name = 'Johnson'
	personObj.title = 'General Manager'

	# updateObject method - Provide a list of fields to update
	personObj.updateObject( ['last_name', 'title'] )


This method also supports transactions, with the default being immediate commit.


**Deleting an entry**

An object can be deleted by calling the *delete* method

	oldId = personObj.delete()

This will return the old primary key (in oldId above) and clear the primary key field off "personObj."


**Fetching an entry**

An entry can be fetched via several means. More advanced means are covered in latter sections, this will focus on the methods available through *DatabaseModel*


By primary key, use *get*:

	personObj = Person.get(5) # If primary key is 5


By field values, use *filter*:

	personObj = Person.filter(age__gt=20, gender='male', eye_color__in=['Brown', 'Hazel'])


This will return a list of all Person objects where *age > 20* and *gender = 'male'* and *eye color is one of "Brown" or "Hazel"*.

The standard "filter" rules apply here, double underscore and operation.

	* = or __eq=   - Equals operator

	* __ne= - Not Equal ( <> ) operator

	* __ is or __isnot - IS or IS NOT ( you can use *None* to represent NULL, or the SQL\_NULL constant )

	* __in or __notin - IN or NOT IN a list of values

	* __isnull=True or __isnull=False - Compare IS NULL or IS NOT NULL.

Or to fetch all objects of a given table, use the *all* method

	allPeople = Person.all()


Any objects fetched can be updated just by changing property values and calling *.updateObject*


**Other Methods**

*asDict* - This will return a dict of the field names -> values

	personDict = personObj.asDict()



Query Builder
-------------

Although the ORM and DatabaseModel are very simple and complete, for optimization or complex projects you may prefer to use the query builder.


TODO: Write this section




Transactions
------------

TODO: Write this section
