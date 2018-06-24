# ichorORM
A python library for postgresql focused on performance and supporting ORM and query-building functionality. It supports transactions and autocommit mode.

ichorORM uses psycopg2 to interact with a configured SQL database and provides an easy and efficient abstraction layer.


Connecting to the Database
==========================

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

	# Get a connection same settings as global connection
	dbConn = getDatabaseConnection() 

	# Get a connection same settings as global connection (for transactions)
	dbConn = getDatabaseConnection(isTransactionMode=True)

	# Get a connection using same settings but connect to a different database:
	dbConnBak = getDatabaseConnection(db_name='bak_my_db')

	

Models
======

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
		REQUIRED_FIELDS = [ 'first_name' ]

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





Transactions
============

ichorORM supports transactions easily to ensure atomic operations which affect several tables, and for bulk-actions.


**Each transaction needs a connection**

Start by obtaining a new connection you will use for this transaction. Make sure to set *isTransactionMode* to True to enable read-commit instead of auto-commit mode

	dbConn = getDatabaseConnection(isTransactionMode=True)


**For use on model methods**

This section covers how to do transactions within the DatabaseModel methods. Doing a transaction with the query builder will be discussed later.


Each of the "save action" methods ( *insertObject*, *updateObject*, *createAndSave* ) take two parameters you will set to performa transaction.

These are "dbConn" in which you will pass the transaction connection you opened in step 1, and "doCommit" which you will set to False. When you are done, you can call *commit* on the 

	dbConn = getDatabaseConnection(isTransactionMode=True)

	person1 = Person(first_name='John', last_name='Smith', age=34)
	person2 = Person(first_name='Jane', last_smith='Doe', age=29)
	person3 = Person(first_name='Bob', last_name='Doll', age=69)

	person1.insertObject(dbConn=dbConn, doCommit=False)
	person2.insertObject(dbConn=dbConn, doCommit=False)
	person3.insertObject(dbConn=dbConn, doCommit=False)

	dbConn.commit()

or
	
	dbConn = getDatabaseConnection(isTransactionMode=True)

	person1 = Person.createAndSave(first_name='John', last_name='Smith', age=34, dbConn=dbConn, doCommit=False)
	person2 = Person.createAndSave(first_name='Jane', last_smith='Doe', age=29, dbConn=dbConn, doCommit=False)
	person3 = Person.createAndSave(first_name='Bob', last_name='Doll', age=69, dbConn=dbConn, doCommit=False)

	dbConn.commit()


For updates:

	dbConn = getDatabaseConnection(isTransactionMode=True)

	peopleChangingTitle = People.filter(title='Customer Service Rep')

	for person in peopleChangingTitle:

		person.title = 'Customer Care Officer'
		person.updateObject( ['title'], dbConn=dbConn, doCommit=False)
	
	dbConn.commit()

This will rename all people with the title "Customer Service Rep" to the new title "Customer Care Officer" in one atomic transaction.


**Rollback**

You can trigger a rollback by calling "rollback" on the connection method during a transaction

	dbConn.rollback()


**For use in query builders**


Each of the execute\* methods ( *execute*, *executeGetRows*, *executeGetObjs*, *executeInsert*, *executeUpdate*, *executeDelete*, etc. ) has a "dbConn" parameter. Any non-read action also has a "doCommit." These have the same meaning as before, so pass the connection to the operations and call "commit" when ready to execute.

The transaction mode is READ\_COMMITTED when isTransactionMode=True, so any of the queries will return right away and any writes (update/insert) will execute when "commit" is called on the connection object.


Query Builder
=============

Although the ORM and DatabaseModel are very simple and complete, for optimization or complex projects you may prefer to use the query builder.


Most query builder classes take one or more DatabaseModel's as parameters. Depending on the methods called, you can use query builder and still get objects returned.


We will start with a basic select query:

**Simple Select Query**

The simplest query is the Select Query.

	selQ = SelectQuery(Person, selectFields=['first_name', 'age'], orderByField='age', orderByDir='DESC', limitNum=50)

	rows = selQ.executeGetRows()

This will return a list of tuples, each containing first\_name followed by age. Each one of these tuples is a returned row. They will be sorted in descending order based on the 'age' field. No more than 50 items will be returned.

Default is to select all fields, no explicit order by, no explicit order direction, and no limit.


You can also have the Model objects returned with all selected fields filled in.

	selQ = SelectQuery(Person, orderByField='age', orderByDir='DESC')

	peopleObjs = selQ.executeGetObjs()

This will fetch all fields and return People objects for each one. This would be the same as calling Person.all(), except the results are ordered by age descending.



Wheres
------

Now it's not very useful to return all objects, we want to be able to filter them.

All query types have a method, *addStage* which takes 1 argument, "AND" or "OR" (default "AND"). This creates a group in the WHERE clause based on conditions, added via *addCondition.* If "OR" is selected, each conditional in this group will be linked with an "OR", otherwise "AND".

*addCondition* takes a 1. Field name, 2. Field operation, 3. Right-side value

For example:

	selQ = SelectQuery(Person)

	selQWhere1 = selQ.addStage('AND')

	selQWhere1.addCondition('age', '>', 30)
	selQWhere1.addCondition('eye_color', '=', 'Blue')

	selQWhere2 = selQ.addStage('OR')

	selQWhere2.addCondition('age', '<', 35)
	selQWhere2.addCondition('last_name', '=', 'Smith')

	matchedPeople = selQ.executeGetObjs()


This will generate a query with two "groups" in the WHERE clause. The executed query will look something like this:

	SELECT * FROM person WHERE ( age > 30 AND eye_color = 'Blue' ) AND ( age < 35 or last_name = 'Smith' )


Notice the top-level stages are joined by an "AND". You can get as complicated as you want here!

The object returned by *addStage* also has an *addStage* method to add sub stages.

So, for example, if I wanted to filter where (age is > 30 and eye color is 'Blue') OR  ( age < 35 or last\_name = 'Smith' ):

	selQ = SelectQuery(Person)

	selQOuterWhere = selQ.addStage('OR')

	selQWhere1 = selQOuterWhere.addStage('AND')

	selQWhere1.addCondition('age', '>', 30)
	selQWhere1.addCondition('eye_color', '=', 'Blue')

	selQWhere2 = selQOuterWhere.addStage('OR')

	selQWhere2.addCondition('age', '<', 35)
	selQWhere2.addCondition('last_name', '=', 'Smith')

	matchedPeople = selQ.executeGetObjs()


so basically creating an "outer stage" set to OR and adding substages to that, we now get a query like:

	SELECT * FROM person WHERE ( ( age > 30 AND eye_color = 'Blue' ) OR ( age < 35 or last_name = 'Smith' ) )


Advanced Select / Join Multiple Tables
--------------------------------------


**SelectInnerJoinQuery**

This performs an inner join between multiple tables. This should generally not be used over the more powerful SelectGenericJoinQuery

Pass as the first argument a list of Models to use.

For selectFields, prefix with the table name ( e.x. "person.age" )

For conditionals, do the same. Make sure conditionals perform the joins!

	
	selQ = SelectInnerJoinQuery( [Person, Meal] )

	selQWhere = selQ.addStage('AND')

	selQWhere.addCondition('meal.id_person', '=', 'person.id')

	# As dict objs
	dictObjs = selQ.executeGetDictObjs()

	# Or as a mapping
	mapping = selQ.executeGetMapping()


This will generate a query like

	SELECT person.*, meal.* FOR person, meal WHERE meal.id_person = person.id


**SelectGenericJoinQuery**

This is the prefered method for getting the results of joined tables.

It take sthe primary model ( the FROM ) as the first argument.

For selectFields, prefix with the table name ( e.x. "person.age" )


	selQ = SelectGenericJoinQuery( Person )

	selQWhere = selQ.addStage()

	selQWhere.addCondition('age', '>', 18)


Join on another table by calling *joinModel* passing the model to join, a join type constant JOIN\_\* (e.x. JOIN\_INNER, JOIN\_LEFT, JOIN\_RIGHT, JOIN\_OUTER\_FULL) , and "AND" or "OR" outer-mode for this stage.

The stage is returned so you can call .addCondition on it to add more conditionals on the join line.


	joinWhere = selQ.joinModel( Meal, 'INNER', 'AND' )

	joinWhere.addCondition( 'id_person', '=', Person.PRIMARY_KEY )

	# As dict objs
	dictObjs = selQ.executeGetDictObjs()

	# Or as a mapping
	mapping = selQ.executeGetMapping()


If you call "executeGetDictObjs" you will get a list of DictObjs. This is an object where access is supported either via dot (.field) or sub (['field']). The first level is the table name, the second level is the field names. For example, obj['person']['first_name'] would be the person.first\_name field

If you call executeGetMapping you will get a list of OrderedDict (in same order specified in selectFields). For example, obj['person.first\_name'] if you named the field like that in selectFields, or if you just had selectFields=['first\_name'... ] then it would be obj['first\_name']


This will generate a query like:

	SELECT * from Person
	INNER JOIN Meal ON ( meal.id_person = person.id )
	WHERE person.age > 18



Update Query
------------


Update queries use the UpdateQuery object. The stages work the same as in a SelectQuery.

Use the method *setFieldValue* to update the value of a field.

	upQ = UpdateQuery(Person)

	upQ.setFieldValue('title', 'Customer Care Expert')

	upQWhere = upQ.addStage()

	upQWhere.addCondition('title', '=', 'Customer Service Rep')

	upQ.executeUpdate()


*execute* can also be used as an alias to *executeUpdate*

The *executeUpdate* method has a parameter *replaceSpecialValues*. When True, this will convert special values such as the string 'NOW()' and 'current\_timestamp' with a datetime of now.

Also keep in mind that you can pass a getDatabaseConnection(isTransactionMode=True) to executeUpdate and set doCommit=False to link multiple updates or inserts and updates into a single transaction (executed when dbConn.commit() is called)


Insert Query
------------

An InsertQuery object is used to build queries to perform inserts.

	insQ = InsertQuery(Person)

	insQ.setFieldValue('first_name', 'Tim')
	insQ.setFieldValue('age', 22)

	insQ.executeInsert()

*execute* can also be used as an alias to *executeInsert*

Also keep in mind that you can pass a getDatabaseConnection(isTransactionMode=True) to executeInsert and set doCommit=False to link multiple inserts or inserts and updates into a single transaction (executed when dbConn.commit() is called)


Delete Query
------------

A DeleteQuery object is used to build queries to delete records

	delQ = DeleteQuery(Person)

	delQWhere = delQ.addStage()

	delQWhere.addCondition('age', '<', 18)

	delQWhere.executeDelete()


*execute* can also be used as an alias to *executeDelete*

Keep in mind you can also delect records in a transaction by passing dbConn and doCommit=False to *execute* or *executeDelete*. Changes will be applied when *commit* is called on that connection.


