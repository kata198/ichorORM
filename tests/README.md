Overview
========

These tests are written as GoodTests (https://github.com/kata198/GoodTests)

The provided script ./runTests.py will download GoodTests to the current directory (if not installed) and will run the test suite.

If you cannot reach the internet from your machine, manually download "GoodTests.py" from the above url and place it in this directory (or /usr/bin , or anywhere in your PATH ).


Configuration
=============

If you are running a postgresql server on the local host with default user-level ident ( i.e. you can type "psql" and get dropped into a prompt ) then no extra configuration may be necessary.

However, if you have specific credentials, server is on a different host, or want to segregate tests to a specific "dbname", then you must edit the config located at the top of "ichorORMTests/LocalConfig.py"


Tests Format
============

There are some global models which can be used (are guarenteed to be setup upon first invocation of test suite).
These all have a field, "datasetuid", which should be a unique uuid generated per test-class.
Because GoodTests runs each class in a process, this effectively segregates the records to the current test being executed.

Tests may also define a local model which exists just within that test unit.


**NOTE:** These tests do provide some examples of usage, but they should not be considered "the best usage."

For example, the tests make extensive use of directly executing SQL queries in order to isolate what is being tested. For example, the InsertQuery test is testing the InsertQuery class, so it will use methods on the InsertQuery to perform inserts, but testing the results does not use a SelectQuery, rather, it will SELECT with explicit SQL. This is to ensure that the InsertQuery test doesn't break if SelectQuery breaks; basically, we are isolating what is being tested.
