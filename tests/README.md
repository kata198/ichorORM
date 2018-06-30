Overview
========

These tests are written as GoodTests (https://github.com/kata198/GoodTests)

The provided script ./runTests.py will download GoodTests to the current directory (if not installed) and will run the test suite.

If you cannot reach the internet from your machine, manually download "GoodTests.py" from the above url and place it in this directory (or /usr/bin , or anywhere in your PATH ).


Configuration
=============

If you are running a postgresql server on the local host with default user-level ident ( i.e. you can type "psql" and get dropped into a prompt ) then no extra configuration may be necessary.

However, if you have specific credentials, server is on a different host, or want to segregate tests to a specific "dbname", then you must edit the config located at the top of "ichorORMTests/LocalConfig.py"
