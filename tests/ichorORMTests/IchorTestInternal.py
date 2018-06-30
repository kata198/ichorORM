'''
    IchorTestInternal - This contains the code which tests that the
        test code has been configured correctly, and ensures that
        the data schema has been setup to the current version.
'''

def ensureTestSetup():
    '''
        ensureTestSetup - Call this in your setup_class method to ensure
            global schema is all setup.
    '''

    # _TEST_DATA_STATE_NUM - This marks the "state" of the database.
    #    Increment this number if a table is added or modified
    #     to ensure schema is recreated next run
    _TEST_DATA_STATE_NUM = 2

    import ichorORM
    import sys

    def _testCanConnectDatabase():
        dbConn = ichorORM.getDatabaseConnection(isTransactionMode=False)

        failed = False
        try:
            results = dbConn.doSelect("SELECT 'Hello World'")
        except Exception as e:
            failed = True
            results = []

        if not len(results) or results[0][0] != 'Hello World':
            sys.stderr.write("WARNING: Seemed to be able to connect but database could not process command.\n")
            failed = True

        if failed:
            sys.stderr.write('CANNOT connect to database. Make sure you have properly configured LocalConfig.py in the test directory.\n')
            return False
        return True

    _testResult = _testCanConnectDatabase()
    if _testResult == False:
        raise ImportError('Cannot continue until ichorORM is configured and connected to a working postgresql database.')

    del _testResult
    del _testCanConnectDatabase


    def _ensureDatabaseSetup():

        import datetime
        import uuid
        import time

        myUuid = str(uuid.uuid4())

        dbConn = ichorORM.getDatabaseConnection(isTransactionMode=True)

        isReady = False
        okToCreate = False

        # First, try to create our tests table. If we successfully create this table (it does not already exist),
        #   then we already know we are ready to create.
        try:
            dbConn.executeSql("CREATE TABLE ichor_orm_tests(id serial PRIMARY KEY, created_at timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL, my_uuid varchar(64) NOT NULL, state_num integer NOT NULL, is_complete smallint DEFAULT 0 NOT NULL, did_create_table smallint DEFAULT 0 NOT NULL); INSERT INTO ichor_orm_tests(my_uuid, state_num, did_create_table) VALUES ('%s', %d, 1)" %(myUuid, _TEST_DATA_STATE_NUM))


            dbConn.commit()
            okToCreate = True
        except:
            time.sleep(.1)
            # We failed to create the table, so now we sleep a bit and try the other logic
            isReady = False

        dbConn = ichorORM.getDatabaseConnection(isTransactionMode=False)

        def checkIfReady():
            _dbConn = ichorORM.getDatabaseConnection()
            now = datetime.datetime.now()
            isReady = False

            resultRows = _dbConn.doSelect('SELECT id, is_complete, created_at FROM ichor_orm_tests WHERE state_num = ' + str(_TEST_DATA_STATE_NUM))

            badIds = []

            for resultRow in resultRows:
                
                (resId, resIsComplete, resCreatedAt) = resultRow
                if str(resIsComplete) == '1':
                    isReady = True
                else:
                    recordAge = now - resCreatedAt
                    if recordAge.total_seconds() > 120:
                        # If more than 2 min old, clear bad record.
                        badIds.append(resId)
            if badIds:
                # If any bad ids, clear them to prevent delays in future ops
                try:
                    dbConn.executeSql('DELETE FROM ichor_orm_tests WHERE id IN ( %s )' %( ', '.join([str(badId) for badId in badIds])))
                except:
                    pass

            return isReady
        # end checkIfReady function

        dbConn = ichorORM.getDatabaseConnection(isTransactionMode=False)

        if not okToCreate:
            # Check if some process has already completed the setup
            isReady = checkIfReady()
            if isReady:
                return

            # We aren't ready, so throw our marker in
            dbConn.executeSql("INSERT INTO ichor_orm_tests(my_uuid, state_num) VALUES ('%s', %d)" %(myUuid, _TEST_DATA_STATE_NUM))
            # Wait 2 seconds
            time.sleep(2)

            # Check if we got the marker
            resultRows = dbConn.doSelect('SELECT my_uuid, created_at FROM ichor_orm_tests WHERE state_num = ' + str(_TEST_DATA_STATE_NUM) + ' ORDER BY created_at ASC')
            if resultRows[0][0] == myUuid:
                okToCreate = True
            

        if okToCreate is True:
            # At this point, we have the "lead" entry, and win the rights to create the database.
            print ( "Creating schema..." )

            from ichor_test_models.all import ALL_MODELS

            createSuccess = True

            # Drop all existing models
            for model in reversed(ALL_MODELS):
                try:
                    dbConn.executeSql('DROP TABLE %s CASCADE' %( model.TABLE_NAME, ))
                except:
                    pass

            # Create tables
            for model in ALL_MODELS:
                
                try:
                    dbConn.executeSql(model._CREATE_TABLE_SQL)
                except Exception as e:
                    createSuccess = False
                    sys.stderr.write('ERROR: Failed to create model "%s". %s  %s\n' %(model.TABLE_NAME, str(type(e)), str(e) ))


            print ( "Marking done..." )
            # We are done creating, so lets go ahead and mark everything as complete to others can continue
            if createSuccess:
                dbConn.executeSql('UPDATE ichor_orm_tests SET is_complete=1 WHERE state_num = ' + str(_TEST_DATA_STATE_NUM))
                return
            else:
                raise ImportError('Failed to create at least one model.')
        else:
            
            startWaitTime = datetime.datetime.now()

            while True:
                time.sleep(.5)

                isReady = checkIfReady()
                if isReady:
                    return

                now = datetime.datetime.now()
                
                runTime = now - startWaitTime
                if runTime.total_seconds() > 120:
                    msg = 'Error: Ran for 120 seconds waiting for table creation but did not occur. Try again?'
                    sys.stderr.write(msg + '\n')
                    raise ImportError(msg)


    _ensureDatabaseSetup()

    del _ensureDatabaseSetup

