'''
    Copyright (c) 2016-2018 Timothy Savannah

    Licensed under the terms of the Lesser GNU Lesser General Public License version 2.1
    
      license can be found at https://raw.githubusercontent.com/kata198/ichorORM/master/LICENSE

    connection - Database access stuff
'''
# vim: set ts=4 sw=4 expandtab:


import sys
import threading
import traceback
import time
import psycopg2
import psycopg2.extensions as psycopg2_ext

from .objs import IgnoreParameter, UseGlobalSetting

__all__ = ('setGlobalConnectionParams', 'getDatabaseConnection', 'DatabaseConnection', 'DatabaseConnectionFailure')

global DEFAULT_HOST
global DEFAULT_PORT
global DEFAULT_DBNAME
global DEFAULT_USER
global DEFAULT_PASS

DEFAULT_HOST = None
DEFAULT_PORT = None
DEFAULT_DBNAME = None
DEFAULT_USER = None
DEFAULT_PASS = None


def setGlobalConnectionParams(host=IgnoreParameter, port=IgnoreParameter, dbname=IgnoreParameter, user=IgnoreParameter, password=IgnoreParameter):
    '''
        setGlobalConnectionParams - Sets the global connection parameters which will be used by default
                                      for connections to postgresql.

                                    Every parameter defaults to "IgnoreParameter" and will thus not be set unless
                                      specified to be something different.

                                    i.e. to change the database with which to connect, you can call just:

                                      setGlobalConnectionParams( dbname="blah" )

                                    without upsetting the existing host, username, etc.

                            @param host <str> default IgnoreParameter - The hostname/ip with which to connect

                            @param port <int> default IgnoreParameter - If alternate port than 5432, specify here.

                            @param dbname <str> default IgnoreParameter - The database name with which to USE

                            @param user <str/None> default IgnoreParameter - The username with which to use.
                                                    Use None (the default state) to not provide a username.

                            @param password <str> default IgnoreParameter - The password with which to use
                                                    Use None (the default state) to not provide a password.
    '''
    global DEFAULT_HOST
    global DEFAULT_PORT
    global DEFAULT_USER
    global DEFAULT_PASS
    global DEFAULT_DBNAME

    if host != IgnoreParameter:
        DEFAULT_HOST = host
    if port != IgnoreParameter:
        DEFAULT_PORT = port
    if dbname != IgnoreParameter:
        DEFAULT_DBNAME = dbname
    if user != IgnoreParameter:
        DEFAULT_USER = user
    if password != IgnoreParameter:
        DEFAULT_PASS = password


def resolveConnectionParamsTuple(host=UseGlobalSetting, port=UseGlobalSetting, dbname=UseGlobalSetting, user=UseGlobalSetting, password=UseGlobalSetting):
    '''
        resolveConnectionParamsTuple - Return a tuple of connection parameters based off provided parameters
                                        and global defaults.

                   For any of the connection parameters which are left at the
                     default UseGlobalSetting, the global setting will be used.
                   If None is provided, the connection string will not include the given element.
                   If a value is provided, it will be used in the connection string.

                   e.x. resolveConnectionParamsTuple(dbname='otherdb') will return dbname as 'otherdb',
                     and pull the remainder of settings (host, user, password) from the global.

                   Use this to establish alternate connections.

                  @param host <str/None/UseGlobalSetting> default UseGlobalSetting
                                                - If provided, use this hostname, if None don't provide host,
                                                    if UseGlobalSetting use the global setting.

                  @param port <str/None/UseGlobalSetting> default UseGlobalSetting
                                                - If provided, use this as the port. If None, use default [5432].
                                                    if UseGlobalSetting use the global setting.

                  @param dbname <str/None/UseGlobalSetting> default UseGlobalSetting
                                                - If provided, use this dbname, if None don't provide dbname,
                                                    if UseGlobalSetting use the global setting.

                  @param user <str/None/UseGlobalSetting> default UseGlobalSetting
                                                - If provided, use this username, if None don't provide username,
                                                    if UseGlobalSetting use the global setting.

                  @param password <str/None/UseGlobalSetting> default UseGlobalSetting
                                                - If provided, use this password, if None don't provide password,
                                                    if UseGlobalSetting use the global setting.


                  @return < tuple<str/None> > - A tuple of (host, port, dbname, username, password)
    '''

    global DEFAULT_HOST
    global DEFAULT_USER
    global DEFAULT_PASS
    global DEFAULT_DBNAME
    global DEFAULT_PORT

    if host == UseGlobalSetting:
        host = DEFAULT_HOST
    if port == UseGlobalSetting:
        port = DEFAULT_PORT
    if dbname == UseGlobalSetting:
        dbname = DEFAULT_DBNAME
    if user == UseGlobalSetting:
        user = DEFAULT_USER
    if password == UseGlobalSetting:
        password = DEFAULT_PASS

    return (host, port, dbname, user, password)


def resolveConnectionParamsDict(host=UseGlobalSetting, port=UseGlobalSetting, dbname=UseGlobalSetting, user=UseGlobalSetting, password=UseGlobalSetting):
    '''
        resolveConnectionParamsDict - Return a dict of connection parameters based off provided parameters
                                        and global defaults.

                   For any of the connection parameters which are left at the
                     default UseGlobalSetting, the global setting will be used.
                   If None is provided, the connection string will not include the given element.
                   If a value is provided, it will be used in the connection string.

                   e.x. resolveConnectionParamsDict(dbname='otherdb') will return dbname as 'otherdb',
                     and pull the remainder of settings (host, user, password) from the global.

                   Use this to establish alternate connections.

                  @param host <str/None/UseGlobalSetting> default UseGlobalSetting
                                                - If provided, use this hostname, if None don't provide host,
                                                    if UseGlobalSetting use the global setting.

                  @param port <str/None/UseGlobalSetting> default UseGlobalSetting
                                                - If provided, use this as the port. If None, use default [5432].
                                                    if UseGlobalSetting use the global setting.

                  @param dbname <str/None/UseGlobalSetting> default UseGlobalSetting
                                                - If provided, use this dbname, if None don't provide dbname,
                                                    if UseGlobalSetting use the global setting.

                  @param user <str/None/UseGlobalSetting> default UseGlobalSetting
                                                - If provided, use this username, if None don't provide username,
                                                    if UseGlobalSetting use the global setting.

                  @param password <str/None/UseGlobalSetting> default UseGlobalSetting
                                                - If provided, use this password, if None don't provide password,
                                                    if UseGlobalSetting use the global setting.

                  @return dict<<str> -> <str/None>>  - A dict of 'host' -> hostname, 'dbname' -> dbname,
                                                                 'user' -> username, 'password' -> password
    '''

    global DEFAULT_HOST
    global DEFAULT_USER
    global DEFAULT_PASS
    global DEFAULT_DBNAME
    global DEFAULT_PORT

    if host == UseGlobalSetting:
        host = DEFAULT_HOST
    if port == UseGlobalSetting:
        port = DEFAULT_PORT
    if dbname == UseGlobalSetting:
        dbname = DEFAULT_DBNAME
    if user == UseGlobalSetting:
        user = DEFAULT_USER
    if password == UseGlobalSetting:
        password = DEFAULT_PASS

    return { 'host' : host, 'port' : port, 'dbname' : dbname, 'user' : user, 'password' : password }

# TODO: Make this info come from a config file
MAX_LOCK_TIMEOUT = 10.0


def getDatabaseConnection(host=UseGlobalSetting, port=UseGlobalSetting, dbname=UseGlobalSetting, user=UseGlobalSetting, password=UseGlobalSetting, isTransactionMode=False):
    '''
        getDatabaseConnection - Gets a database connection.

            Should use this instead of creating a DatabaseConnection manually, to ensure ease of refactoring
              (like if we introduce connection pooling, global connections, etc)

        @see DatabaseConnection.__init__ for arguments

        @return DatabaseConnection object
    '''
    (host, port, dbname, user, password) = resolveConnectionParamsTuple(host, port, dbname, user, password)

    return DatabaseConnection(host=host, port=port, dbname=dbname, user=user, password=password, isTransactionMode=isTransactionMode)


class DatabaseConnection(object):
    '''
        DatabaseConnection - Manages connections to the postgresql database
    '''

    def __init__(self, host=UseGlobalSetting, port=UseGlobalSetting, dbname=UseGlobalSetting, user=UseGlobalSetting, password=UseGlobalSetting, isTransactionMode=False):
        '''
            __init__ - Create a DatabaseConnection object

              @param host <str/None/UseGlobalSetting> Default UseGlobalSetting -
                                    IP or hostname to postgresql server.
                                      If left at default UseGlobalSetting, the global value
                                        will be used. Use "None" to not provide this element when connecting,
                                        otherwise give a value to use.

              @param port <int/None/UseGlobalSetting> Default UseGlobalSetting -
                                    Port number to access postgresql on.
                                      If left at default UseGlobalSetting, the global value
                                        will be used. Use "None" to use the default [5432],
                                        otherwise provide an alternate port

              @param dbname <str/None/UseGlobalSetting> Default UseGlobalSetting -
                                    Name of database to use
                                      If left at default UseGlobalSetting, the global value
                                        will be used. Use "None" to not provide this element when connecting,
                                        otherwise give a value to use.

              @param user <str/None/UseGlobalSetting> Default UseGlobalSetting -
                                    Username to use
                                      If left at default UseGlobalSetting, the global value
                                        will be used. Use "None" to not provide this element when connecting,
                                        otherwise give a value to use.

              @param password <str/None/UseGlobalSetting> Default UseGlobalSetting -
                                    Password to use
                                      If left at default UseGlobalSetting, the global value
                                        will be used. Use "None" to not provide this element when connecting,
                                        otherwise give a value to use.

              @param isTransactionMode <bool> default False, whether or not to default this connection to using transactions.
                If False, autocommit is enabled.

        '''

        (host, port, dbname, user, password) = resolveConnectionParamsTuple(host, port, dbname, user, password)

        self.host = host
        self.port = port
        self.user = user
        self.dbname = dbname
        self.password = password

        self.isTransaction = isTransactionMode

        self._connection = None
        self._cursor = None


    def _getConnectStr(self):
        '''
            _getConnectStr - Generate a connection string for this database connection
        '''
        connectParts = []
        if self.dbname:
            connectParts.append("dbname='%s'" %(self.dbname, ))
        if self.user:
            connectParts.append("user='%s'" %(self.user, ))
        if self.password:
            connectParts.append("password='%s'" %(self.password, ))
        if self.host:
            connectParts.append("host='%s'" %(self.host, ))
        if self.port:
            connectParts.append("port='%s'" %(str(self.port), ))

        #print ( "Returning connection string: %s" %( " ".join(connectParts), ))
        return " ".join(connectParts)
#        return "dbname='%s' user='%s' host='%s'" %(self.dbname, self.user, self.host)

    def getConnection(self, forceReconnect=False):
        '''
            getConnection - Return a psycopg2 connection based on
                connection info on this object.

                Will reuse existing connection, if present.

              @param forceReconnect <bool> default False - If True,
                will force the connection to be re-established

              @return < None/ psycopg2.connection object> -
                Connection, if successful, otherwise None

        '''
        if forceReconnect is True or self._connection is None:
            connectStr = self._getConnectStr()

            try:
                self._connection = psycopg2.connect(connectStr)
            except Exception as connectException:
                self._connection = None
                exc_info = sys.exc_info()

                traceback.print_exception(*exc_info)
                sys.stderr.write('Failed to connect to postgres database: %s\n\n' %(repr(connectStr)))
                sys.stderr.write('%s:  %s\n\n' %(connectException.__class__.__name__, str(connectException)))

                # Raise this exception? Currently, return None

        # If we are in transaction mode, set to read-commit. Otherwise, use autocommit
        #    (commit after every transaction)
        if self._connection:
            if not self.isTransaction:
                self._connection.set_isolation_level(psycopg2_ext.ISOLATION_LEVEL_AUTOCOMMIT)
            else:
                self._connection.set_isolation_level(psycopg2_ext.ISOLATION_LEVEL_READ_COMMITTED)

        return self._connection

    def closeConnection(self):
        '''
            closeConnection - Close the database connection
        '''
        if self._connection:
            try:
                self._connection.close()
            except:
                pass
        self._connection = None
        self._cursor = None


    def beginTransactionMode(self):
        '''
            beginTransactionMode - Set transaction mode.
              This disables autocommit on future connection, and closes current connection.

            @see #commitTransaction to commit the current transaction
            @see #endTransactionMode to unset transaction mode

            Alias is "startTransactionMode"
        '''
        if self.isTransaction is False:
            self.closeConnection()

        self.isTransaction = True

    startTransactionMode = beginTransactionMode

    def commitTransaction(self):
        '''
            commitTransaction - Commit the current in-progress transaction.

              Requires transaction mode to be on, @see #beginTransactionMode
        '''
        if not self.isTransaction:
            return False

        return self.commit()
        # TODO: investigate a return value?


    def endTransactionMode(self):
        '''
            endTransactionMode - Disable transaction mode.
              This will enable autocommit on future connections, and closes current connection.

            @see #beginTransactionMode to re-enable transaction mode
        '''
        if self.isTransaction:
            self.closeConnection()

        self.isTransaction = False


    def getCursor(self, forceReconnect=False):
        '''
            getCursor - Gets a psycopg cursor to the database

            @param forceReconnect <bool> - Default False, if True
                will force the connection to be re-established

            @return psycopg2.cursor object
        '''
        cursor = self._cursor

        if not cursor or cursor.closed:
            conn = self.getConnection()
            if conn is None:
                raise DatabaseConnectionFailure('Could not connect to psycopg2 database.')

            cursor = conn.cursor()
            if not cursor or cursor.closed:
                freshConn = self.getConnection(forceReconnect=True)
                cursor = freshConn.cursor()

                if not cursor or cursor.closed:
                    raise DatabaseConnectionFailure('Failed to establish a cursor, even with a forced reconnect.')

        self._cursor = cursor

        return cursor


    def commit(self):
        '''
            commit - Commit whatever is on the current connection
        '''

        if not self._connection:
            return False

        self._cursor = None

        return self._connection.commit()


    def rollback(self):
        '''
            rollback - rollback whatever transaction is on the current connection
        '''
        if not self._connection:
            return False

        self._cursor = None

        return self._connection.rollback()

    def _sendSqlCommand(self, query, cursorCmdLambda=None, cursorCmdLambdaArgs=None):
        '''
            _sendSqlCommand - Send a command to the SQL server using psycopg2.

                This method covers the common retry for failed/closed cursors,
                  or disconnects, etc.
                Should not be called directly, rather use one of the implementing
                  methods. For a "raw" SQL, use #executeSql

              @param query <str> - SQL query string

              @param cursorCmdLambda <None / lambda > -

                If provided, is a lambda which recieves the cursor as
                  an argument, and should perform the action with cursor.

               Return value of this function is the return of that lambda.

                If None (default / not provided) the following default will be used:

                lambda _cursor : _cursor.execute(query)


              @param cursorCmdLambdaArgs <list/None) - If provided, will
                serve as the additional arguments to the #cursorCmdLambda
                after the common first arg, "cursor"

              @return tuple(cursor,  <???>) - The cursor used for this transaction, and  Whatever the provided lambda returns.
        '''

        ret = None

        if not cursorCmdLambdaArgs:
            cursorCmdLambdaArgs = []

        if not cursorCmdLambda:
            cursorCmdLambda = lambda _cursor : _cursor.execute(query)

        cursor = self.getCursor()

        try:
            ret = cursorCmdLambda ( cursor, *cursorCmdLambdaArgs )
        except Exception as cursorException:
            if cursor.closed is True:
                cursor = self.getCursor(forceReconnect=True)
                try:
                    ret = cursorCmdLambda ( cursor, *cursorCmdLambdaArgs )
                except Exception as cursorException2:
                    raise cursorException2
            else:
                raise cursorException

        return (cursor, ret)


    def executeSql(self, query):
        '''
            executeSql - Execute arbitrary SQL.

            @param query <str> - SQL query to execute

            No return. Maybe can guage number of rows returned etc
        '''

        (cursor, result) = self._sendSqlCommand( query )

        return result

    def executeSqlParams(self, query, params):
        '''
            executeSqlParams - Execute arbitary SQL with parameterized values

            @param query <str> - SQL Query

            @param params <dict> - Params to pass,  %(name)s  should have an entry "name"
        '''

        (cursor, result) = self._sendSqlCommand( query, lambda _cursor : _cursor.execute(query, params) )

        return result


    def doSelect(self, query):
        '''
            doSelect - Perform a SELECT query and return all the rows.
                Results are ordered by SELECT order

            @param list<tuple> - List of rows, each tuple of cols
        '''
        (cursor, result) = self._sendSqlCommand ( query )

        rows = cursor.fetchall()
        return rows

    def doSelectParams(self, query, params):
        (cursor, result) = self._sendSqlCommand( query, lambda _cursor : _cursor.execute(query, params) )

        rows = cursor.fetchall()
        return rows

    def doInsert(self, query, valueDicts=None, doCommit=True, returnPk=True):
        '''
            doInsert - Perform an INSERT query with a parameterized query

            @param query - Query string.

              For fields, e.x. "name" and "value":

                INSERT INTO mytable (name, number) VALUES ( %(name)s , %(value)s )

            @param valueDicts - A list of dicts, where each dict key = column name and value = column value.

                So with above example select, valueDicts might be:

                [
                    { 'name' : 'Jimbo' , 'value' : 18 },
                    { 'name' : 'Timbob', 'value' : 3033 },
                ]

                which would cause the insert line to be executed twice,
                  once for each row to be inserted (element in the list)

             @param doCommit <bool> Default True - If True, will commit transaction after these inserts

                (formerly named autoCommit)

             @param returnPk <bool> Default True - If True, will return the primary key(s) inserted

             @return list<int> - if returnPk is True, otherwise None
        '''
        if valueDicts is None:
            valueDicts = [{}]

        if returnPk:
            ret = []

            # TODO: investigate if we can lower overhead by using cursor.executemany with returnPk=True
            #   Calling executemany and then SELECT LASTVAL(); on the cursor only returns the pk of the
            #   last entry. So at the expense of slightly higher overhead here, we use a cursor per valueDict (per record)
            #    and select the id from each.
            for valueDict in valueDicts:
                (cursor, result) = self._sendSqlCommand ( query, lambda _cursor : _cursor.execute(query, valueDict), )
                if returnPk:
                    cursor.execute('SELECT LASTVAL();')
                    ret += [res[0] for res in cursor.fetchall()]
        else:
            ret = None
            (cursor, result) = self._sendSqlCommand ( query, lambda _cursor : _cursor.executemany(query, valueDicts), )


        if doCommit is True:
            self.commit()


        return ret


class DatabaseConnectionFailure(Exception):
    pass

# vim: set ts=4 sw=4 expandtab:
