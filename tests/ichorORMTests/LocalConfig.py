'''
    LocalConfig.py - This contains the configuration for the local system to connect to a postgres database.

    Set the values below to match your system before running tests.


    Each test should import this file and call the exported "ensureTestSetup" method in setup_class
'''

#############################################
#  Begin site customizations
#    ( set these values according to
#      your system's configuration )
#############################################

# _CONFIG_HOSTNAME - Set this to a hostname to connect to for testing,
#         or leave as None for the default "localhost"
_CONFIG_HOSTNAME = None

# _CONFIG_PORT - Set this to an alternate port if not using default 5432
_CONFIG_PORT = None

# _CONFIG_USERNAME - Set this to a username to connect with,
#         or leave as None for the default ( will use system account based on current user )
_CONFIG_USERNAME = None

# _CONFIG_PASSWORD - Set this to a password to connect with,
#         or leave as None for the default ( will not provide an explicit password )
_CONFIG_PASSWORD = None

# _CONFIG_DBNAME - Set this to a specific dbname to use,
#         or leave as None for the default ( will use postgres default, 
#           which in most configurations is same as username)
_CONFIG_DBNAME = None


#############################################
#  End site customizations
#    ( Do not modify below this line )
#############################################

__all__ = ( 'ensureTestSetup', )

# NOTE: The below code tries to just execute SQL directly and use minimum ichorORM code
#         possible. 
#  It tests that you have properly configured the connection above, and that the "base"
#    set of models for the test cases have been loaded properly,
#     and if not loads them.
#
#  Save the actual framework testing for the unit tests themselves
import ichorORM

# Set connection params
ichorORM.setGlobalConnectionParams(host=_CONFIG_HOSTNAME, port=_CONFIG_PORT, dbname=_CONFIG_DBNAME, user=_CONFIG_USERNAME, password=_CONFIG_PASSWORD)

from IchorTestInternal import ensureTestSetup

