#!/usr/bin/env GoodTests.py
'''
    General dummy test
'''

import subprocess
import sys


import LocalConfig


import ichorORM


class TestDummy(object):
    '''
        A dummy test
    '''

    def setup_class(self):
        LocalConfig.ensureTestSetup()

    def test_Connect(self):
        
        dbConn = ichorORM.getDatabaseConnection()
        
        pass
        pass
        b = 1
        c = 2


if __name__ == '__main__':
    sys.exit(subprocess.Popen('GoodTests.py -n1 "%s" %s' %(sys.argv[0], ' '.join(['"%s"' %(arg.replace('"', '\\"'), ) for arg in sys.argv[1:]]) ), shell=True).wait())
