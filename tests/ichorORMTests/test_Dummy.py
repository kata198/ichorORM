#!/usr/bin/env GoodTests.py
'''
    General dummy test
'''

import subprocess
import sys

import ichorORM


class TestDummy(object):
    '''
        A dummy test
    '''

    def test_Connect(self):
        
        dbConn = ichorORM.getDatabaseConnection()
        import pdb; pdb.set_trace()
        pass
        pass
        b = 1
        c = 2


if __name__ == '__main__':
    sys.exit(subprocess.Popen('GoodTests.py -n1 "%s" %s' %(sys.argv[0], ' '.join(['"%s"' %(arg.replace('"', '\\"'), ) for arg in sys.argv[1:]]) ), shell=True).wait())
