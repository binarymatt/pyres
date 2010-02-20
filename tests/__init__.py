import unittest
import os
from pyres import ResQ, str_to_class

class Basic(object):
    queue = 'basic'
    
    @staticmethod
    def perform(name):
        s = "name:%s" % name
        print s
        return s

class ReturnAllArgsJob(object):
    queue = 'basic'

    @staticmethod
    def perform(*args):
        return args

class TestProcess(object):
    queue = 'high'
    
    @staticmethod
    def perform():
        import time
        time.sleep(.5)
        return 'Done Sleeping'
        
    
class ErrorObject(object):
    queue = 'basic'
    
    @staticmethod
    def perform():
        raise Exception("Could not finish job")

class LongObject(object):
    queue = 'long_runnning'
    
    @staticmethod
    def perform(sleep_time):
        import time
        time.sleep(sleep_time)
        print 'Done Sleeping'

def test_str_to_class():
    ret = str_to_class('tests.Basic')
    assert ret
    assert ret == Basic
    assert str_to_class('hello.World') == None

def test_safe_str_to_class():
    from pyres import safe_str_to_class
    assert safe_str_to_class('tests.Basic') == Basic
    assert safe_str_to_class('test.Mine') == None
    assert safe_str_to_class('hello.World') == None

class PyResTests(unittest.TestCase):
    def setUp(self):
        self.resq = ResQ()
        self.redis = self.resq.redis
        self.redis.flushall()
    
    def tearDown(self):
        self.redis.flushall()
        del self.redis
        del self.resq
