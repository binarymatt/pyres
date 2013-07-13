import unittest
import os
from pyres import ResQ, str_to_class

class Basic(object):
    queue = 'basic'

    @staticmethod
    def perform(name):
        s = "name:%s" % name
        return s

class BasicMulti(object):
    queue = 'basic'
    @staticmethod
    def perform(name, age):
        print('name: %s, age: %s' % (name, age))


class ReturnAllArgsJob(object):
    queue = 'basic'

    @staticmethod
    def perform(*args):
        return args


class RetryOnExceptionJob(object):
    queue = 'basic'
    retry_every = 5
    retry_timeout = 15

    @staticmethod
    def perform(fail_until):
        if ResQ._current_time() < fail_until:
            raise Exception("Don't blame me!  I'm supposed to fail!")
        else:
            return True

class TimeoutJob(object):
    queue = 'basic'

    @staticmethod
    def perform(wait_for):
        import time
        time.sleep(wait_for)
        return "Done Sleeping"

class CrashJob(object):
    queue = 'basic'

    @staticmethod
    def perform():
        # Dangerous, this will cause a hard crash of the python process
        import ctypes
        ctypes.string_at(1)
        return "Never got here"

class PrematureExitJob(object):
    queue = 'basic'

    @staticmethod
    def perform(exit_code):
        import sys
        sys.exit(exit_code)
        return "Never got here"

class PrematureHardExitJob(object):
    queue = 'basic'

    @staticmethod
    def perform(exit_code):
        os._exit(exit_code)
        return "Never got here"

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
        print('Done Sleeping')

def test_str_to_class():
    ret = str_to_class('tests.Basic')
    assert ret
    assert ret == Basic
    assert str_to_class('hello.World') == None

class ImportTest(unittest.TestCase):
    def test_safe_str_to_class(self):
        from pyres import safe_str_to_class
        assert safe_str_to_class('tests.Basic') == Basic
        self.assertRaises(ImportError, safe_str_to_class, 'test.Mine')
        self.assertRaises(ImportError, safe_str_to_class, 'tests.World')


class PyResTests(unittest.TestCase):
    def setUp(self):
        self.resq = ResQ()
        self.redis = self.resq.redis
        self.redis.flushall()

    def tearDown(self):
        self.redis.flushall()
        del self.redis
        del self.resq
