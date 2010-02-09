from tests import PyResTests, Basic, TestProcess
from pyres import horde
import os

class KhanTests(PyResTests):
    def test_khan_init(self):
        from pyres.exceptions import NoQueueError
        self.assertRaises(NoQueueError, horde.Khan, 2, [])
        self.assertRaises(ValueError, horde.Khan, 'test', ['test'])
        self.assertRaises(Exception, horde.Khan, 2, ['test'], TestProcess)
    
    def test_register_worker(self):
        khan = horde.Khan(pool_size=1, queues=['basic'])
        khan.startup()
        name = "%s:%s" % (os.uname()[1],os.getpid())
        assert khan.started
        assert self.redis.sismember('resque:khans',name)