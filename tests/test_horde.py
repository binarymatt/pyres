from tests import PyResTests, Basic, TestProcess
from pyres import horde
import os

class KhanTests(PyResTests):
    def test_khan_init(self):
        from pyres.exceptions import NoQueueError
        self.assertRaises(NoQueueError, horde.Khan, 2, [])
        self.assertRaises(ValueError, horde.Khan, 'test', ['test'])
    
    def test_register_khan(self):
        khan = horde.Khan(pool_size=1, queues=['basic'])
        khan.register_khan()
        name = "%s:%s" % (os.uname()[1],os.getpid())
        assert self.redis.sismember('resque:khans',name)