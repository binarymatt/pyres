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
        name = "%s:%s:1" % (os.uname()[1],os.getpid())
        assert self.redis.sismember('resque:khans',name)

    def test_unregister_khan(self):
        khan = horde.Khan(pool_size=1, queues=['basic'])
        khan.register_khan()
        name = "%s:%s:1" % (os.uname()[1],os.getpid())
        assert self.redis.sismember('resque:khans',name)
        assert self.redis.scard('resque:khans') == 1
        khan.unregister_khan()
        assert not self.redis.sismember('resque:khans', name)
        assert self.redis.scard('resque:khans') == 0

    def test_setup_minions(self):
        khan = horde.Khan(pool_size=1, queues=['basic'])
        khan.setup_minions()
        assert len(khan._workers) == 1
        khan._shutdown_minions()

    def test_setup_resq(self):
        khan = horde.Khan(pool_size=1, queues=['basic'])
        assert not hasattr(khan, 'resq')
        khan.setup_resq()
        assert hasattr(khan, 'resq')

    def test_add_minion(self):
        khan = horde.Khan(pool_size=1, queues=['basic'])
        khan.setup_minions()
        khan.register_khan()
        name = "%s:%s:1" % (os.uname()[1],os.getpid())
        assert self.redis.sismember('resque:khans',name)
        khan.add_minion()
        assert len(khan._workers) == 2
        assert not self.redis.sismember('resque:khans',name)
        name = '%s:%s:2' % (os.uname()[1], os.getpid())
        assert khan.pool_size == 2
        assert self.redis.sismember('resque:khans',name)
        khan._shutdown_minions()

    def test_remove_minion(self):
        khan = horde.Khan(pool_size=1, queues=['basic'])
        khan.setup_minions()
        khan.register_khan()
        assert khan.pool_size == 1
        khan._remove_minion()
        assert khan.pool_size == 0
