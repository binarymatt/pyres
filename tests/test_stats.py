from tests import PyResTests
from pyres import Stat
class StatTests(PyResTests):
    def test_incr(self):
        stat_obj = Stat('test_stat', self.resq)
        stat_obj.incr()
        assert self.redis.get('resque:stat:test_stat') == str(1)
        stat_obj.incr()
        assert self.redis.get('resque:stat:test_stat') == str(2)
        stat_obj.incr(2)
        assert self.redis.get('resque:stat:test_stat') == str(4)
    
    def test_decr(self):
        stat_obj = Stat('test_stat', self.resq)
        stat_obj.incr()
        stat_obj.incr()
        assert self.redis.get('resque:stat:test_stat') == str(2)
        stat_obj.decr()
        assert self.redis.get('resque:stat:test_stat') == str(1)
        stat_obj.incr()
        stat_obj.decr(2)
        assert self.redis.get('resque:stat:test_stat') == str(0)
    
    def test_get(self):
        stat_obj = Stat('test_stat', self.resq)
        stat_obj.incr()
        stat_obj.incr()
        assert stat_obj.get() == 2
    
    def test_clear(self):
        stat_obj = Stat('test_stat', self.resq)
        stat_obj.incr()
        stat_obj.incr()
        assert self.redis.exists('resque:stat:test_stat')
        stat_obj.clear()
        assert not self.redis.exists('resque:stat:test_stat')