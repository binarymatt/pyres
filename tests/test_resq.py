from tests import PyResTests, Basic, TestProcess
from pyres import ResQ
from pyres.worker import Worker
from pyres.job import Job
import os
class ResQTests(PyResTests):
    def test_enqueue(self):
        self.resq.enqueue(Basic,"test1")
        self.resq.enqueue(Basic,"test2", "moretest2args")
        ResQ._enqueue(Basic, "test3")
        assert self.redis.llen("resque:queue:basic") == 3
        assert self.redis.sismember('resque:queues','basic')
    
    def test_push(self):
        self.resq.push('pushq','content-newqueue')
        self.resq.push('pushq','content2-newqueue')
        assert self.redis.llen('resque:queue:pushq') == 2
        assert self.redis.lindex('resque:queue:pushq', 0) == ResQ.encode('content-newqueue')
        assert self.redis.lindex('resque:queue:pushq', 1) == ResQ.encode('content2-newqueue')
    
    def test_pop(self):
        self.resq.push('pushq','content-newqueue')
        self.resq.push('pushq','content2-newqueue')
        assert self.redis.llen('resque:queue:pushq') == 2
        assert self.resq.pop('pushq') == 'content-newqueue'
        assert self.redis.llen('resque:queue:pushq') == 1
        assert self.resq.pop('pushq') == 'content2-newqueue'
        assert self.redis.llen('resque:queue:pushq') == 0
    
    def test_peek(self):
        self.resq.enqueue(Basic,"test1")
        self.resq.enqueue(Basic,"test2")
        assert len(self.resq.peek('basic',0,20)) == 2
    
    def test_size(self):
        self.resq.enqueue(Basic,"test1")
        self.resq.enqueue(Basic,"test2")
        assert self.resq.size('basic') == 2
        assert self.resq.size('noq') == 0
    
    def test_redis_property(self):
        from redis import Redis
        rq = ResQ(server="localhost:6379")
        red = Redis()
        #rq2 = ResQ(server=red)
        self.assertRaises(Exception, rq.redis,[Basic])
    
    def test_info(self):
        self.resq.enqueue(Basic,"test1")
        self.resq.enqueue(TestProcess)
        info = self.resq.info()
        assert info['queues'] == 2
        assert info['servers'] == ['localhost:6379']
        assert info['workers'] == 0
        worker = Worker(['basic'])
        worker.register_worker()
        info = self.resq.info()
        assert info['workers'] == 1
    
    def test_workers(self):
        worker = Worker(['basic'])
        worker.register_worker()
        name = "%s:%s:%s" % (os.uname()[1],os.getpid(),'basic')
        assert len(self.resq.workers()) == 1
        #assert Worker.find(name, self.resq) in self.resq.workers()
    
    def test_enqueue_from_string(self):
        self.resq.enqueue_from_string('tests.Basic','basic','test1')
        name = "%s:%s:%s" % (os.uname()[1],os.getpid(),'basic')
        assert self.redis.llen("resque:queue:basic") == 1
        job = Job.reserve('basic', self.resq)
        worker = Worker(['basic'])
        worker.process(job)
        assert not self.redis.get('resque:worker:%s' % worker)
        assert not self.redis.get("resque:stat:failed")
        assert not self.redis.get("resque:stat:failed:%s" % name)
    
    def test_remove_queue(self):
        self.resq.enqueue_from_string('tests.Basic','basic','test1')
        assert 'basic' in self.resq._watched_queues
        assert self.redis.sismember('resque:queues','basic')
        assert self.redis.llen('resque:queue:basic') == 1
        self.resq.remove_queue('basic')
        assert 'basic' not in self.resq._watched_queues
        assert not self.redis.sismember('resque:queues','basic')
        assert not self.redis.exists('resque:queue:basic')
    
    def test_keys(self):
        self.resq.enqueue_from_string('tests.Basic','basic','test1')
        assert 'queue:basic' in self.resq.keys()
        assert 'queues' in self.resq.keys()
    
    def test_queues(self):
        assert self.resq.queues() == []
        self.resq.enqueue_from_string('tests.Basic','basic','test1')
        assert len(self.resq.queues()) == 1
        self.resq.enqueue_from_string('tests.Basic','basic','test1')
        assert len(self.resq.queues()) == 1
        self.resq.enqueue_from_string('tests.Basic','basic2','test1')
        assert len(self.resq.queues()) == 2
        assert 'test' not in self.resq.queues()
        assert 'basic' in self.resq.queues()