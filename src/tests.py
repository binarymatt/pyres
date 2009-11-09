import unittest
import os
from pyres import ResQ, str_to_class
from pyres.job import Job
from pyres.worker import Worker, JuniorWorker
class Basic(object):
    queue = 'basic'
    
    @staticmethod
    def perform(name):
        return "name:%s" % name
    
def test_str_to_class():
    ret = str_to_class('tests.Basic')
    assert ret

class PyResTests(unittest.TestCase):
    def setUp(self):
        self.resq = ResQ()
        self.redis = self.resq._redis
        self.redis.flush(True)
    
    def tearDown(self):
        del self.redis
        del self.resq
    

class ResQTests(PyResTests):
    def test_enqueue(self):
        ResQ.enqueue(Basic,"test1")
        ResQ.enqueue(Basic,"test2")
        assert self.redis.llen("queue:basic") == 2
        assert self.redis.sismember('queues','basic')
    
    def test_push(self):
        self.resq.push('pushq','content-newqueue')
        self.resq.push('pushq','content2-newqueue')
        assert self.redis.llen('queue:pushq') == 2
        assert self.redis.lindex('queue:pushq', 0) == ResQ.encode('content-newqueue')
        assert self.redis.lindex('queue:pushq', 1) == ResQ.encode('content2-newqueue')
    
    def test_pop(self):
        self.resq.push('pushq','content-newqueue')
        self.resq.push('pushq','content2-newqueue')
        assert self.redis.llen('queue:pushq') == 2
        assert self.resq.pop('pushq') == 'content-newqueue'
        assert self.redis.llen('queue:pushq') == 1
        assert self.resq.pop('pushq') == 'content2-newqueue'
        assert self.redis.llen('queue:pushq') == 0
    
    def test_peek(self):
        ResQ.enqueue(Basic,"test1")
        ResQ.enqueue(Basic,"test2")
        assert len(self.resq.peek('basic',0,20)) == 2

class JobTests(PyResTests):
    def test_reserve(self):
        ResQ.enqueue(Basic,"test1")
        job = Job.reserve('basic', self.resq)
        assert job._queue == 'basic'
        assert job._payload
    
    def test_perform(self):
        ResQ.enqueue(Basic,"test1")
        job = Job.reserve('basic',self.resq)
        assert job.perform() == "name:test1"
    
    def test_fail(self):
        ResQ.enqueue(Basic,"test1")
        job = Job.reserve('basic',self.resq)
        assert self.redis.llen('failed') == 0
        job.fail("problem")
        assert self.redis.llen('failed') == 1
    

class WorkerTests(PyResTests):
    def test_worker_init(self):
        try:
            worker = Worker([])
        except:
            assert True
        
    def test_register(self):
        worker = Worker(['basic'])
        worker.register_worker()
        name = "%s:%s:%s" % (os.uname()[1],os.getpid(),'basic')
        assert self.redis.sismember('workers',name)
    
    def test_unregister(self):
        worker = Worker(['basic'])
        worker.register_worker()
        name = "%s:%s:%s" % (os.uname()[1],os.getpid(),'basic')
        assert self.redis.sismember('workers',name)
        worker.unregister_worker()
        assert name not in self.redis.smembers('workers')
    
    def test_working_on(self):
        name = "%s:%s:%s" % (os.uname()[1],os.getpid(),'basic')
        ResQ.enqueue(Basic,"test1")
        job = Job.reserve('basic', self.resq)
        worker = Worker(['basic'])
        worker.working_on(job)
        assert self.redis.exists("worker:%s" % name)
    
    def test_processed(self):
        name = "%s:%s:%s" % (os.uname()[1],os.getpid(),'basic')
        worker = Worker(['basic'])
        worker.processed()
        assert self.redis.exists("stat:processed")
        assert self.redis.exists("stat:processed:%s" % name)
        assert self.redis.get("stat:processed") == 1
        assert self.redis.get("stat:processed:%s" % name) == 1
        worker.processed()
        assert self.redis.get("stat:processed") == 2
        assert self.redis.get("stat:processed:%s" % name) == 2
    
    def test_failed(self):
        name = "%s:%s:%s" % (os.uname()[1],os.getpid(),'basic')
        worker = Worker(['basic'])
        worker.failed()
        assert self.redis.exists("stat:failed")
        assert self.redis.exists("stat:failed:%s" % name)
        assert self.redis.get("stat:failed") == 1
        assert self.redis.get("stat:failed:%s" % name) == 1
        worker.failed()
        assert self.redis.get("stat:failed") == 2
        assert self.redis.get("stat:failed:%s" % name) == 2
    
    def test_process(self):
        assert False

