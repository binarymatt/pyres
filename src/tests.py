import unittest
import os
from pyres import ResQ, str_to_class
from pyres.job import Job
from pyres.worker import Worker
class Basic(object):
    queue = 'basic'
    
    @staticmethod
    def perform(name):
        s = "name:%s" % name
        print s
        return s

class TestProcess(object):
    queue = 'high'
    
    @staticmethod
    def perform():
        import time
        time.sleep(.5)
        return 'Done Sleeping'
        
    
class ErrorObjcet(object):
    queue = 'basic'
    
    @staticmethod
    def perform():
        raise Exception("Could not finish job")

def test_str_to_class():
    ret = str_to_class('tests.Basic')
    assert ret

class PyResTests(unittest.TestCase):
    def setUp(self):
        self.resq = ResQ()
        self.redis = self.resq.redis
        self.redis.flush(True)
    
    def tearDown(self):
        self.redis.flush(True)
        del self.redis
        del self.resq
    

class ResQTests(PyResTests):
    def test_enqueue(self):
        self.resq.enqueue(Basic,"test1")
        self.resq.enqueue(Basic,"test2")
        ResQ._enqueue(Basic, "test3")
        assert self.redis.llen("queue:basic") == 3
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
        rq2 = ResQ(server=red)
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
        assert name in self.resq.workers()
    

class JobTests(PyResTests):
    def test_reserve(self):
        self.resq.enqueue(Basic,"test1")
        job = Job.reserve('basic', self.resq)
        assert job._queue == 'basic'
        assert job._payload
    
    def test_perform(self):
        self.resq.enqueue(Basic,"test1")
        job = Job.reserve('basic',self.resq)
        self.resq.enqueue(TestProcess)
        job2 = Job.reserve('high', self.resq)
        assert job.perform() == "name:test1"
        assert job2.perform()
    
    def test_fail(self):
        self.resq.enqueue(Basic,"test1")
        job = Job.reserve('basic',self.resq)
        assert self.redis.llen('failed') == 0
        job.fail("problem")
        assert self.redis.llen('failed') == 1
    

class WorkerTests(PyResTests):
    def test_worker_init(self):
        from pyres.exceptions import NoQueueError
        self.assertRaises(NoQueueError, Worker,[])
        self.assertRaises(Exception, Worker,['test'],TestProcess())
    
    def test_startup(self):
        worker = Worker(['basic'])
        worker.startup()
        name = "%s:%s:%s" % (os.uname()[1],os.getpid(),'basic')
        assert self.redis.sismember('workers',name)
        import signal
        assert signal.getsignal(signal.SIGTERM) == worker.shutdown_all
        assert signal.getsignal(signal.SIGINT) == worker.shutdown_all
        assert signal.getsignal(signal.SIGQUIT) == worker.schedule_shutdown
        assert signal.getsignal(signal.SIGUSR1) == worker.kill_child
    
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
        self.resq.enqueue(Basic,"test1")
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
        name = "%s:%s:%s" % (os.uname()[1],os.getpid(),'basic')
        self.resq.enqueue(Basic,"test1")
        job = Job.reserve('basic', self.resq)
        worker = Worker(['basic'])
        worker.process(job)
        assert not self.redis.get('worker:%s' % worker)
        assert not self.redis.get("stat:failed")
        assert not self.redis.get("stat:failed:%s" % name)
        self.resq.enqueue(Basic,"test1")
        worker.process()
        assert not self.redis.get('worker:%s' % worker)
        assert not self.redis.get("stat:failed")
        assert not self.redis.get("stat:failed:%s" % name)
        
    
    def test_signals(self):
        worker = Worker(['basic'])
        worker.startup()
        import inspect, signal
        frame = inspect.currentframe()
        worker.schedule_shutdown(frame, signal.SIGQUIT)
        assert worker._shutdown
        del worker
        worker = Worker(['high'])
        #self.resq.enqueue(TestSleep)
        #worker.work()
        #assert worker.child
        assert not worker.kill_child(frame, signal.SIGUSR1)
    
    def test_job_failure(self):
        self.resq.enqueue(ErrorObjcet)
        worker = Worker(['basic'])
        #worker.process()
        name = "%s:%s:%s" % (os.uname()[1],os.getpid(),'basic')
        #assert not self.redis.get('worker:%s' % worker)
        #assert self.redis.get("stat:failed")
        #assert self.redis.get("stat:failed:%s" % name)
        assert False