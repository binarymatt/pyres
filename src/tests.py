import unittest
import os
from pyres import ResQ, str_to_class, Stat
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
        
    
class ErrorObject(object):
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
        print self.resq.keys()
        assert 'queue:basic' in self.resq.keys()
        assert 'queues' in self.resq.keys()
    

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
        assert self.redis.llen('resque:failed') == 0
        job.fail("problem")
        assert self.redis.llen('resque:failed') == 1
    

class WorkerTests(PyResTests):
    def test_worker_init(self):
        from pyres.exceptions import NoQueueError
        self.assertRaises(NoQueueError, Worker,[])
        self.assertRaises(Exception, Worker,['test'],TestProcess())
    
    def test_startup(self):
        worker = Worker(['basic'])
        worker.startup()
        name = "%s:%s:%s" % (os.uname()[1],os.getpid(),'basic')
        assert self.redis.sismember('resque:workers',name)
        import signal
        assert signal.getsignal(signal.SIGTERM) == worker.shutdown_all
        assert signal.getsignal(signal.SIGINT) == worker.shutdown_all
        assert signal.getsignal(signal.SIGQUIT) == worker.schedule_shutdown
        assert signal.getsignal(signal.SIGUSR1) == worker.kill_child
    
    def test_register(self):
        worker = Worker(['basic'])
        worker.register_worker()
        name = "%s:%s:%s" % (os.uname()[1],os.getpid(),'basic')
        assert self.redis.sismember('resque:workers',name)
    
    def test_unregister(self):
        worker = Worker(['basic'])
        worker.register_worker()
        name = "%s:%s:%s" % (os.uname()[1],os.getpid(),'basic')
        assert self.redis.sismember('resque:workers',name)
        worker.unregister_worker()
        assert name not in self.redis.smembers('resque:workers')
    
    def test_working_on(self):
        name = "%s:%s:%s" % (os.uname()[1],os.getpid(),'basic')
        self.resq.enqueue(Basic,"test1")
        job = Job.reserve('basic', self.resq)
        worker = Worker(['basic'])
        worker.working_on(job)
        assert self.redis.exists("resque:worker:%s" % name)
    
    def test_processed(self):
        name = "%s:%s:%s" % (os.uname()[1],os.getpid(),'basic')
        worker = Worker(['basic'])
        worker.processed()
        assert self.redis.exists("resque:stat:processed")
        assert self.redis.exists("resque:stat:processed:%s" % name)
        assert self.redis.get("resque:stat:processed") == 1
        assert self.redis.get("resque:stat:processed:%s" % name) == 1
        worker.processed()
        assert self.redis.get("resque:stat:processed") == 2
        assert self.redis.get("resque:stat:processed:%s" % name) == 2
    
    def test_failed(self):
        name = "%s:%s:%s" % (os.uname()[1],os.getpid(),'basic')
        worker = Worker(['basic'])
        worker.failed()
        assert self.redis.exists("resque:stat:failed")
        assert self.redis.exists("resque:stat:failed:%s" % name)
        assert self.redis.get("resque:stat:failed") == 1
        assert self.redis.get("resque:stat:failed:%s" % name) == 1
        worker.failed()
        assert self.redis.get("resque:stat:failed") == 2
        assert self.redis.get("resque:stat:failed:%s" % name) == 2
    
    def test_process(self):
        name = "%s:%s:%s" % (os.uname()[1],os.getpid(),'basic')
        self.resq.enqueue(Basic,"test1")
        job = Job.reserve('basic', self.resq)
        worker = Worker(['basic'])
        worker.process(job)
        assert not self.redis.get('resque:worker:%s' % worker)
        assert not self.redis.get("resque:stat:failed")
        assert not self.redis.get("resque:stat:failed:%s" % name)
        self.resq.enqueue(Basic,"test1")
        worker.process()
        assert not self.redis.get('resque:worker:%s' % worker)
        assert not self.redis.get("resque:stat:failed")
        assert not self.redis.get("resque:stat:failed:%s" % name)
        
    
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
        self.resq.enqueue(ErrorObject)
        worker = Worker(['basic'])
        worker.process()
        name = "%s:%s:%s" % (os.uname()[1],os.getpid(),'basic')
        assert not self.redis.get('resque:worker:%s' % worker)
        assert self.redis.get("resque:stat:failed") == 1
        assert self.redis.get("resque:stat:failed:%s" % name) == 1
    
    def test_get_job(self):
        worker = Worker(['basic'])
        self.resq.enqueue(Basic,"test1")
        job = Job.reserve('basic', self.resq)
        worker.working_on(job)
        name = "%s:%s:%s" % (os.uname()[1],os.getpid(),'basic')
        assert worker.job() == ResQ.decode(self.redis.get('resque:worker:%s' % name))
        worker.done_working()
        w2 = Worker(['basic'])
        print w2.job()
        assert w2.job() == {}
    
    def test_working(self):
        worker = Worker(['basic'])
        self.resq.enqueue_from_string('tests.Basic','basic','test1')
        worker.register_worker()
        job = Job.reserve('basic', self.resq)
        worker.working_on(job)
        name = "%s:%s:%s" % (os.uname()[1],os.getpid(),'basic')
        workers = Worker.working(self.resq)
        assert len(workers) == 1
        print "...ccc...",str(worker)
        print "..ddd...",str(workers[0])
        assert str(worker) == str(workers[0])
        assert worker != workers[0]

class StatTests(PyResTests):
    def test_incr(self):
        stat_obj = Stat('test_stat', self.resq)
        stat_obj.incr()
        assert self.redis.get('resque:stat:test_stat') == 1
        stat_obj.incr()
        assert self.redis.get('resque:stat:test_stat') == 2
        stat_obj.incr(2)
        assert self.redis.get('resque:stat:test_stat') == 4
    
    def test_decr(self):
        stat_obj = Stat('test_stat', self.resq)
        stat_obj.incr()
        stat_obj.incr()
        assert self.redis.get('resque:stat:test_stat') == 2
        stat_obj.decr()
        assert self.redis.get('resque:stat:test_stat') == 1
        stat_obj.incr()
        stat_obj.decr(2)
        assert self.redis.get('resque:stat:test_stat') == 0
    
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
    
if __name__ == "__main__":
    unittest.main()
