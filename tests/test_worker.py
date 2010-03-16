from tests import PyResTests, Basic, TestProcess, ErrorObject, RetryOnExceptionJob
from pyres import ResQ
from pyres.job import Job
from pyres.scheduler import Scheduler
from pyres.worker import Worker
import os
import time
import datetime


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
        assert self.redis.get("resque:stat:processed") == str(1)
        assert self.redis.get("resque:stat:processed:%s" % name) == str(1)
        assert worker.get_processed() == 1
        worker.processed()
        assert self.redis.get("resque:stat:processed") == str(2)
        assert self.redis.get("resque:stat:processed:%s" % name) == str(2)
        assert worker.get_processed() == 2
    
    def test_failed(self):
        name = "%s:%s:%s" % (os.uname()[1],os.getpid(),'basic')
        worker = Worker(['basic'])
        worker.failed()
        assert self.redis.exists("resque:stat:failed")
        assert self.redis.exists("resque:stat:failed:%s" % name)
        assert self.redis.get("resque:stat:failed") == str(1)
        assert self.redis.get("resque:stat:failed:%s" % name) == str(1)
        assert worker.get_failed() == 1
        worker.failed()
        assert self.redis.get("resque:stat:failed") == str(2)
        assert self.redis.get("resque:stat:failed:%s" % name) == str(2)
        assert worker.get_failed() == 2
    
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
        assert self.redis.get("resque:stat:failed") == str(1)
        assert self.redis.get("resque:stat:failed:%s" % name) == str(1)
    
    def test_get_job(self):
        worker = Worker(['basic'])
        self.resq.enqueue(Basic,"test1")
        job = Job.reserve('basic', self.resq)
        worker.working_on(job)
        name = "%s:%s:%s" % (os.uname()[1],os.getpid(),'basic')
        assert worker.job() == ResQ.decode(self.redis.get('resque:worker:%s' % name))
        assert worker.processing() == ResQ.decode(self.redis.get('resque:worker:%s' % name))
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
        assert str(worker) == str(workers[0])
        assert worker != workers[0]
    
    def test_started(self):
        import datetime
        worker = Worker(['basic'])
        dt = datetime.datetime.now()
        worker.started = dt
        name = "%s:%s:%s" % (os.uname()[1],os.getpid(),'basic')
        assert self.redis.get('resque:worker:%s:started' % name) == str(int(time.mktime(dt.timetuple())))
        assert worker.started == str(int(time.mktime(dt.timetuple())))
        worker.started = None
        assert not self.redis.exists('resque:worker:%s:started' % name)
    
    def test_state(self):
        worker = Worker(['basic'])
        assert worker.state() == 'idle'
        self.resq.enqueue_from_string('tests.Basic','basic','test1')
        worker.register_worker()
        job = Job.reserve('basic', self.resq)
        worker.working_on(job)
        assert worker.state() == 'working'
        worker.done_working()
        assert worker.state() == 'idle'
    
    def test_prune_dead_workers(self):
        worker = Worker(['basic']) # we haven't registered this worker, so the assertion below holds
        assert self.redis.scard('resque:workers') == 0
        self.redis.sadd('resque:workers',"%s:%s:%s" % (os.uname()[1],'1','basic'))
        self.redis.sadd('resque:workers',"%s:%s:%s" % (os.uname()[1],'2','basic'))
        self.redis.sadd('resque:workers',"%s:%s:%s" % (os.uname()[1],'3','basic'))
        assert self.redis.scard('resque:workers') == 3
        worker.prune_dead_workers()
        assert self.redis.scard('resque:workers') == 0
        self.redis.sadd('resque:workers',"%s:%s:%s" % ('host-that-does-not-exist','1','basic'))
        self.redis.sadd('resque:workers',"%s:%s:%s" % ('host-that-does-not-exist','2','basic'))
        self.redis.sadd('resque:workers',"%s:%s:%s" % ('host-that-does-not-exist','3','basic'))
        worker.prune_dead_workers()
        # the assertion below should hold, because the workers we registered above are on a
        # different host, and thus should not be pruned by this process
        assert self.redis.scard('resque:workers') == 3

    def test_retry_on_exception(self):
        now = datetime.datetime.now()
        self.set_current_time(now)
        worker = Worker(['basic'])
        scheduler = Scheduler()

        # queue up a job that will fail for 30 seconds
        self.resq.enqueue(RetryOnExceptionJob,
                now + datetime.timedelta(seconds=30))
        worker.process()
        assert worker.get_failed() == 0

        # check it retries the first time
        self.set_current_time(now + datetime.timedelta(seconds=5))
        scheduler.handle_delayed_items()
        assert None == worker.process()
        assert worker.get_failed() == 0

        # check it runs fine when it's stopped crashing
        self.set_current_time(now + datetime.timedelta(seconds=60))
        scheduler.handle_delayed_items()
        assert True == worker.process()
        assert worker.get_failed() == 0

    def test_retries_give_up_eventually(self):
        now = datetime.datetime.now()
        self.set_current_time(now)
        worker = Worker(['basic'])
        scheduler = Scheduler()

        # queue up a job that will fail for 60 seconds
        self.resq.enqueue(RetryOnExceptionJob,
                now + datetime.timedelta(seconds=60))
        worker.process()
        assert worker.get_failed() == 0

        # check it retries the first time
        self.set_current_time(now + datetime.timedelta(seconds=5))
        scheduler.handle_delayed_items()
        assert None == worker.process()
        assert worker.get_failed() == 0

        # check it fails when we've been trying too long
        self.set_current_time(now + datetime.timedelta(seconds=20))
        scheduler.handle_delayed_items()
        assert None == worker.process()
        assert worker.get_failed() == 1

    def set_current_time(self, time):
        ResQ._current_time = staticmethod(lambda: time)
