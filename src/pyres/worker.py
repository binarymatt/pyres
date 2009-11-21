from pyres.exceptions import NoQueueError
from pyres.job import Job
from pyres import ResQ, Stat
import signal
import datetime
import os, sys
import time
import simplejson
class Worker(object):
    def __init__(self, queues=[], server="localhost:6379"):
        self.queues = queues
        self.validate_queues()
        self._shutdown = False
        self.child = None
        if isinstance(server,basestring):
            self.resq = ResQ(server)
        elif isinstance(server, ResQ):
            self.resq = server
        else:
            raise Exception("Bad server argument")
        
    
    def validate_queues(self):
        if not self.queues:
            raise NoQueueError("Please give each worker at least one queue.")
    
    def register_worker(self):
        self.resq.redis.sadd('workers',str(self))
        #self.resq._redis.add("worker:#{self}:started", Time.now.to_s)
        #Stat.clear("processed:#{self}")
        #Stat.clear("failed:#{self}")
    
    def unregister_worker(self):
        self.resq.redis.srem('workers',str(self))
    
    def startup(self):
        self.register_signal_handlers()
        self.register_worker()
    
    def register_signal_handlers(self):
        signal.signal(signal.SIGTERM, self.shutdown_all)
        signal.signal(signal.SIGINT, self.shutdown_all)
        signal.signal(signal.SIGQUIT, self.schedule_shutdown)
        signal.signal(signal.SIGUSR1, self.kill_child)
    
    def shutdown_all(self, signum, frame):
        self.schedule_shutdown(signum, frame)
        self.kill_child(signum, frame)
    
    def schedule_shutdown(self, signum, frame):
        self._shutdown = True
    
    def kill_child(self, signum, frame):
        if self.child:
            print "Killing child at %s" % self.child
            os.kill(self.child, signal.SIGKILL)
    
    def work(self, interval=5):
        self.startup()
        while True:
            if self._shutdown:
                break
            job = self.reserve()
            if job:
                print "got: %s" % job
                self.child = os.fork()
                if self.child:
                    print 'Forked %s at %s' % (self.child, datetime.datetime.now())
                    os.waitpid(self.child, 0)
                else:
                    print 'Processing %s since %s' % (job._queue, datetime.datetime.now())
                    self.process(job)
                    os._exit(0)
                self.child = None
            else:
                if interval == 0:
                    break
                time.sleep(interval)
        self.unregister_worker()
    
    def process(self, job=None):
        if not job:
            job = self.reserve()
        try:
            self.working_on(job)
            job.perform()
        except Exception, e:
            print "%s failed: %s" % (job, e)
            job.fail(e)
            self.failed()
        else:
            print "done: %s" % job
        finally:
            self.done_working()
    
    def reserve(self):
        for q in self.queues:
            print "Checking %s" % q
            job = Job.reserve(q, self.resq)
            if job:
                print "Found job on %s" % q
                return job
    
    def working_on(self, job):
        data = {
            'queue': job._queue,
            'run_at': str(datetime.datetime.now()),
            'payload': job._payload
        }
        data = simplejson.dumps(data)
        self.resq.redis["worker:%s" % str(self)] = data
    
    def job(self):
        return ResQ.decode(self.resq.redis.get("worker:%s" % self)) or {}
    
    def done_working(self):
        self.processed()
        self.resq.redis.delete("worker:%s" % str(self))
    
    def processed(self):
        total_processed = Stat("processed", self.resq)
        worker_processed = Stat("processed:%s" % str(self), self.resq)
        total_processed.incr()
        worker_processed.incr()
    
    def failed(self):
        total_failed = Stat("failed", self.resq)
        stat = Stat("failed:%s" % self, self.resq)
        total_failed.incr()
        stat.incr()
    
    def job(self):
        data = self.resq.redis.get("worker:%s" % self)
        if data:
            return ResQ.decode(data)
        return {}
    
    def __str__(self):
        if getattr(self,'id', None):
            return self.id
        import os; 
        hostname = os.uname()[1]
        pid = os.getpid()
        return '%s:%s:%s' % (hostname, pid, ','.join(self.queues))
    
    @classmethod
    def run(cls, queues, server):
        worker = cls(queues=queues, host=server)
        worker.work()
    
    @classmethod
    def all(cls, host):
        resq = ResQ(host)
        return resq.redis.smembers('workers')
    
    @classmethod
    def working(cls, host):
        resq = ResQ(host)
        total = []
        for key in Worker.all(host):
            if Worker.exists(key,resq):
                total.append(key)
        #total = [id if Worker.exists(id,resq) for id in resq.redis.smembers('workers')]
        names = [Worker.find(key[7:],resq) for key in resq._redis.mget(*total)] if total else []
        return names
    
    @classmethod
    def find(cls, worker_id, resq):
        if Worker.exists(worker_id, resq):
            queues = worker_id.split(':')[-1].split(',')
            worker = cls(queues,resq._server)
            worker.id = worker_id
            return worker
        else:
            return None
    
    @classmethod
    def exists(cls, worker_id, resq):
        return resq.redis.sismember('workers', worker_id)
    
class JuniorWorker(Worker):
    def work(self, interval=5):
        self.startup()
        while True:
            if self._shutdown:
                break
            job = self.reserve()
            if job:
                print "got: %s" % job
                self.child = os.fork()
                if self.child:
                    print 'Forked %s at %s' % (self.child, datetime.datetime.now())
                    os.waitpid(self.child, 0)
                else:
                    print 'Processing %s since %s' % (job._queue, datetime.datetime.now())
                    self.process(job)
                    os._exit(0)
                self.child = None
            else:
                break
                
        self.unregister_worker()
    

if __name__ == "__main__":
    from optparse import OptionParser
    parser = OptionParser()
    parser.add_option("-q", dest="queue_list")
    parser.add_option("-s", dest="server", default="localhost:6379")
    (options,args) = parser.parse_args()
    if not options.queue_list:
        parser.error("Please give each worker at least one queue.")
    queues = options.queue_list.split(',')
    import sys
    sys.path.insert(0,'/Users/mgeorge/dev/pyres/src')
    Worker.run(queues, options.server)