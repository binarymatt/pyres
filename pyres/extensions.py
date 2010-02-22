try:
    import multiprocessing
except:
    import sys
    sys.exit("multiprocessing was not available")
import os, datetime, time, signal
from pyres import ResQ
    
from pyres.exceptions import NoQueueError
from pyres.worker import Worker

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
    

class Manager(object):
    def __init__(self, queues, host, max_children=10):
        self.queues = queues
        self._host = host
        self.max_children = max_children
        self._shutdown = False
        self.children = []
        self.resq = ResQ(host)
        self.validate_queues()
        self.reports = {}
    
    def __str__(self):
        import os; 
        hostname = os.uname()[1]
        pid = os.getpid()
        return 'Manager:%s:%s:%s' % (hostname, pid, ','.join(self.queues))
    
    def validate_queues(self):
        if not self.queues:
            raise NoQueueError("Please give each worker at least one queue.")
    
    def check_rising(self, queue, size):
        if queue in self.reports:
            time = time.time()
            old_size = self.reports[queue][0]
            old_time = self.reports[queue][1]
            if time > old_time + 5 and size > old_size + 20:
                return True
        else:
            self.reports[queue] = (size, time.time())
            return False
    
    def work(self):
        self.startup()
        while True:
            if self._shutdown:
                break
            #check to see if stuff is still going
            for queue in self.queues:
                #check queue size
                size = self.resq.size(queue)
                if check_rising(queue,size):
                    if len(self.children) < self.max_children:
                        self.start_child(queue)
    
    def startup(self):
        self.register_manager()
        self.register_signals()
    
    def register_manager(self):
        self.resq.redis.sadd('managers',str(self))
    
    def unregister_manager(self):
        self.resq.redis.srem('managers',str(self))
    
    def register_signals(self):
        signal.signal(signal.SIGTERM, self.shutdown_all)
        signal.signal(signal.SIGINT, self.shutdown_all)
        signal.signal(signal.SIGQUIT, self.schedule_shutdown)
        signal.signal(signal.SIGUSR1, self.kill_children)
    
    def shutdown_all(self, signum, frame):
        self.schedule_shutdown(signum, frame)
        self.kill_children(signum, frame)
    
    def schedule_shutdown(self, signum, frame):
        self._shutdown = True
    
    def kill_children(self):
        for child in self.children:
            child.terminate()
    
    def start_child(self, queue):
        p = multiprocessing.Process(target=JuniorWorker.run, args=([queue], self._host))
        self.children.append(p)
        p.start()
        return True
    
    @classmethod
    def run(cls, queues=[], host="localhost:6379"):
        manager = cls(queues, host)
        manager.work()