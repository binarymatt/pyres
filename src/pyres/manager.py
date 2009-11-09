from pyres.exceptions import NoQueueError
class Manager(object):
    def __init__(self, queues, host):
        self.queues = queues
        self._host = host
        self.resq = ResQ(host)
        self.validate_queues()
    
    def __str__(self):
        import os; 
        hostname = os.uname()[1]
        pid = os.getpid()
        return 'Manager:%s:%s:%s' % (hostname, pid, ','.join(self.queues))
    
    def validate_queues(self):
        if not self.queues:
            raise NoQueueError("Please give each worker at least one queue.")
    
    def work(self):
        self.startup()
        while True:
            pass
    
    def startup(self):
        self.register_manager()
        self.register_signals()
    
    def register_manager(self):
        self.resq._redis.sadd('managers',str(self))
    
    def unregister_manager(self):
        self.resq._redis.srem('managers',str(self))
    
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
        for child in self._children:
            child.terminate()
    
    def start_child(self, queue):
        from pyres.worker import JuniorWorker
        p = Process(target=JuniorWorker.run, args=([queue], self._host))
        self.children.append(p)
        p.start()
        return True
    
    @classmethod
    def run(cls, queues=[], host="localhost:6379"):
        manager = cls(queues, host)
        manager.work()