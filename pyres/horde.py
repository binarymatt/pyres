try:
    import multiprocessing
except:
    import sys
    sys.exit("multiprocessing was not available")

import time, os, signal
from pyres.worker import Worker
from pyres import ResQ
from pyres.utils import OrderedDict
import datetime

class Minion(multiprocessing.Process, Worker):
    def __init__(self, queues, server, password):
        multiprocessing.Process.__init__(self, name='Minion')
        self.queues = queues
        self.validate_queues()
        self._shutdown = False
        self.child = None
        if isinstance(server,basestring):
            self.resq = ResQ(server=server, password=password)
        elif isinstance(server, ResQ):
            self.resq = server
        else:
            raise Exception("Bad server argument")
        #Worker.__init__(self, queues=queues, server="localhost:6379", password=None)
        #super(Minion,self).__init__(name='Minion')
    
    def work(self, interval=5):
        self.startup()
        while True:
            if self._shutdown:
                print 'shutdown scheduled'
                break
            job = self.reserve()
            if job:
                self.process(job)
            else:
                time.sleep(interval)
        self.unregister_worker()
    
    def run(self):
        self.work()
        #while True:
        #    job = self.q.get()
        #    print 'pid: %s is running %s ' % (self.pid,job)
    

class Khan(object):
    _command_map = {
        'ADD': 'add_minion',
        'REMOVE': '_remove_minion',
        'SHUTDOWN': '_schedule_shutdown'
    }
    _workers = OrderedDict()
    def __init__(self, pool_size=5, queues=[], server='localhost:6379', password=None):
        #super(Khan,self).__init__(queues=queues,server=server,password=password)
        self._shutdown = False
        self.pool_size = pool_size
        self.queues = queues
        self.server = server
        self.password = password
        self.pid = os.getpid()
        if isinstance(server,basestring):
            self.resq = ResQ(server=server, password=password)
        elif isinstance(server, ResQ):
            self.resq = server
        else:
            raise Exception("Bad server argument")
        #self._workers = list()
    
    def startup(self):
        self.register_signal_handlers()
        self.register_worker()
    
    def register_signal_handlers(self):
        signal.signal(signal.SIGTERM, self.schedule_shutdown)
        signal.signal(signal.SIGINT, self.schedule_shutdown)
        signal.signal(signal.SIGQUIT, self.schedule_shutdown)
        signal.signal(signal.SIGUSR1, self.kill_child)
        signal.signal(signal.SIGUSR1, self.add_child)
    
    def _schedule_shutdown(self):
        self.schedule_shutdown(None, None)
    
    def schedule_shutdown(self, signum, frame):
        print 'Shutdown scheduled'
        self._shutdown = True
    
    def kill_child(self, signum, frame):
        self._remove_minion()
    
    def add_child(self, signum, frame):
        self.add_minion()
    
    def register_khan(self):
        self.resq.redis.sadd('resque:khans',str(self))
        self.started = datetime.datetime.now()
    
    def _check_commands(self):
        if not self._shutdown:
            print 'Checking commands'
            command_key = 'resque:khan:%s' % self
            command = self.resq.redis.pop(command_key)
            print 'COMMAND', command
            if command:
                self.process_command(command)
                self._check_commands()
    
    def process_command(self, command):
        print 'Processing Command'
        #available commands, shutdown, add 1, remove 1
        command = self._command_map.get(command, None)
        if command:
            fn = getattr(self, command)
            if fn:
                fn()
    
    def add_minion(self):
        print 'Adding minion'
        m = self._add_minion()
        m.start()
    
    def _add_minion(self):
        print 'Adding mminion'
        #parent_conn, child_conn = multiprocessing.Pipe()
        m = Minion(self.queues, self.server, self.password)
        #m.start()
        self._workers[m.pid] = m
        return m
        #self._workers.append(m)
    
    def _shutdown_minions(self):
        """
        send the SIGNINT signal to each worker in the pool.
        """
        for minion in self._workers.values():
            minion.terminate()
            minion.join()
    
    def _remove_minion(self, pid=None):
        #if pid:
        #    m = self._workers.pop(pid)
        pid, m = self._workers.popitem(False)
        m.terminate()
        return m
    
    def register_worker(self):
        self.resq.redis.sadd('resque:khans',str(self))
        #self.resq._redis.add("worker:#{self}:started", Time.now.to_s)
        self.started = datetime.datetime.now()
    
    def unregister_worker(self):
        print 'Unregistering'
        self.resq.redis.srem('resque:khans',str(self))
        self.started = None
    
    def work(self, interval=5):
        self.startup()
        for i in range(self.pool_size):
            m = self._add_minion()
            m.start()
        
        while True:
            self._check_commands()
            if self._shutdown:
                #send signals to each child
                self._shutdown_minions()
                break
            #get job
            else:
                time.sleep(interval)
        self.unregister_worker()
    
    def __str__(self):
        hostname = os.uname()[1]
        return '%s:%s' % (hostname, self.pid)
        
    @classmethod
    def run(cls, pool_size=5, queues=[], server='localhost:6379'):
        worker = cls(pool_size=pool_size, queues=queues, server=server)
        worker.work()

#if __name__ == "__main__":
#    k = Khan()
#    k.run()

if __name__ == "__main__":
    from optparse import OptionParser
    parser = OptionParser(usage="%prog [options] queue list")
    parser.add_option("-s", dest="server", default="localhost:6379")
    (options,args) = parser.parse_args()
    if len(args) < 1:
        parser.print_help()
        parser.error("Please give the horde at least one queue.")
    Khan.run(pool_size=2, queues=args, server=options.server)
    #khan.run()
    #Worker.run(queues, options.server)
