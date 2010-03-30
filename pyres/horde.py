import sys
try:
    import multiprocessing
except:
    sys.exit("multiprocessing was not available")

import time, os, signal
import datetime
import logging

from pyres import ResQ, Stat
from pyres.exceptions import NoQueueError
from pyres.utils import OrderedDict
from pyres.job import Job
import pyres.json_parser as json

class Minion(multiprocessing.Process):
    def __init__(self, queues, server, password):
        #super(Minion,self).__init__(name='Minion')
        multiprocessing.Process.__init__(self, name='Minion')
        
        format = '%(asctime)s %(levelname)s %(filename)s-%(lineno)d: %(message)s'
        logHandler = logging.StreamHandler()
        logHandler.setFormatter(logging.Formatter(format))
        self.logger = multiprocessing.get_logger()
        self.logger.addHandler(logHandler)
        self.logger.setLevel(logging.DEBUG)
        
        self.queues = queues
        self._shutdown = False
        self.hostname = os.uname()[1]
        self.server = server
        self.password = password
        
    def prune_dead_workers(self):
        pass
    
    def schedule_shutdown(self, signum, frame):
        self._shutdown = True
    
    def register_signal_handlers(self):
        signal.signal(signal.SIGTERM, self.schedule_shutdown)
        signal.signal(signal.SIGINT, self.schedule_shutdown)
        signal.signal(signal.SIGQUIT, self.schedule_shutdown)
    
    def register_minion(self):
        self.resq.redis.sadd('resque:minions',str(self))
        self.started = datetime.datetime.now()
    
    def startup(self):
        self.register_signal_handlers()
        self.prune_dead_workers()
        self.register_minion()
    
    def __str__(self):
        return '%s:%s:%s' % (self.hostname, self.pid, ','.join(self.queues))
    
    def reserve(self):
        for q in self.queues:
            self.logger.debug('checking queue: %s' % q)
            job = Job.reserve(q, self.resq, self.__str__())
            if job:
                self.logger.info('Found job on %s' % q)
                return job
    
    def process(self, job):
        if not job:
            return
        try:
            self.working_on(job)
            return job.perform()
        except Exception, e:
            exceptionType, exceptionValue, exceptionTraceback = sys.exc_info()
            self.logger.error("%s failed: %s" % (job, e))
            job.fail(exceptionTraceback)
            self.failed()
        else:
            self.logger.info('completed job')
            self.logger.debug('job details: %s' % job)
        finally:
            self.done_working()
    
    def working_on(self, job):
        self.logger.debug('marking as working on')
        data = {
            'queue': job._queue,
            'run_at': int(time.mktime(datetime.datetime.now().timetuple())),
            'payload': job._payload
        }
        data = json.dumps(data)
        self.resq.redis["resque:minion:%s" % str(self)] = data
        self.logger.debug("minion:%s" % str(self))
        #self.logger.debug(self.resq.redis["resque:minion:%s" % str(self)])
    
    def failed(self):
        Stat("failed", self.resq).incr()
    
    def processed(self):
        total_processed = Stat("processed", self.resq)
        total_processed.incr()
    
    def done_working(self):
        self.logger.info('done working')
        self.processed()
        self.resq.redis.delete("resque:minion:%s" % str(self))
    
    def unregister_minion(self):
        self.resq.redis.srem('resque:minions',str(self))
        self.started = None
    
    def work(self, interval=5):
        
        self.startup()
        while True:
            if self._shutdown:
                self.logger.info('shutdown scheduled')
                break
            job = self.reserve()
            if job:
                self.process(job)
            else:
                time.sleep(interval)
        self.unregister_minion()
    
    def run(self):
        
        if isinstance(self.server,basestring):
            self.resq = ResQ(server=self.server, password=self.password)
        elif isinstance(self.server, ResQ):
            self.resq = self.server
        else:
            raise Exception("Bad server argument")
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
    def __init__(self, pool_size=5, queues=[], server='localhost:6379', password=None):
        #super(Khan,self).__init__(queues=queues,server=server,password=password)
        self._shutdown = False
        self.pool_size = int(pool_size)
        self.queues = queues
        self.server = server
        self.password = password
        self.pid = os.getpid()
        self.validate_queues()
        self._workers = OrderedDict()
        self.server = server
        self.password = password
        
        #self._workers = list()
    
    def setup_resq(self):
        if isinstance(self.server,basestring):
            self.resq = ResQ(server=self.server, password=self.password)
        elif isinstance(self.server, ResQ):
            self.resq = self.server
        else:
            raise Exception("Bad server argument")
    
    def validate_queues(self):
        "Checks if a worker is given atleast one queue to work on."
        if not self.queues:
            raise NoQueueError("Please give each worker at least one queue.")
    
    def startup(self):
        self.register_signal_handlers()
    
    def register_signal_handlers(self):
        signal.signal(signal.SIGTERM, self.schedule_shutdown)
        signal.signal(signal.SIGINT, self.schedule_shutdown)
        signal.signal(signal.SIGQUIT, self.schedule_shutdown)
        signal.signal(signal.SIGUSR1, self.kill_child)
        signal.signal(signal.SIGUSR1, self.add_child)
    
    def _schedule_shutdown(self):
        self.schedule_shutdown(None, None)
    
    def schedule_shutdown(self, signum, frame):
        logging.info('Khan Shutdown scheduled')
        self._shutdown = True
    
    def kill_child(self, signum, frame):
        self._remove_minion()
    
    def add_child(self, signum, frame):
        self.add_minion()
    
    def register_khan(self):
        if not hasattr(self, 'resq'):
            self.setup_resq()
        
        self.resq.redis.sadd('resque:khans',str(self))
        self.started = datetime.datetime.now()
    
    def _check_commands(self):
        if not self._shutdown:
            logging.debug('Checking commands')
            command_key = 'resque:khan:%s' % self
            
            command = self.resq.redis.lpop('resque:khan:%s' % str(self))
            logging.debug('COMMAND FOUND: %s ' % command)
            if command:
                import pdb;pdb.set_trace()
                self.process_command(command)
                self._check_commands()
    
    def process_command(self, command):
        logging.info('Processing Command')
        #available commands, shutdown, add 1, remove 1
        command_item = self._command_map.get(command, None)
        if command_item:
            fn = getattr(self, command_item)
            if fn:
                fn()
    
    def add_minion(self):
        m = self._add_minion()
        m.start()
    
    def _add_minion(self):
        logging.info('Adding minion')
        #parent_conn, child_conn = multiprocessing.Pipe()
        m = Minion(self.queues, self.server, self.password)
        #m.start()
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
    
    def unregister_khan(self):
        logging.debug('unregistering khan')
        self.resq.redis.srem('resque:khans',str(self))
        self.started = None
    
    def work(self, interval=2):
        self.startup()
        for i in range(self.pool_size):
            m = self._add_minion()
            m.start()
            self._workers[m.pid] = m
            logging.info('minion added at %s' % m.pid)
        self.setup_resq()
        self.register_khan()
        while True:
            self._check_commands()
            if self._shutdown:
                #send signals to each child
                self._shutdown_minions()
                break
            #get job
            else:
                time.sleep(interval)
        self.unregister_khan()
    
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
