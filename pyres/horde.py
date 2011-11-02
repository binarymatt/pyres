import sys
try:
    import multiprocessing
except:
    sys.exit("multiprocessing was not available")

import time, os, signal
import datetime
import logging
import logging.handlers
from pyres import ResQ, Stat, get_logging_handler, special_log_file
from pyres.exceptions import NoQueueError
from pyres.utils import OrderedDict
from pyres.job import Job
import pyres.json_parser as json
try:
    from setproctitle import setproctitle
except:
    def setproctitle(name):
        pass

def setup_logging(procname, namespace='', log_level=logging.INFO, log_file=None):
    
    logger = multiprocessing.get_logger()
    #logger = multiprocessing.log_to_stderr()
    logger.setLevel(log_level)
    handler = get_logging_handler(log_file, procname, namespace)
    logger.addHandler(handler)
    return logger

class Minion(multiprocessing.Process):
    def __init__(self, queues, server, password, log_level=logging.INFO, log_path=None):
        multiprocessing.Process.__init__(self, name='Minion')
        
        #format = '%(asctime)s %(levelname)s %(filename)s-%(lineno)d: %(message)s'
        #logHandler = logging.StreamHandler()
        #logHandler.setFormatter(logging.Formatter(format))
        #self.logger = multiprocessing.get_logger()
        #self.logger.addHandler(logHandler)
        #self.logger.setLevel(logging.DEBUG)
        
        self.queues = queues
        self._shutdown = False
        self.hostname = os.uname()[1]
        self.server = server
        self.password = password
        
        self.log_level = log_level
        self.log_path = log_path
        self.log_file = None
        
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
        self.logger.debug('checking queues: %s' % self.queues)
        job = Job.reserve(self.queues, self.resq, self.__str__())
        if job:
            self.logger.info('Found job on %s' % job._queue)
            return job
    
    def process(self, job):
        if not job:
            return
        try:
            self.working_on(job)
            job.perform()
        except Exception, e:
            exceptionType, exceptionValue, exceptionTraceback = sys.exc_info()
            self.logger.error("%s failed: %s" % (job, e))
            job.fail(exceptionTraceback)
            self.failed()
        else:
            self.logger.debug("Hells yeah")
            self.logger.info('completed job: %s' % job)
        finally:
            self.done_working()
    
    def working_on(self, job):
        setproctitle('pyres_minion:%s: working on job: %s' % (os.getppid(), job._payload))
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
        self.logger.debug('done working')
        self.processed()
        self.resq.redis.delete("resque:minion:%s" % str(self))
    
    def unregister_minion(self):
        self.resq.redis.srem('resque:minions',str(self))
        self.started = None
    
    def work(self, interval=5):
        
        self.startup()
        while True:
            setproctitle('pyres_minion:%s: waiting for job on: %s' % (os.getppid(),self.queues))
            self.logger.info('waiting on job')
            if self._shutdown:
                self.logger.info('shutdown scheduled')
                break
            job = self.reserve()
            if job:
                self.process(job)
            else:
                time.sleep(interval)
        self.unregister_minion()
    
    def clear_logger(self):
        for handler in self.logger.handlers:
            self.logger.removeHandler(handler)
    
    def run(self):
        setproctitle('pyres_minion:%s: Starting' % (os.getppid(),))
        if self.log_path:
            if special_log_file(self.log_path):
                self.log_file = self.log_path
            else:
                self.log_file = os.path.join(self.log_path, 'minion-%s.log' % self.pid)
        namespace = 'minion:%s' % self.pid
        self.logger = setup_logging('minion', namespace, self.log_level, self.log_file)
        #self.clear_logger()
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
    def __init__(self, pool_size=5, queues=[], server='localhost:6379', password=None, logging_level=logging.INFO, log_file=None):
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
        self.logging_level = logging_level
        self.log_file = log_file
        
        #self._workers = list()
    
    def setup_resq(self):
        if hasattr(self,'logger'):
            self.logger.info('Connecting to redis server - %s' % self.server)
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
        signal.signal(signal.SIGUSR2, self.add_child)
    
    def _schedule_shutdown(self):
        self.schedule_shutdown(None, None)
    
    def schedule_shutdown(self, signum, frame):
        self.logger.info('Khan Shutdown scheduled')
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
            self.logger.debug('Checking commands')
            command = self.resq.redis.lpop('resque:khan:%s' % str(self))
            self.logger.debug('COMMAND FOUND: %s ' % command)
            if command:
                self.process_command(command)
                self._check_commands()
    
    def process_command(self, command):
        self.logger.info('Processing Command')
        #available commands, shutdown, add 1, remove 1
        command_item = self._command_map.get(command, None)
        if command_item:
            fn = getattr(self, command_item)
            if fn:
                fn()
    
    def add_minion(self):
        self._add_minion()
        self.resq.redis.srem('resque:khans',str(self))
        self.pool_size += 1
        self.resq.redis.sadd('resque:khans',str(self))
    
    def _add_minion(self):
        if hasattr(self,'logger'):
            self.logger.info('Adding minion')
        if self.log_file:
            if special_log_file(self.log_file):
                log_path = self.log_file
            else:
                log_path = os.path.dirname(self.log_file)
        else:
            log_path = None
        m = Minion(self.queues, self.server, self.password, log_level=self.logging_level, log_path=log_path)
        m.start()
        self._workers[m.pid] = m
        if hasattr(self,'logger'):
            self.logger.info('minion added at: %s' % m.pid)
        return m
    
    def _shutdown_minions(self):
        """
        send the SIGNINT signal to each worker in the pool.
        """
        setproctitle('pyres_manager: Waiting on children to shutdown.')
        for minion in self._workers.values():
            minion.terminate()
            minion.join()
    
    def _remove_minion(self, pid=None):
        #if pid:
        #    m = self._workers.pop(pid)
        pid, m = self._workers.popitem(False)
        m.terminate()
        self.resq.redis.srem('resque:khans',str(self))
        self.pool_size -= 1
        self.resq.redis.sadd('resque:khans',str(self))
        return m
    
    def unregister_khan(self):
        if hasattr(self,'logger'):
            self.logger.debug('unregistering khan')
        self.resq.redis.srem('resque:khans',str(self))
        self.started = None
    
    def setup_minions(self):
        for i in range(self.pool_size):
            self._add_minion()

    def _setup_logging(self):
        self.logger = setup_logging('khan', 'khan', self.logging_level, self.log_file)
    
    def work(self, interval=2):
        setproctitle('pyres_manager: Starting')
        self.startup()
        self.setup_minions()
        self._setup_logging()
        self.logger.info('Running as pid: %s' % self.pid)
        self.logger.info('Added %s child processes' % self.pool_size)
        self.logger.info('Setting up pyres connection')
        self.setup_resq()
        self.register_khan()
        setproctitle('pyres_manager: running %s' % self.queues)
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
        return '%s:%s:%s' % (hostname, self.pid, self.pool_size)
        
    @classmethod
    def run(cls, pool_size=5, queues=[], server='localhost:6379', logging_level=logging.INFO, log_file=None):
        worker = cls(pool_size=pool_size, queues=queues, server=server, logging_level=logging_level, log_file=log_file)
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
