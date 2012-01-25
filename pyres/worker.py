import logging
import signal
import datetime, time
import os, sys
import json_parser as json
import commands
import random

from pyres.exceptions import NoQueueError
from pyres.job import Job
from pyres import ResQ, Stat, __version__



logger = logging.getLogger(__name__)

class Worker(object):
    """Defines a worker. The ``pyres_worker`` script instantiates this Worker
    class and passes a comma-separated list of queues to listen on.::

       >>> from pyres.worker import Worker
       >>> Worker.run([queue1, queue2], server="localhost:6379")

    """
    
    job_class = Job
    
    def __init__(self, queues=(), server="localhost:6379", 
                 password=None, pid=None, hostname=None):
        '''Creates a Worker instance.  Client code will usually not
        have to set pid or hostname. These are available mostly for
        the ability to set pid and hostname by methods such as 'all'

        '''
        self.queues = queues
        self.validate_queues()
        self._shutdown = False
        self.child = None
        if pid is None:
            self.pid = os.getpid()
        else:
            self.pid = pid
        if hostname is None:
            self.hostname = os.uname()[1]
        else:
            self.hostname = hostname

        if isinstance(server, basestring):
            self.resq = ResQ(server=server, password=password)
        elif isinstance(server, ResQ):
            self.resq = server
        else:
            raise Exception("Bad server argument")

    def validate_queues(self):
        """Checks if a worker is given at least one queue to work on."""
        if not self.queues:
            raise NoQueueError("Please give each worker at least one queue.")

    def register_worker(self):
        self.resq.redis.sadd('resque:workers', str(self))
        self.resq.redis.sadd('resque:worker:%s:queues' % str(self), *self.queues)
        self.started = datetime.datetime.now()

    def _set_started(self, dt):
        if dt:
            key = int(time.mktime(dt.timetuple()))
            self.resq.redis.set("resque:worker:%s:started" % self, key)
        else:
            self.resq.redis.delete("resque:worker:%s:started" % self)

    def _get_started(self):
        datestring = self.resq.redis.get("resque:worker:%s:started" % self)
        #ds = None
        #if datestring:
        #    ds = datetime.datetime.strptime(datestring, '%Y-%m-%d %H:%M:%S')
        return datestring

    started = property(_get_started, _set_started)

    def unregister_worker(self):
        self.resq.redis.srem('resque:workers', str(self))
        self.resq.redis.delete('resque:worker:%s:queues' % str(self))
        self.started = None
        Stat("processed:%s" % self, self.resq).clear()
        Stat("failed:%s" % self, self.resq).clear()

    def prune_dead_workers(self):
        all_workers = Worker.all(self.resq)
        known_workers = self.worker_pids()
        for worker in all_workers:
            host = worker.hostname
            pid =  worker.pid
            if host != self.hostname:
                continue
            if pid in known_workers:
                continue
            print self.hostname, worker.hostname, worker
            logger.warning("pruning dead worker: %s" % worker)
            worker.unregister_worker()

    def startup(self):
        self.register_signal_handlers()
        self.prune_dead_workers()
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
            logger.info("Killing child at %s" % self.child)
            os.kill(self.child, signal.SIGKILL)

    def __str__(self):
        if getattr(self,'id', None):
            return self.id
        return '%s:%s:%s' % (self.hostname, self.pid, ','.join(self.queues))

    def _setproctitle(self, msg):
        setproctitle("pyres_worker-%s [%s]: %s" % (__version__,
                                                   ','.join(self.queues),
                                                   msg))

    def work(self, interval=5):
        """Invoked by ``run`` method. ``work`` listens on a list of queues and sleeps
        for ``interval`` time.

        ``interval`` -- Number of seconds the worker will wait until processing the next job. Default is "5".

        Whenever a worker finds a job on the queue it first calls ``reserve`` on
        that job to make sure another worker won't run it, then *forks* itself to
        work on that job.

        Finally, the ``process`` method actually processes the job by eventually calling the Job instance's ``perform`` method.

        """
        self._setproctitle("Starting")
        self.startup()

        while True:
            if self._shutdown:
                logger.info('shutdown scheduled')
                break

            job = self.reserve(interval)

            if job:
                logger.debug('picked up job')
                logger.debug('job details: %s' % job)
                self.before_fork(job)
                self.child = os.fork() 
                if self.child:
                    self._setproctitle("Forked %s at %s" %
                                       (self.child,
                                        datetime.datetime.now()))
                    logger.info('Forked %s at %s' % (self.child,
                                                      datetime.datetime.now()))

                    try:
                        os.waitpid(self.child, 0)
                    except OSError as ose:
                        import errno

                        if ose.errno != errno.EINTR:
                            raise ose
                    #os.wait()
                    logger.debug('done waiting')
                else:
                    self._setproctitle("Processing %s since %s" %
                                       (job._queue,
                                        datetime.datetime.now()))
                    logger.info('Processing %s since %s' %
                                 (job._queue, datetime.datetime.now()))
                    self.after_fork(job)

                    # re-seed the Python PRNG after forking, otherwise
                    # all job process will share the same sequence of
                    # random numbers
                    random.seed()

                    self.process(job)
                    os._exit(0)
                self.child = None
            else:
                if interval == 0:
                    break
                #procline @paused ? "Paused" : "Waiting for #{@queues.join(',')}"
                self._setproctitle("Waiting")
                #time.sleep(interval)
        self.unregister_worker()

    def before_fork(self, job):
        """
        hook for making changes immediately before forking to process
        a job
        """
        pass

    def after_fork(self, job):
        """
        hook for making changes immediately after forking to process a
        job
        """
        pass

    def before_process(self, job):
        return job

    def process(self, job=None):
        if not job:
            job = self.reserve()
        try:
            self.working_on(job)
            job = self.before_process(job)
            return job.perform()
        except Exception, e:
            exceptionType, exceptionValue, exceptionTraceback = sys.exc_info()
            logger.exception("%s failed: %s" % (job, e))
            job.fail(exceptionTraceback)
            self.failed()
        else:
            logger.info('completed job')
            logger.debug('job details: %s' % job)
        finally:
            self.done_working()

    def reserve(self, timeout=10):
        logger.debug('checking queues %s' % self.queues)
        job = self.job_class.reserve(self.queues, self.resq, self.__str__(), timeout=timeout)
        if job:
            logger.info('Found job on %s' % job._queue)
            return job

    def working_on(self, job):
        logger.debug('marking as working on')
        data = {
            'queue': job._queue,
            'run_at': str(int(time.mktime(datetime.datetime.now().timetuple()))),
            'payload': job._payload
        }
        data = json.dumps(data)
        self.resq.redis["resque:worker:%s" % str(self)] = data
        logger.debug("worker:%s" % str(self))
        logger.debug(self.resq.redis["resque:worker:%s" % str(self)])

    def done_working(self):
        logger.info('done working')
        self.processed()
        self.resq.redis.delete("resque:worker:%s" % str(self))

    def processed(self):
        total_processed = Stat("processed", self.resq)
        worker_processed = Stat("processed:%s" % str(self), self.resq)
        total_processed.incr()
        worker_processed.incr()

    def get_processed(self):
        return Stat("processed:%s" % str(self), self.resq).get()

    def failed(self):
        Stat("failed", self.resq).incr()
        Stat("failed:%s" % self, self.resq).incr()

    def get_failed(self):
        return Stat("failed:%s" % self, self.resq).get()

    def job(self):
        data = self.resq.redis.get("resque:worker:%s" % self)
        if data:
            return ResQ.decode(data)
        return {}


    def processing(self):
        return self.job()

    def state(self):
        if self.resq.redis.exists('resque:worker:%s' % self):
            return 'working'
        return 'idle'

    def worker_pids(self):
        """Returns an array of all pids (as strings) of the workers on
        this machine.  Used when pruning dead workers."""
        return map(lambda l: l.strip().split(' ')[0],
                   commands.getoutput("ps -A -o pid,command | \
                                       grep pyres_worker").split("\n"))

    @classmethod
    def run(cls, queues, server="localhost:6379", interval=None):
        worker = cls(queues=queues, server=server)
        if interval is not None:
            worker.work(interval)
        else:
            worker.work()

    @classmethod
    def all(cls, host="localhost:6379"):
        if isinstance(host, basestring):
            resq = ResQ(host)
        elif isinstance(host, ResQ):
            resq = host

        return [Worker.find(w, resq) for w in resq.redis.smembers('resque:workers') or []]

    @classmethod
    def working(cls, host):
        if isinstance(host, basestring):
            resq = ResQ(host)
        elif isinstance(host, ResQ):
            resq = host
        total = []
        for key in Worker.all(host):
            total.append('resque:worker:%s' % key)
        names = []
        for key in total:
            value = resq.redis.get(key)
            if value:
                w = Worker.find(key[14:], resq) #resque:worker:
                names.append(w)
        return names

    @classmethod
    def find(cls, worker_id, resq):
        if Worker.exists(worker_id, resq):
            queues = resq.redis.smembers('resque:worker:%s:queues' % worker_id)
            bits = worker_id.split(':')
            worker = cls(queues, resq, pid=bits[1],
                         hostname=bits[0])
            worker.id = worker_id
            return worker
        else:
            return None

    @classmethod
    def exists(cls, worker_id, resq):
        return resq.redis.sismember('resque:workers', worker_id)


try:
    from setproctitle import setproctitle
except ImportError:
    def setproctitle(name):
        pass


if __name__ == "__main__":
    from optparse import OptionParser
    parser = OptionParser()
    parser.add_option("-q", dest="queue_list")
    parser.add_option("-s", dest="server", default="localhost:6379")
    (options,args) = parser.parse_args()
    if not options.queue_list:
        parser.print_help()
        parser.error("Please give each worker at least one queue.")
    queues = options.queue_list.split(',')
    Worker.run(queues, options.server)
