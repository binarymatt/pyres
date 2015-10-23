__version__ = '1.5'

from redis import Redis
from pyres.compat import string_types
import pyres.json_parser as json

import os
import time, datetime
import sys
import logging

logger = logging.getLogger(__name__)

def special_log_file(filename):
    if filename in ("stderr", "stdout"):
        return True
    if filename.startswith("syslog"):
        return True
    return False

def get_logging_handler(filename, procname, namespace=None):
    if namespace:
        message_format = namespace + ': %(message)s'
    else:
        message_format = '%(message)s'
    format = '%(asctime)s %(process)5d %(levelname)-8s ' + message_format

    if not filename:
        filename = "stderr"
    if filename == "stderr":
        handler = logging.StreamHandler(sys.stderr)
    elif filename == "stdout":
        handler = logging.StreamHandler(sys.stdout)
    elif filename.startswith("syslog"): # "syslog:local0"
        from logging.handlers import SysLogHandler
        facility_name = filename[7:] or 'user'
        facility = SysLogHandler.facility_names[facility_name]

        if os.path.exists("/dev/log"):
            syslog_path = "/dev/log"
        elif os.path.exists("/var/run/syslog"):
            syslog_path = "/var/run/syslog"
        else:
            raise Exception("Unable to figure out the syslog socket path")

        handler = SysLogHandler(syslog_path, facility)
        format = procname + "[%(process)d]: " + message_format
    else:
        try:
            from logging.handlers import WatchedFileHandler
            handler = WatchedFileHandler(filename)
        except:
            from logging.handlers import RotatingFileHandler
            handler = RotatingFileHandler(filename,maxBytes=52428800,
                                          backupCount=7)
    handler.setFormatter(logging.Formatter(format, '%Y-%m-%d %H:%M:%S'))
    return handler

def setup_logging(procname, log_level=logging.INFO, filename=None):
    if log_level == logging.NOTSET:
        return
    main_package = __name__.split('.', 1)[0] if '.' in __name__ else __name__
    logger = logging.getLogger(main_package)
    logger.setLevel(log_level)
    handler = get_logging_handler(filename, procname)
    logger.addHandler(handler)

def setup_pidfile(path):
    if not path:
        return
    dirname = os.path.dirname(path)
    if dirname and not os.path.exists(dirname):
        os.makedirs(dirname)
    with open(path, 'w') as f:
        f.write(str(os.getpid()))

def my_import(name):
    """Helper function for walking import calls when searching for classes by
    string names.
    """
    mod = __import__(name)
    components = name.split('.')
    for comp in components[1:]:
        mod = getattr(mod, comp)
    return mod

def safe_str_to_class(s):
    """Helper function to map string class names to module classes."""
    lst = s.split(".")
    klass = lst[-1]
    mod_list = lst[:-1]
    module = ".".join(mod_list)

    # ruby compatibility kludge: resque sends just a class name and
    # not a module name so if I use resque to queue a ruby class
    # called "Worker" then pyres will throw a "ValueError: Empty
    # module name" exception.  To avoid that, if there's no module in
    # the json then we'll use the classname as a module name.
    if not module:
        module = klass

    mod = my_import(module)
    if hasattr(mod, klass):
        return getattr(mod, klass)
    else:
        raise ImportError('')

def str_to_class(s):
    """Alternate helper function to map string class names to module classes."""
    lst = s.split(".")
    klass = lst[-1]
    mod_list = lst[:-1]
    module = ".".join(mod_list)
    try:
        mod = __import__(module)
        if hasattr(mod, klass):
            return getattr(mod, klass)
        else:
            return None
    except ImportError:
        return None

class ResQ(object):
    """The ResQ class defines the Redis server object to which we will
    enqueue jobs into various queues.


    Example usage::

        >>> from pyres import *
        >>> r = ResQ(server="192.168.1.10:6379", password="some_pwd")
            # Assuming redis is running on default port with no password

    **r** is a resque object on which we can enqueue tasks.::

        >>>> r.enqueue(SomeClass, args)

    SomeClass can be any python class with a *perform* method and a *queue*
    attribute on it.

    """
    def __init__(self, server="localhost:6379", password=None):
        """
        :param server: The IP address and port of the Redis server to which
                        you want to connect, and optional Redis DB number.
                        In the format `server:port` e.g. `localhost:6379`
        :type server: str
        :param password: The password, if required, of the Redis server
        :type password: str
        """
        self.password = password
        self.redis = server
        self._watched_queues = set()

    def push(self, queue, item):
        """Push a work item on to the specified queue.

        See the Redis Docs: `RPUSH <http://redis.io/commands/rpush>`_

        :param queue: the name of the queue to use
        :type queue: str
        :param item: the work item to put on the queue
        :type item: dict
        """
        self.watch_queue(queue)
        self.redis.rpush("resque:queue:%s" % queue, ResQ.encode(item))

    def pop(self, queues, timeout=10):
        """Fetch an item off of any of the specified queues

        See the Redis Docs: `BLPOP <http://redis.io/commands/blpop>`_

        :param queues: Names of queues to fetch from
        :type queues: `list` of `str`
        :param timeout: How long to block before giving up
        :type timeout: int

        :returns: The name of the queue and the job
        :rtype: (str, dict)
        """
        if isinstance(queues, string_types):
            queues = [queues]
        ret = self.redis.blpop(["resque:queue:%s" % q for q in queues],
                               timeout=timeout)
        if ret:
            key, ret = ret
            return key[13:].decode(), ResQ.decode(ret)  # trim "resque:queue:"
        else:
            return None, None

    def size(self, queue):
        """Get the number of items on the specified queue

        See the Redis Docs: `LLEN <http://redis.io/commands/llen>`_

        :param queue: The name of the queue to check
        :type queue: str

        :returns: The number of items on the specified queue
        :rtype: int
        """
        return int(self.redis.llen("resque:queue:%s" % queue))

    def watch_queue(self, queue):
        """Add a queue to be watched

        See the Redis Docs: `SADD <http://redis.io/commands/sadd>`_
        :param queue: the name of the queue to be watched
        :type queue: str
        """
        if queue in self._watched_queues:
            return
        else:
            if self.redis.sadd('resque:queues',str(queue)):
                self._watched_queues.add(queue)

    def peek(self, queue, start=0, count=1):
        """Fetch `count` number of items in the specified `queue`, starting at
        `start`.

        :param queue: The name of the queue to peek at
        :type queue: str
        :param start: Where in the queue to start fetching data from
        :type start: int
        :param count: The number of items to peek at
        :type count: int

        :returns: A list of items in the queue
        :rtype: `list` of `dict`
        """
        return self.list_range('resque:queue:%s' % queue, start, count)

    def list_range(self, key, start, count):
        """Fetch `count` number of items in the specified `queue`, starting at
        `start`.

        :param queue: The name of the queue to peek at.
                      It must start with 'resque:queue:'.
        :type queue: str
        :param start: Where in the queue to start fetching data from
        :type start: int
        :param count: The number of items to peek at
        :type count: int

        :returns: A list of items in the queue
        :rtype: `list` of `dict`
        """
        items = self.redis.lrange(key, start,start+count-1) or []
        ret_list = []
        for i in items:
            ret_list.append(ResQ.decode(i))
        return ret_list

    def _get_redis(self):
        return self._redis

    def _set_redis(self, server):
        if isinstance(server, string_types):
            self.dsn = server
            address, _, db = server.partition('/')
            host, port = address.split(':')
            self._redis = Redis(host=host, port=int(port), db=int(db or 0), password=self.password)
            self.host = host
            self.port = int(port)
        elif isinstance(server, Redis):
            if hasattr(server, "host"):
                self.host = server.host
                self.port = server.port
            else:
                connection = server.connection_pool.get_connection('_')
                self.host = connection.host
                self.port = connection.port
            self.dsn = '%s:%s' % (self.host, self.port)
            self._redis = server
        else:
            raise Exception("I don't know what to do with %s" % str(server))
    redis = property(_get_redis, _set_redis)

    def enqueue(self, klass, *args):
        """Enqueue a job. The job will be enqueued in to the queue name
        specified by ``klass.queue``, and it will eventually be run with
        ``klass.perform(*args)``.

        :param klass: A class instance that represents a job
        :type klass: Any class that has a ``queue`` attribute and a
                     class level ``perform`` method.
        :param args: The arguments to be passed to the ``perform`` method of
                     the ``klass`` when it is called.
        :type args: list

        """
        queue = getattr(klass,'queue', None)
        if queue:
            class_name = '%s.%s' % (klass.__module__, klass.__name__)
            self.enqueue_from_string(class_name, queue, *args)
        else:
            logger.warning("unable to enqueue job with class %s" % str(klass))

    def enqueue_from_string(self, klass_as_string, queue, *args, **kwargs):
        payload = {'class':klass_as_string, 'args':args, 'enqueue_timestamp': time.time()}
        if 'first_attempt' in kwargs:
            payload['first_attempt'] = kwargs['first_attempt']
        self.push(queue, payload)
        logger.info("enqueued '%s' job on queue %s" % (klass_as_string, queue))
        if args:
            logger.debug("job arguments: %s" % str(args))
        else:
            logger.debug("no arguments passed in.")

    def queues(self):
        """Get the queues that are being watched for jobs.

        See the Redis Docs: `SMEMBERS <http://redis.io/commands/smembers>`_

        :returns: A list of queue names
        :rtype: `list` of `str`
        """
        return [sm.decode() for sm in self.redis.smembers("resque:queues")] or []

    def workers(self):
        """Get the workers that are being used.

        See the Redis Docs: `SMEMBERS <http://redis.io/commands/smembers>`_

        :returns: A list of worker names
        :rtype: `list` of `str`
        """
        return [w.decode() for w in self.redis.smembers("resque:workers")] or []

    def info(self):
        """Get some information about the current status of pyres

        :returns: status information
        :rtype: {
            'pending': int,
            'processed': int,
            'queues': `list` of `str`,
            'workers': `list` of `str`,
            'failed': int,
            'servers': `list` of `str`}

        """
        pending = 0
        for q in self.queues():
             pending += self.size(q)
        return {
            'pending'   : pending,
            'processed' : Stat('processed',self).get(),
            'queues'    : len(self.queues()),
            'workers'   : len(self.workers()),
            #'working'   : len(self.working()),
            'failed'    : Stat('failed',self).get(),
            'servers'   : ['%s:%s' % (self.host, self.port)]
        }

    def keys(self):
        """Get a list of keys being used by pyres

        :returns: a list of keys
        :rtype: `list` of `str`
        """
        return [key.decode().replace('resque:','')
                for key in self.redis.keys('resque:*')]

    def reserve(self, queues):
        """Fetch a job from one of the queues in ``queues`` and mark it
        as reserved. If no jobs are available, do nothing.

        :param queues: a list of queues to try to fetch from.
        :type queues: `list` of `str` or just `str`

        :returns: the reserved :class:`Job` or `None`
        :rtype: :class:`Job` or `None`
        """
        from pyres.job import Job
        return Job.reserve(queues, self)

    def __str__(self):
        return "PyRes Client connected to %s" % self.dsn

    def working(self):
        from pyres.worker import Worker
        return Worker.working(self)

    def remove_queue(self, queue):
        """Remove a queue from the watched queues list

        See the Redis Docs: `SREM <http://redis.io/commands/srem>`_

        :param queue: The name of the queue to remove
        :type queue: str
        """
        if queue in self._watched_queues:
            self._watched_queues.remove(queue)
        self.redis.srem('resque:queues',queue)
        del self.redis['resque:queue:%s' % queue]

    def close(self):
        """Close the underlying redis connection."""
        self.redis.connection_pool.get_connection('_').disconnect()

    def enqueue_at(self, datetime, klass, *args, **kwargs):
        """Enqueue a job at a specific time. The job will be enqueued in to the
        queue name specified by ``klass.queue``, and it will be run at or after
        ``datetime`` with ``klass.perform(*args)``. Similar to :func:`enqueue`.

        .. note:: A :class:`Scheduler` instance must be
                  running for these jobs to run.

        :param datetime: When to run the the job
        :type datetime: :py:mod:`datetime.datetime`
        :param klass: A class instance that represents a job
        :type klass: Any class that has a ``queue`` attribute and a
                     class level ``perform`` method.
        :param args: The arguments to be passed to the ``perform`` method of
                     the ``klass`` when it is called.
        :type args: list

        """
        class_name = '%s.%s' % (klass.__module__, klass.__name__)
        self.enqueue_at_from_string(datetime, class_name, klass.queue, *args, **kwargs)

    def enqueue_at_from_string(self, datetime, klass_as_string, queue, *args, **kwargs):
        logger.info("scheduled '%s' job on queue %s for execution at %s" %
                     (klass_as_string, queue, datetime))
        if args:
            logger.debug("job arguments are: %s" % str(args))
        payload = {'class': klass_as_string, 'queue': queue, 'args': args}
        if 'first_attempt' in kwargs:
            payload['first_attempt'] = kwargs['first_attempt']
        self.delayed_push(datetime, payload)

    def delayed_push(self, datetime, item):
        """Schedule a work `item` to be run on a specified queue at or after
        the specified `datetime`. Similar to :func:`enqueue`.

        See the Redis Docs: `RPUSH <http://redis.io/commands/rpush>`_, \
            `ZADD <http://redis.io/commands/zadd>`_

        :param datetime: When to run the job
        :type datetime: :py:mod:`datetime.datetime`
        :param item: the work item to put on the queue
        :type item: dict
        """
        key = int(time.mktime(datetime.timetuple()))
        self.redis.rpush('resque:delayed:%s' % key, ResQ.encode(item))
        self.redis.zadd('resque:delayed_queue_schedule', key, key)

    def delayed_queue_peek(self, start, count):
        """Fetch `count` number of item numbers, starting at `start`.

        See the Redis Docs: `RANGE <http://redis.io/commands/zrange>`_

        :param start: Where in the queue to start fetching data from
        :type start: int
        :param count: The number of items to peek at
        :type count: int

        :returns: A list of items in the queue
        :rtype: `list` of `int`
        """
        return [int(item) for item in self.redis.zrange(
                'resque:delayed_queue_schedule', start, start+count) or []]

    def delayed_timestamp_peek(self, timestamp, start, count):
        """Fetch `count` number of items scheduled for a given `timestamp`,
        starting at `start`.


        :param timestamp: The timestamp to check
        :type timestamp: str
        :param start: Where in the queue to start fetching data from
        :type start: int
        :param count: The number of items to peek at
        :type count: int

        :returns: A list of items in the queue
        :rtype: `list` of `dict`
        """
        return self.list_range('resque:delayed:%s' % timestamp, start, count)

    def delayed_queue_schedule_size(self):
        """Fetch the number of scheduled jobs

        See the Redis Docs: `ZCARD <http://redis.io/commands/zcard>`_, \
            `ZRANGE <http://redis.io/commands/zrange>`_

        :returns: the number of schedule jobs
        :rtype: int
        """
        size = 0
        length = self.redis.zcard('resque:delayed_queue_schedule')
        for i in self.redis.zrange('resque:delayed_queue_schedule',0,length):
            size += self.delayed_timestamp_size(i.decode())
        return size

    def delayed_timestamp_size(self, timestamp):
        """Fetch the number of jobs scheduled for a specific time

        See the Redis Docs: `LLEN <http://redis.io/commands/llen>`_

        :param timestamp: the timestamp to check
        :type timestamp: str

        :returns: the number of jobs scheduled for the specified time
        :rtype: int
        """
        #key = int(time.mktime(timestamp.timetuple()))
        return self.redis.llen("resque:delayed:%s" % timestamp)

    def next_delayed_timestamp(self):
        """Get the timestamp of the next job that should be run

        See the Redis Docs: \
            `ZRANGEBYSCORE <http://redis.io/commands/zrangebyscore>`_

        :returns: The timestamp of the next job to run
        :rtype: str
        """
        key = int(time.mktime(ResQ._current_time().timetuple()))
        array = self.redis.zrangebyscore('resque:delayed_queue_schedule',
                                         '-inf', key, start=0, num=1)
        timestamp = None
        if array:
            timestamp = array[0]

        if timestamp:
            return timestamp.decode()

    def next_item_for_timestamp(self, timestamp):
        """Get the next job to be run at a given timestamp. If an item is
        found, it will be removed from the queue so that it will not be run
        again.

        See the Redis Docs: \
            `LPOP <http://redis.io/commands/lpop>`_, \
            `LLEN <http://redis.io/commands/llen>`_, \
            `DEL <http://redis.io/commands/del>`_, \
            `ZREM <http://redis.io/commands/zrem>`_


        :param timestamp: The timestamp to get a job for
        :type timestamp: str

        :returns: The job to run next
        :rtype: dict
        """
        #key = int(time.mktime(timestamp.timetuple()))
        key = "resque:delayed:%s" % timestamp
        ret = self.redis.lpop(key)
        item = None
        if ret:
            item = ResQ.decode(ret)
        if self.redis.llen(key) == 0:
            self.redis.delete(key)
            self.redis.zrem('resque:delayed_queue_schedule', timestamp)
        return item

    @classmethod
    def encode(cls, item):
        """Convert an `item` to a string (Currently JSON)

        :param item: the item to encode
        :type item: dict
        """
        return json.dumps(item)

    @classmethod
    def decode(cls, item):
        """Decode an `item`. If the `item` is not a string, call
        its `decode` method (`item.decode()`) before running the regular
        decoding.

        :param item: The item to decode

        :returns: The decoded item
        :rtype: `dict` or `str`
        """
        if not isinstance(item, string_types):
            item = item.decode()
        ret = json.loads(item)
        return ret

    @classmethod
    def _enqueue(cls, klass, *args):
        queue = getattr(klass,'queue', None)
        _self = cls()
        if queue:
            class_name = '%s.%s' % (klass.__module__, klass.__name__)
            _self.push(queue, {'class':class_name,'args':args,
                               'enqueue_timestamp': time.time()})

    @staticmethod
    def _current_time():
        return datetime.datetime.now()


class Stat(object):
    """A Stat class which shows the current status of the queue.

    """
    def __init__(self, name, resq):
        self.name = name
        self.key = "resque:stat:%s" % self.name
        self.resq = resq

    def get(self):
        val = self.resq.redis.get(self.key)
        if val:
            return int(val)
        return 0

    def incr(self, ammount=1):
        self.resq.redis.incr(self.key, ammount)

    def decr(self, ammount=1):
        self.resq.redis.decr(self.key, ammount)

    def clear(self):
        self.resq.redis.delete(self.key)

