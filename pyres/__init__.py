__version__ = '0.8'

from redis import Redis
import pyres.json_parser as json

import time, datetime

import logging

def my_import(name):
    """Helper function for walking import calls when searching for classes by string names."""
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
    
    The ``__init__`` takes these keyword arguments:
    
        ``server`` -- IP address and port of the Redis server to which you want to connect. Default is `localhost:6379`.
    
        ``password`` -- The password, if required, of your Redis server. Default is "None".
    
        ``timeout`` -- The timeout keyword is in the signature, but is unused. Default is "None".
    
        ``retry_connection`` -- This keyword is in the signature but is deprecated. Default is "True".
    
    
    Both ``timeout`` and ``retry_connection`` will be removed as the python-redis client
    no longer uses them. 
    
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
        self.redis = server
        if password:
            self.redis.auth(password)
        self._watched_queues = set()

    def push(self, queue, item):
        self.watch_queue(queue)
        self.redis.rpush("resque:queue:%s" % queue, ResQ.encode(item))

    def pop(self, queue):
        ret = self.redis.lpop("resque:queue:%s" % queue)
        if ret:
            return ResQ.decode(ret)
        return ret

    def size(self, queue):
        return int(self.redis.llen("resque:queue:%s" % queue))

    def watch_queue(self, queue):
        if queue in self._watched_queues:
            return
        else:
            if self.redis.sadd('resque:queues',str(queue)):
                self._watched_queues.add(queue)

    def peek(self, queue, start=0, count=1):
        return self.list_range('resque:queue:%s' % queue, start, count)

    def list_range(self, key, start, count):
        items = self.redis.lrange(key, start,start+count-1) or []
        ret_list = []
        for i in items:
            ret_list.append(ResQ.decode(i))
        return ret_list

    def _get_redis(self):
        return self._redis

    def _set_redis(self, server):
        if isinstance(server, basestring):
            self.dsn = server
            host, port = server.split(':')
            self._redis = Redis(host=host, port=int(port))
        elif isinstance(server, Redis):
            self.dsn = '%s:%s' % (server.host,server.port)
            self._redis = server
        else:
            raise Exception("I don't know what to do with %s" % str(server))
    redis = property(_get_redis, _set_redis)

    def enqueue(self, klass, *args):
        """Enqueue a job into a specific queue. Make sure the class you are passing
        has **queue** attribute and a **perform** method on it.
        
        """
        queue = getattr(klass,'queue', None)
        if queue:
            class_name = '%s.%s' % (klass.__module__, klass.__name__)
            self.push(queue, {'class':class_name,'args':args})
            logging.info("enqueued '%s' job" % class_name)
            if args:
                logging.debug("job arguments: %s" % str(args))
            else:
                logging.debug("no arguments passed in.")
        else:
            logging.warning("unable to enqueue job with class %s" % str(klass))

    def enqueue_from_string(self, klass_as_string, queue, *args, **kwargs):
        payload = {'class':klass_as_string, 'queue': queue, 'args':args}
        if 'first_attempt' in kwargs:
            payload['first_attempt'] = kwargs['first_attempt']
        self.push(queue, payload)
        logging.info("enqueued '%s' job" % klass_as_string)
        if args:
            logging.debug("job arguments: %s" % str(args))
        else:
            logging.debug("no arguments passed in.")
    
    def queues(self):
        return self.redis.smembers("resque:queues") or []
    
    def info(self):
        """Returns a dictionary of the current status of the pending jobs, 
        processed, no. of queues, no. of workers, no. of failed jobs.
        
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
            'servers'   : ['%s:%s' % (self.redis.host, self.redis.port)]
        }
    
    def keys(self):
        return [key.replace('resque:','') for key in self.redis.keys('resque:*')]
    
    def reserve(self, queue):
        from pyres.job import Job
        return Job.reserve(queue, self)
    
    def __str__(self):
        return "PyRes Client connected to %s" % self.redis.server
    
    def workers(self):
        from pyres.worker import Worker
        return Worker.all(self)
    
    def working(self):
        from pyres.worker import Worker
        return Worker.working(self)
    
    def remove_queue(self, queue):
        if queue in self._watched_queues:
            self._watched_queues.remove(queue)
        self.redis.srem('resque:queues',queue)
        del self.redis['resque:queue:%s' % queue]
    
    def close(self):
        """Close the underlying redis connection.
        
        """
        self.redis.disconnect()
    
    def enqueue_at(self, datetime, klass, *args, **kwargs):
        class_name = '%s.%s' % (klass.__module__, klass.__name__)
        logging.info("enqueued '%s' job for execution at %s" % (class_name, datetime))
        if args:
            logging.debug("job arguments are: %s" % str(args))
        payload = {'class':class_name, 'queue': klass.queue, 'args':args}
        if 'first_attempt' in kwargs:
            payload['first_attempt'] = kwargs['first_attempt']
        self.delayed_push(datetime, payload)
    
    def delayed_push(self, datetime, item):
        key = int(time.mktime(datetime.timetuple()))
        self.redis.rpush('resque:delayed:%s' % key, ResQ.encode(item))
        self.redis.zadd('resque:delayed_queue_schedule', key, key)
    
    def delayed_queue_peek(self, start, count):
        return [int(item) for item in self.redis.zrange('resque:delayed_queue_schedule', start, start+count) or []]
    
    def delayed_timestamp_peek(self, timestamp, start, count):
        return self.list_range('resque:delayed:%s' % timestamp, start, count)
        
    def delayed_queue_schedule_size(self):
        return self.redis.zcard('resque:delayed_queue_schedule')
    
    def delayed_timestamp_size(self, timestamp):
        #key = int(time.mktime(timestamp.timetuple()))
        return self.redis.llen("resque:delayed:%s" % timestamp)
    
    def next_delayed_timestamp(self):
        key = int(time.mktime(ResQ._current_time().timetuple()))
        array = self.redis.zrangebyscore('resque:delayed_queue_schedule', '-inf', key)
        timestamp = None
        if array:
            timestamp = array[0]
        return timestamp
    
    def next_item_for_timestamp(self, timestamp):
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
        return json.dumps(item)

    @classmethod
    def decode(cls, item):
        if isinstance(item, basestring):
            ret = json.loads(item)
            return ret
        return None
    
    @classmethod
    def _enqueue(cls, klass, *args):
        queue = getattr(klass,'queue', None)
        _self = cls()
        if queue:
            class_name = '%s.%s' % (klass.__module__, klass.__name__)
            _self.push(queue, {'class':class_name,'args':args})

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
    
