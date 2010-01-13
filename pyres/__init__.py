__version__ = '0.4.1'

from redis import Redis
import simplejson

import types

def my_import(name):
    mod = __import__(name)    
    components = name.split('.')    
    for comp in components[1:]:        
        mod = getattr(mod, comp)    
    return mod

def safe_str_to_class(s):
    lst = s.split(".")
    klass = lst[-1]
    mod_list = lst[:-1]
    module = ".".join(mod_list)
    try:
        mod = my_import(module)
        if hasattr(mod, klass):
            return getattr(mod, klass)
        else:
            return None
    except ImportError:
        return None

def str_to_class(s):
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
    """ResQ class which defines the Queue object to enqueue jobs into various
    queues.
    
    Example usage::

        >>> from pyres import *
        >>> r = ResQ(server="192.168.1.10:6379", password="some_pwd")
            # Assuming redis is running on default port with no password
    
    **r** is a resque object on which we can enqueue tasks.::

        >>>> r.enqueue(SomeClass, args)

    SomeClass can be any python class with *perform* method and a *queue* 
    attribute on it.
    """
    def __init__(self, server="localhost:6379", password=None, 
                 timeout=None, retry_connection=True):
        self.timeout = timeout
        self.retry_connection = retry_connection
        self.redis = server
        if password:
            self.redis.auth(password)
        self._watched_queues = set()

    def push(self, queue, item):
        self.watch_queue(queue)
        self.redis.push("resque:queue:%s" % queue, ResQ.encode(item))

    def pop(self, queue):
        ret = self.redis.pop("resque:queue:%s" % queue)
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
        items = self.redis.lrange(key, start,start+count-1)
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
            self._redis = Redis(host=host, port=int(port), 
                                retry_connection=self.retry_connection,
                                timeout=self.timeout)
        elif isinstance(server, Redis):
            self.dsn = '%s:%s' % (server.host,server.port)
            self._redis = server
        else:
            raise Exception("I don't know what to do with %s" % str(server))
    redis = property(_get_redis, _set_redis)

    def enqueue(self, klass, *args):
        """
        Enqueue a job into a specific queue. Make sure the class you are passing
        has **queue** attribute and a **perform** method on it.
        """
        queue = getattr(klass,'queue', None)
        #print cls._res
        if queue:
            class_name = '%s.%s' % (klass.__module__, klass.__name__)
            #print class_name
            self.push(queue, {'class':class_name,'args':args})
            #Job.create(queue, klass,*args)
    def enqueue_from_string(self, klass_as_string, queue, *args):
        self.push(queue, {'class':klass_as_string,'args':args})
    
    def queues(self):
        return self.redis.smembers("resque:queues")
    
    def info(self):
        """
        Returns a dictionary of the current status of the pending jobs, 
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
        return [key.replace('resque:','') for key in self.redis.keys('*')]
    
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
    
    @classmethod
    def encode(cls, item):
        return simplejson.dumps(item)

    @classmethod
    def decode(cls, item):
        if item:
            ret = simplejson.loads(item)
            return ret
        return None
    
    @classmethod
    def _enqueue(cls, klass, *args):
        queue = getattr(klass,'queue', None)
        #print cls._res
        _self = cls()
        if queue:
            class_name = '%s.%s' % (klass.__module__, klass.__name__)
            #print class_name
            _self.push(queue, {'class':class_name,'args':args})
            #Job.create(queue, klass,*args)

class Stat(object):
    """
    A Stat class which shows the current status of the queue.
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
    
