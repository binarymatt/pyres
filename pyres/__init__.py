__version__ = '0.2.1'

from redis import Redis
import simplejson

import types
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
    
    def __init__(self, server="localhost:6379"):
        self.redis = server
        self._watched_queues = set()

    def push(self, queue, item):
        self.watch_queue(queue)
        self.redis.push("queue:%s" % queue, ResQ.encode(item))

    def pop(self, queue):
        ret = self.redis.pop("queue:%s" % queue)
        if ret:
            return ResQ.decode(ret)
        return ret

    def size(self, queue):
        return int(self.redis.llen("queue:%s" % queue))

    def watch_queue(self, queue):
        if queue in self._watched_queues:
            return
        else:
            if self.redis.sadd('queues',str(queue)):
                self._watched_queues.add(queue)

    def peek(self, queue, start=0, count=1):
        return self.list_range('queue:%s' % queue, start, count)

    def list_range(self, key, start, count):
        items = self.redis.lrange(key,start,start+count-1)
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
        return self.redis.smembers("queues")
    
    def info(self):
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
        self.redis.srem('queues',queue)
        del self.redis['queue:%s' % queue]
    
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
    def __init__(self, name, resq):
        self.name = name
        self.key = "stat:%s" % self.name
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
    
