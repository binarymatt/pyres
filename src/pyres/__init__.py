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
    _res = None
    def __init__(self, server="localhost:6379"):
        self._server = server
        host, port = server.split(':')
        self._redis = Redis(host=host, port=int(port))
        self._watched_queues = {}
    
    def push(self, queue, item):
        self.watch_queue(queue)
        self._redis.push("queue:%s" % queue, ResQ.encode(item))
    
    def pop(self, queue):
        ret = self._redis.pop("queue:%s" % queue)
        if ret:
            return ResQ.decode(ret)
        return ret
    
    def size(self, queue):
        return int(self._redis.llen("queue:%s" % queue))
    
    def watch_queue(self, queue):
        if self._watched_queues.has_key(queue):
            return
        else:
            if self._redis.sadd('queues',str(queue)):
                self._watched_queues[queue] = queue
    def peek(self, queue, start=0, count=1):
        return self.list_range('queue:%s' % queue, start, count)
    
    def list_range(self, key, start, count):
        items = self._redis.lrange(key,start,start+count-1)
        ret_list = []
        for i in items:
            ret_list.append(ResQ.decode(i))
        return ret_list
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
    def enqueue(cls, klass, *args):
        queue = getattr(klass,'queue', None)
        #print cls._res
        resq = cls()
        if queue:
            class_name = '%s.%s' % (klass.__module__, klass.__name__)
            #print class_name
            resq.push(queue, {'klass':class_name,'args':args})
            #Job.create(queue, klass,*args)
    
    @classmethod
    def queues(cls, server="localhost:6379"):
        resq = cls(server)
        return resq._redis.smembers("queues")
    
    #@classmethod
    #def working(cls, server="localhost:6379"):
    #    resq = cls(server)
    #    return Worker.working(resq)
class Stat(object):
    def __init__(self, name, resq):
        self.name = name
        self.key = "stat:%s" % self.name
        self.resq = resq
    
    def get(self):
        val = self.resq._redis.get(self.key)
        return int(val)
    
    def incr(self, ammount=1):
        self.resq._redis.incr(self.key, ammount)
    
    def decr(self, ammount=1):
        self.resq._redis.decr(self.key, ammount)
    
    def clear(self):
        self.resq._redis.delete(self.key)
    
