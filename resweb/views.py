import pystache

from pyres import __version__
from pyres.worker import Worker as Wrkr
from pyres import failure
import os
import datetime

TEMPLATE_PATH = os.path.join(os.path.dirname(__file__), 'templates')
class ResWeb(pystache.View):
    template_path = TEMPLATE_PATH
    def __init__(self, host):
        super(ResWeb, self).__init__()
        self.resq = host
    
    def media_folder(self):
        return '/media/'
    
    def close(self):
        self.resq.close()
    
    def address(self):
        return '%s:%s' % (self.resq.redis.host,self.resq.redis.port)
    
    def version(self):
        return str(__version__)
    
    def pages(self, start, size, link_function, width=20):
        pages = []

        num_pages = size / width
        if size % width > 0:
            num_pages += 1

        if size < width:
            return pages

        for i in range(num_pages):
            current = True
            if start == i*width:
                current = False
            link = link_function(i*width)
            link_name = str(i+1)
            pages.append(dict(link=link,link_name=link_name,current=current))
        return pages

class Overview(ResWeb):
    def __init__(self, host, queue=None, start=0):
        self._queue = queue
        self._start = start
        super(Overview, self).__init__(host)
    
    def queue(self):
        return self._queue
    
    def queues(self):
        queues = []
        for q in self.resq.queues():
            queues.append({
                'queue': q,
                'size': str(self.resq.size(q)),
            })
        return queues
    
    def start(self):
        return str(self._start)
    
    def end(self):
        return str(self._start + 20)
    
    def size(self):
        return str(self.resq.size(self._queue))
    
    def jobs(self):
        jobs = []
        for job in self.resq.peek(self._queue, self._start, self._start+20):
            jobs.append({
                'class':job['class'],
                'args':','.join(job['args'])
            })
        return jobs
    
    def empty_jobs(self):
        return len(self.jobs()) == 0
    
    def empty(self):
        return not self._queue
    
    def fail_count(self):
        #from pyres.failure import Failure
        return str(failure.count(self.resq))
    
    def workers(self):
        workers = []
        for w in self.resq.working():
            data = w.processing()
            host,pid,queues = str(w).split(':')
            item = {
                'state':w.state(),
                'host': host,
                'pid':pid,
                'w':str(w)
            }
            item['queue'] = w.job()['queue']
            if data.has_key('queue'):
                item['data'] = True
                item['code'] = data['payload']['class']
                item['runat'] = str(datetime.datetime.fromtimestamp(float(data['run_at'])))
            else:
                item['data'] = False
            item['nodata'] = not item['data']
            workers.append(item)
        return workers
    def worker_size(self):
        return str(len(self.workers()))
    
    def total_workers(self):
        return str(len(Wrkr.all(self.resq)))
    
    def empty_workers(self):
        if len(self.workers()):
            return False
        else:
            return True
class Queues(Overview):
    template_name = 'queue_full'

class Working(Overview):
    template_name = 'working_full'
    
class Workers(ResWeb):
    def size(self):
        return str(len(self.all()))
    
    def all(self):
        return Wrkr.all(self.resq)
    
    def workers(self):
        workers = []
        for w in self.all():
            data = w.processing()
            host,pid,queues = str(w).split(':')
            item = {
                'state':w.state(),
                'host': host,
                'pid':pid,
                'w':str(w)
            }
            qs = []
            for q in queues.split(','):
                qs.append({
                    'q':str(q)
                })
            item['queues'] = qs
            if data.has_key('queue'):
                item['data'] = True
                item['code'] = data['payload']['class']
                item['runat'] = str(datetime.datetime.fromtimestamp(float(data['run_at'])))
            else:
                item['data'] = False
            item['nodata'] = not item['data']
            workers.append(item)
        return workers

class Queue(ResWeb):
    def __init__(self, host, key, start=0):
        self.key = key
        self._start = start
        super(Queue, self).__init__(host)
    
    def start(self):
        return str(self._start)

    def end(self):
        end = self._start + 20
        if end > int(self.size()):
            end = self.size()
        return str(end)
    
    def queue(self):
        return self.key
    
    def size(self):
        return str(self.resq.size(self.key) or 0)
    
    def jobs(self):
        jobs = []
        for job in self.resq.peek(self.key, self._start, self._start+20):
            jobs.append({
                'class':job['class'],
                'args': str(job['args'])
            })
        return jobs
    
    def pagination(self):
        return self.pages(self._start, int(self.size()), self.link_func)
    
    def link_func(self, start):
        return '/queues/%s/?start=%s' % (self.key, start)
    

class Failed(ResWeb):
    def __init__(self, host, start=0):
        self._start = start
        self.host = host
        super(Failed, self).__init__(host)
    
    def start(self):
        return str(self._start)
    
    def end(self):
        return str(self._start + 20)
    
    def size(self):
        return str(failure.count(self.resq) or 0)
     
    def failed_jobs(self):
        from base64 import b64encode
        try:
            import json
        except ImportError:
            import simplejson as json
        jobs = []
        for job in failure.all(self.resq, self._start, self._start + 20):
            item = job
            item['failed_at'] = str(datetime.datetime.fromtimestamp(float(job['failed_at'])))
            item['worker_url'] = '/workers/%s/' % job['worker']
            item['payload_args'] = str(job['payload']['args'])
            item['payload_class'] = job['payload']['class']
            item['traceback'] = job['backtrace']
            jobs.append(item)
        return jobs
    
    def pagination(self):
        return self.pages(self._start, int(self.size()), self.link_func)
    
    def link_func(self, start):
        return '/failed/?start=%s' % start
        
class Stats(ResWeb):
    def __init__(self, host, key_id):
        self.key_id = key_id
        super(Stats, self).__init__(host)
    
    def sub_nav(self):
        sub_nav = []
        sub_nav.append({
            'section':'stats',
            'subtab':'resque'
        })
        sub_nav.append({
            'section':'stats',
            'subtab':'redis'
        })
        sub_nav.append({
            'section':'stats',
            'subtab':'keys'
        })
        return sub_nav
    
    def title(self):
        if self.key_id == 'resque':
            return 'Pyres'
        elif self.key_id == 'redis':
            return '%s:%s' % (self.resq.redis.host,self.resq.redis.port)
        elif self.key_id == 'keys':
            return 'Keys owned by Pyres'
        else:
            return ''
    
    def stats(self):
        if self.key_id == 'resque':
            return self.resque_info()
        elif self.key_id == 'redis':
            return self.redis_info()
        elif self.key_id == 'keys':
            return self.key_info()
        else:
            return []
    
    def resque_info(self):
        stats = []
        for key, value in self.resq.info().items():
            stats.append({
                'key':str(key),
                'value': str(value)
            })
        return stats
    
    def redis_info(self):
        stats = []
        for key, value in self.resq.redis.info().items():
            stats.append({
                'key':str(key),
                'value': str(value)
            })
        return stats
    def key_info(self):
        stats = []
        for key in self.resq.keys():
            
            stats.append({
                'key': str(key),
                'type': str(self.resq.redis.type('resque:'+key)),
                'size': str(redis_size(key, self.resq)) 
            })
        return stats
    def standard(self):
        return not self.resque_keys()
    
    def resque_keys(self):
        if self.key_id == 'keys':
            return True
        return False

class Stat(ResWeb):
    def __init__(self, host, stat_id):
        self.stat_id = stat_id
        super(Stat, self).__init__(host)
    
    def key(self):
        return str(self.stat_id)
    
    def key_type(self):
        return str(self.resq.redis.type('resque:'+ str(self.stat_id)))
    
    def items(self):
        items = []
        if self.key_type() == 'list':
            lst = self.resq.redis.lrange('resque:'+self.stat_id,0,20) or []
            for k in lst:
                items.append({
                    'row':str(k)
                })
        elif self.key_type() == 'set':
            st = self.resq.redis.smembers('resque:'+self.stat_id) or set([])
            for k in st:
                items.append({
                    'row':str(k)
                })
        elif self.key_type() == 'string':
            items.append({
                'row':str(self.resq.redis.get('resque:'+self.stat_id))
            })
        return items
    
    def size(self):
        return redis_size(self.stat_id,self.resq)
    
class Worker(ResWeb):
    def __init__(self, host, worker_id):
        self.worker_id = worker_id
        super(Worker, self).__init__(host)
        self._worker = Wrkr.find(worker_id, self.resq)
    
    def worker(self):
        return str(self.worker_id)
    
    def host(self):
        host,pid,queues = str(self.worker_id).split(':')
        return str(host)
    def pid(self):
        host,pid,queues = str(self.worker_id).split(':')
        return str(pid)
    
    def state(self):
        return str(self._worker.state())
    
    def started_at(self):
        return str(self._worker.started)
    
    def queues(self):
        host,pid,queues = str(self.worker_id).split(':')
        qs = []
        for q in queues.split(','):
            qs.append({
                'q':str(q)
            })
        return qs
    def processed(self):
        return str(self._worker.get_processed())
    
    def failed(self):
        return str(self._worker.get_failed())
    def data(self):
        data = self._worker.processing()
        return data.has_key('queue')
    def nodata(self):
        return not self.data()
    def code(self):
        data = self._worker.processing()
        if self.data():
            return str(data['payload']['class'])
        return ''
    def runat(self):
        data = self._worker.processing()
        if self.data():
            return str(datetime.datetime.fromtimestamp(float(data['run_at'])))
        return ''
    
        """
        item = {
            'state':w.state(),
            'host': host,
            'pid':pid,
            'w':str(w)
        }
        qs = []
        for q in queues.split(','):
            qs.append({
                'q':str(q)
            })
        item['queues'] = qs
        if data.has_key('queue'):
            item['data'] = True
            item['code'] = data['payload']['class']
            item['runat'] = data['run_at']
        else:
            item['data'] = False
        item['nodata'] = not item['data']
        """
        pass

class Delayed(ResWeb):
    def __init__(self, host, start=0):
        self._start = start
        super(Delayed, self).__init__(host)

    def start(self):
        return str(self._start)

    def end(self):
        return str(self._start + 20)

    def size(self):
        item = self.resq.delayed_queue_schedule_size() or 0
        return str(item)

    def jobs(self):
        jobs = []
        for timestamp in self.resq.delayed_queue_peek(self.start(), self.end()):
            t = datetime.datetime.fromtimestamp(float(timestamp))
            item = dict(timestamp=str(timestamp))
            item['size'] = str(self.resq.delayed_timestamp_size(timestamp))
            
            item['formated_time'] = str(t)
            
            jobs.append(item)
        return jobs

    def pagination(self):
        return self.pages(self._start, int(self.size()), self.link_func)

    def link_func(self, start):
        return '/delayed/?start=%s' % start

class DelayedTimestamp(ResWeb):
    def __init__(self, host, timestamp, start=0):
        self._start = start
        self._timestamp = timestamp
        super(DelayedTimestamp, self).__init__(host)
    
    def formated_timestamp(self):
        return str(datetime.datetime.fromtimestamp(float(self._timestamp)))
    
    def start(self):
        return str(self._start)

    def end(self):
        return str(self._start + 20)

    def size(self):
        item = self.resq.delayed_timestamp_size(self._timestamp) or 0
        return str(item)

    def jobs(self):
        jobs = []
        for job in self.resq.delayed_timestamp_peek(self._timestamp, int(self.start()), int(self.end())):
            item = {
                'class': str(job['class']),
                'args': str(job['args'])
            }
            jobs.append(item)
        return jobs
    
    def no_jobs(self):
        if int(self.size()) > 0:
            return False
        return True
    
    def pagination(self):
        return self.pages(self._start, int(self.size()), self.link_func)

    def link_func(self, start):
        return '/delayed/?start=%s' % start

def redis_size(key, resq):
    key_type = resq.redis.type('resque:'+key)
    item = 0
    if key_type == 'list':
        item = resq.redis.llen('resque:'+key)
    elif key_type == 'set':
        item = resq.redis.scard('resque:'+key)
    elif key_type == 'string':
        item = 1
    return str(item)
