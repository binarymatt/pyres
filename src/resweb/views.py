import pystache
from pyres import ResQ
from pyres.worker import Worker
class ResWeb(pystache.View):
    template_path = 'templates'
    def __init__(self, host):
        super(ResWeb, self).__init__()
        self.resq = ResQ(host)
    def media_folder(self):
        return '/media/'
    def address(self):
        return '%s:%s' % (self.resq.redis.host,self.resq.redis.port)
    

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
        from pyres.failure import Failure
        return str(Failure.count(self.resq))
    
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
                item['runat'] = data['run_at']
            else:
                item['data'] = False
            item['nodata'] = not item['data']
            workers.append(item)
        return workers
    def worker_size(self):
        return str(len(self.workers()))
    
    def total_workers(self):
        return str(len(Worker.all(self.resq)))
    
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
        return Worker.all(self.resq)
    
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
                item['runat'] = data['run_at']
            else:
                item['data'] = False
            item['nodata'] = not item['data']
            workers.append(item)
        return workers

class Failed(ResWeb):
    def __init__(self, host, start=0):
        self._start = start
        super(Failed, self).__init__(host)
    
    def failed_jobs(self):
        return ''

class Stats(ResWeb):
    pass