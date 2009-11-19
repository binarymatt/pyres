# Create your views here.
from django.conf import settings

from resweb.decorators import render_to
from pyres import ResQ
from pyres.failure import Failure
from pyres.worker import Worker
class Queue(object):
    def __init__(self, name, count):
        self.name = name
        self.count = count
    

@render_to('overview.html')
def index(request):
    queue_list = []
    for q in ResQ.queues(settings.RESQ_HOST):
        q_item = Queue(q,ResQ(settings.RESQ_HOST).size(q))
        queue_list.append(q_item)
    failure_count = Failure.count(ResQ(settings.RESQ_HOST))
    return locals()

@render_to('working.html')
def working(request):
    workers = []
    for worker in Worker.working(request.resq._server):
        host, pid, queues = str(worker).split(':')
        worker._host
        worker._pid
        workers.append(worker)
    current_count = len(workers)
    total_workers = len(Worker.all(settings.RESQ_HOST))
    return locals()

def failed(request):
    return {}

@render_to('queues.html')
def queues(request):
    queue_list = []
    for q in ResQ.queues(settings.RESQ_HOST):
        q_item = Queue(q,ResQ(settings.RESQ_HOST).size(q))
        queue_list.append(q_item)
    failure_count = Failure.count(ResQ(settings.RESQ_HOST))
    return locals()

@render_to('queue_detail.html')
def queue_detail(request, queue_id):
    queue = Queue(queue_id,request.resq.size(queue_id))
    waiting = request.resq.peek(queue_id,0,20)
    return locals()

def workers(request):
    return {}

def stats(request):
    return {}