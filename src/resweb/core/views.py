# Create your views here.
from django.conf import settings

from resweb.decorators import render_to
from pyres import ResQ
from pyres.failure import Failure
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
    current = request.resq.working()
    total_workers = len(request.resq.workers())
    return {}

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