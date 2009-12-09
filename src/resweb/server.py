from jinja2 import Environment, PackageLoader
env = Environment(loader=PackageLoader('resweb', 'templates'))
from itty import *
from pyres import ResQ
from pyres.failure import Failure
from pyres.worker import Worker
HOST = "localhost:6379"
resq = ResQ(HOST)

@get("/")
def index(request):
    #queues = ResQ.queues(HOST)
    #failure_count = Failure.count(ResQ(HOST))
    rq = ResQ(HOST)
    queues = rq.queues()
    failure_count = Failure.count(rq)
    template = env.get_template('overview.html')
    dic = {
        'queues':queues,
        'failure_count':failure_count,
        'resq': resq
    }
    return str(template.render(dic))



@get("/working/")
def working(request):
    workers = Worker.working(resq._server)
    template = env.get_template('working.html')
    dic = {
        'all_workers':Worker.all(HOST),
        'workers':workers,
        'resq': resq
    }
    return str(template.render(dic))

@get("/queues/")
def queues(request):    
    #queues = ResQ.queues(HOST)
    #failure_count = Failure.count(ResQ(HOST))
    rq = ResQ(HOST)
    queues = rq.queues()
    failure_count = Failure.count(rq)
    template = env.get_template('queues.html')
    dic = {
        'queues':queues,
        'failure_count':failure_count,
        'resq': resq
    }
    return str(template.render(dic))

@get('/queues/(?P<queue_id>\w+)/')
def queue(request, queue_id):
    context = {}
    context['queue'] = queue_id
    context['resq'] = resq
    template = env.get_template('queue_detail.html')
    return str(template.render(context))

@get('/failed/')
def failed(request):
    context = {
        'resq': resq
    }
    template = env.get_template('failed.html')
    return str(template.render(context))

@get('/workers/')
def workers(request):
    context = {
        'workers': Worker.all(HOST)
    }
    template = env.get_template('workers.html')
    return str(template.render(context))

@get('/stats/')
def stats(request):
    template = env.get_template('stats.html')
    return str(template.render({}))

run_itty(host="0.0.0.0", port=8080)
