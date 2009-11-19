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
    queues = ResQ.queues(HOST)
    failure_count = Failure.count(ResQ(HOST))
    template = env.get_template('overview.html')
    dic = {
        'queues':queues,
        'failure_count':failure_count,
        'resq': resq
    }
    return str(template.render(dic))



@get("/working/")
def working(request):
    workers = Worker.working(request.resq._server)
    template = env.get_template('working.html')
    dic = {
        'all_workers':Worker.all(settings.RESQ_HOST),
        'workers':workers,
        'resq': resq
    }
    return str(template.render(dic))

@get("/queues/")
def queues(request):
    return ""

@get('/queues/(?P<queue_id>\w+)/')
def queue(request, queue_id):
    context = {}
    context['queue'] = queue_id
    context['resq'] = resq
    template = env.get_template('queue_detail.html')
    return str(template.render(context))

@get(r'/failed/$')
def failed(request):
    return ""

@get(r'/workers/$')
def workers(request):
    return ""

@get(r'/stats/$')
def stats(request):
    return ""

run_itty()