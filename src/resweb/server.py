from itty import *
from pyres import ResQ
from pyres.failure import Failure
from pyres.worker import Worker
from views import Overview, Queues, Workers, Working, Failed, Stats
HOST = "localhost:6379"
#resq = ResQ(HOST)

@get("/")
def index(request):
    return str(Overview(HOST).render())

@get("/working/")
def working(request):
    return str(Working(HOST).render())

@get("/queues/")
def queues(request):
    return str(Queues(HOST).render())

@get('/queue/(?P<queue_id>\w+)/')
def queue(request, queue_id):
    return str(Queues(HOST, queue_id).render())

@get('/failed/')
def failed(request):
    return str(Failed(HOST).render())

@get('/workers/(?P<worker_id>\w+)/')
def worker(request, worker_id):
    return str(Worker(worker_id).render())

@get('/workers/')
def workers(request):
    return str(Workers(HOST).render())

@get('/stats/')
def stats(request):
    return str(Stats(HOST).render())

@get('/media/(?P<filename>.+)')
def my_media(request, filename):
    print filename
    #my_media.content_type = content_type(filename)
    my_root = os.path.join(os.path.dirname(__file__), 'media')
    output = static_file(filename, root=my_root)
    return Response(output, content_type=content_type(filename))
    #return static_file(request, filename=filename, root=my_root)

run_itty()