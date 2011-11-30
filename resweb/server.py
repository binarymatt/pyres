import os
from itty import Redirect, get, post, serve_static_file, run_itty, handle_request
from pyres import ResQ
from pyres import failure
from views import (
    Overview,
    Queues,
    Queue,
    Workers,
    Working,
    Failed,
    Stats,
    Stat,
    Worker,
    Delayed,
    DelayedTimestamp
)
from base64 import b64decode

HOST = ResQ("localhost:6379")
MY_ROOT = os.path.join(os.path.dirname(__file__), 'media')
#resq = ResQ(HOST)

@get("/")
def index(request):
    return Overview(HOST).render().encode('utf-8')

@get("/working/")
def working(request):
    return Working(HOST).render().encode('utf-8')

@get("/queues/")
def queues(request):
    return Queues(HOST).render().encode('utf-8')

@get('/queues/(?P<queue_id>\w.*)/')
def queue(request, queue_id):
    start = int(request.GET.get('start',0))
    return Queue(HOST, queue_id, start).render().encode('utf-8')

@get('/failed/')
def failed(request):
    start = request.GET.get('start',0)
    start = int(start)
    return Failed(HOST, start).render().encode('utf-8')

@post('/failed/retry/')
def failed_retry(request):
    failed_job = request.POST['failed_job']
    job = b64decode(failed_job)
    decoded = ResQ.decode(job)
    failure.retry(HOST, decoded['queue'], job)
    raise Redirect('/failed/')

@post('/failed/delete/')
def failed_delete(request):
    failed_job = request.POST['failed_job']
    job = b64decode(failed_job)
    failure.delete(HOST, job)
    raise Redirect('/failed/')

@get('/failed/delete_all/')
def delete_all_failed(request):
     #move resque:failed to resque:failed-staging
     HOST.redis.rename('resque:failed','resque:failed-staging')
     HOST.redis.delete('resque:failed-staging')
     raise Redirect('/failed/')


@get('/failed/retry_all')
def retry_failed(request, number=5000):
    failures = failure.all(HOST, 0, number)
    for f in failures:
        j = b64decode(f['redis_value'])
        failure.retry(HOST, f['queue'], j)
    raise Redirect('/failed/')

@get('/workers/(?P<worker_id>\w.+)/')
def worker(request, worker_id):
    return Worker(HOST, worker_id).render().encode('utf-8')

@get('/workers/')
def workers(request):
    return Workers(HOST).render().encode('utf-8')

@get('/stats/')
def stats(request):
    raise Redirect('/stats/resque/')

@get('/stats/(?P<key>\w+)/')
def stats(request, key):
    return Stats(HOST, key).render().encode('utf-8')

@get('/stat/(?P<stat_id>\w.+)')
def stat(request, stat_id):
    return Stat(HOST, stat_id).render().encode('utf-8')

@get('/delayed/')
def delayed(request):
    start = request.GET.get('start',0)
    start = int(start)
    return Delayed(HOST, start).render().encode('utf-8')

@get('/delayed/(?P<timestamp>\w.+)')
def delayed_timestamp(request, timestamp):
    start = request.GET.get('start',0)
    start = int(start)
    return DelayedTimestamp(HOST, timestamp, start).render().encode('utf-8')

@get('/media/(?P<filename>.+)')
def my_media(request, filename):
    #return serve_static_file(request, filename)
    #my_media.content_type = content_type(filename)

    return serve_static_file(request, filename, root=MY_ROOT)
    #output = static_file(filename, root=MY_ROOT)
    #return Response(output, content_type=content_type(filename))
    #return static_file(request, filename=filename, root=my_root)


# The hook to make it run in a mod_wsgi environment.
def application(environ, start_response):
    return handle_request(environ, start_response)

if __name__ == "__main__":
    run_itty()
