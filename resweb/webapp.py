try:
    import json
except ImportError:
    import simplejson as json

from base64 import b64decode
from flask import Flask, g, request, redirect
from pyres import ResQ, failure
from flaskext.redis import Redis

app = Flask(__name__)
db = Redis(app)
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

@app.route("/")
def index():
    return Overview(ResQ(server=g.redis)).render()

@app.route("/working/")
def working():
    return Working( ResQ(server=g.redis)).render()

@app.route("/queues/")
def queues():
    return Queues( ResQ(server=g.redis)).render()

@app.route('/queues/<queue_id>/')
def queue(queue_id):
    start = int(request.args.get('start',0))
    return Queue( ResQ(server=g.redis), queue_id, start).render()

@app.route('/failed/')
def failed():
    start = request.args.get('start',0)
    start = int(start)
    return str(Failed( ResQ(server=g.redis), start).render())

@app.route('/failed/retry/', methods=['POST'])
def failed_retry():
    failed_job = request.form['failed_job']
    job = b64decode(failed_job)
    decoded = ResQ.decode(job)
    failure.retry( ResQ(server=g.redis), decoded['queue'], job)
    return redirect('/failed/')

@app.route('/failed/delete/', methods=['POST'])
def failed_delete():
    failed_job = request.form['failed_job']
    job = b64decode(failed_job)
    failure.delete( ResQ(server=g.redis), job)
    return redirect('/failed/')

@app.route('/failed/delete_all/')
def delete_all_failed():
     #move resque:failed to resque:failed-staging
     g.redis.rename('resque:failed','resque:failed-staging')
     g.redis.delete('resque:failed-staging')
     return redirect('/failed/')


@app.route('/failed/retry_all')
def retry_failed(number=5000):
    failures = failure.all(ResQ(server=g.redis), 0, number)
    for f in failures:
        j = b64decode(f['redis_value'])
        failure.retry(ResQ(server=g.redis), f['queue'], j)
    return redirect('/failed/')

@app.route('/workers/(?P<worker_id>\w.+)/')
def worker(worker_id):
    return str(Worker(ResQ(server=g.redis), worker_id).render())

@app.route('/workers/')
def workers():
    return str(Workers(ResQ(server=g.redis)).render())



@app.route('/stats/<key>/')
def stats(key):
    return str(Stats(ResQ(server=g.redis), key).render())

@app.route('/stats/')
def stats_r():
    return redirect('/stats/resque/')

@app.route('/stat/(?P<stat_id>\w.+)')
def stat(stat_id):
    return str(Stat(ResQ(server=g.redis), stat_id).render())

@app.route('/delayed/')
def delayed():
    start = request.args.get('start',0)
    start = int(start)
    return str(Delayed(ResQ(server=g.redis), start).render())

@app.route('/delayed/<timestamp>')
def delayed_timestamp(timestamp):
    start = request.args.get('start',0)
    start = int(start)
    return str(DelayedTimestamp(ResQ(server=g.redis), timestamp, start).render())

if __name__ == "__main__":
    app.run(debug=True)
