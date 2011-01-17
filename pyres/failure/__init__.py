from pyres.failure.redis import RedisBackend

backend = RedisBackend

def create(*args, **kwargs):
    return backend(*args, **kwargs)

def count(resq):
    return backend.count(resq)

def all(resq, start, count):
    return backend.all(resq, start, count)

def clear(resq):
    return backend.clear(resq)

def requeue(resq, failure_object):
    queue = failure_object._queue
    payload = failure_object._payload
    return resq.push(queue, payload)

def retry(resq, queue, payload):
    job = resq.decode(payload)
    resq.push(queue, job['payload'])
    return delete(resq, payload)

def delete(resq, payload):
    return resq.redis.lrem(name='resque:failed', num=1, value=payload)
