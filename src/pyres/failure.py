import datetime
from pyres import ResQ
class Failure(object):
    def __init__(self, exp, queue, payload):
        self._exception = exp
        #self._worker = worker
        self._queue = queue
        self._payload = payload
    
    def save(self, resq):
        data = {
            'failed_at' : str(datetime.datetime.now()),
            'payload'   : self._payload,
            'error'     : self._exception,
            'queue'     : self._queue
        }
        data = ResQ.encode(data)
        resq.redis.push('failed', data)
    
    @classmethod
    def count(cls, resq):
        return int(resq.redis.llen('failed'))
    
    @classmethod
    def create(cls, options={}):
        pass
    
