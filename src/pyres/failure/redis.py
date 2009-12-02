from base import BaseBackend
class RedisBackend(BaseBackend):
    def save(self, resq=None):
        if not resq:
            resq = ResQ()
        data = {
            'failed_at' : str(datetime.datetime.now()),
            'payload'   : self._payload,
            'error'     : self._parse_message(self._exception),
            'backtrace' : self._parse_traceback(self._traceback),
            'queue'     : self._queue
        }
        data = ResQ.encode(data)
        resq.redis.push('failed', data)