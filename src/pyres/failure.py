import datetime
from pyres import ResQ 
import sys, traceback
class Failure(object):
    def __init__(self, exp, queue, payload):
        excc, _, tb = sys.exc_info()
        
        self._exception = excc
        self._traceback = tb
        #self._worker = worker
        self._queue = queue
        self._payload = payload
    
    def _parse_traceback(self, trace):
        """Return the given traceback string formatted for a notification."""
        p_traceback = [ "%s:%d:in `%s'" % (filename, lineno, funcname) 
                        for filename, lineno, funcname, _
                        in traceback.extract_tb(trace) ]
        p_traceback.reverse()

        return p_traceback
    
    def _parse_message(self, exc):
        """Return a message for a notification from the given exception."""
        return '%s: %s' % (exc.__class__.__name__, str(exc))
    
    def save(self, resq):
        data = {
            'failed_at' : str(datetime.datetime.now()),
            'payload'   : self._payload,
            'error'     : self._parse_message(self._exception),
            'backtrace' : self._parse_traceback(self._traceback),
            'queue'     : self._queue
        }
        data = ResQ.encode(data)
        resq.redis.push('failed', data)
    
    @classmethod
    def count(cls, resq):
        return int(resq.redis.llen('failed'))
    
    
