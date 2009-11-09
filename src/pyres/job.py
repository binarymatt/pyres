from pyres import ResQ, str_to_class
from pyres.failure import Failure
class Job(object):
    def __init__(self, queue, payload, resq):
        self._queue = queue
        self._payload = payload
        self.resq = resq
    
    def perform(self):
        payload_class_str = self._payload["klass"]
        payload_class = str_to_class(payload_class_str)
        args = self._payload.get("args", None)
        if args:
            return payload_class.perform(*args)
        else:
            return payload_class.perform()
    
    def fail(self, exception):
        #Failure.create(exception)
        failure = Failure(exception, self._queue, self._payload)
        failure.save(self.resq)
    
    @classmethod
    def reserve(cls, queue, res):
        payload = res.pop(queue)
        print "payload: %s" % payload
        if payload:
            return cls(queue, payload, res)
