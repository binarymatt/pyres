from pyres import ResQ, str_to_class, safe_str_to_class
from pyres import failure
class Job(object):
    """
    Every job on the ResQ is a *Job* object which has queue and payload(all the
    args data and when its created etc).
    """
    def __init__(self, queue, payload, resq, worker=None):
        self._queue = queue
        self._payload = payload
        self.resq = resq
        self._worker = worker
    
    def perform(self):
        """This method converts payload into args and calls the **perform** method
        on the payload class.
        """
        payload_class_str = self._payload["class"]
        payload_class = safe_str_to_class(payload_class_str)
        payload_class.resq = self.resq
        args = self._payload.get("args", None)
        if args:
            return payload_class.perform(*args)
        else:
            return payload_class.perform()
    
    def fail(self, exception):
        #Failure.create(exception)
        fail = failure.create(exception, self._queue, self._payload, self._worker)
        fail.save(self.resq)
        return fail
    
    @classmethod
    def reserve(cls, queue, res, worker=None):
        """Reserve a job on the queue. In simple marking this job so that other worker
        will not pick it up"""
        payload = res.pop(queue)
        if payload:
            return cls(queue, payload, res, worker)
