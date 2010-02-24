from pyres import ResQ, str_to_class, safe_str_to_class
from pyres import failure

class Job(object):
    """Every job on the ResQ is an instance of the *Job* class.
    
    The ``__init__`` takes these keyword arguments:
    
        ``queue`` -- A string defining the queue to which this Job will be added.
    
        ``payload`` -- A dictionary which contains the string name of a class which extends this Job and
        a list of args which will be passed to that class.
        
        ``resq`` -- An instance of the ResQ class.
    
        ``worker`` -- The name of a specific worker if you'd like this Job to be done by that worker. Default is "None".
    
    """
    def __init__(self, queue, payload, resq, worker=None):
        self._queue = queue
        self._payload = payload
        self.resq = resq
        self._worker = worker
    
    def __str__(self):
        return "(Job{%s} | %s | %s)" % (
            self._queue, self._payload['class'], repr(self._payload['args']))
    
    def perform(self):
        """This method converts payload into args and calls the ``perform`` method
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
        """This method provides a way to fail a job and will use whatever failure backend
        you've provided. The default is the ``RedisBackend``.
        
        """
        fail = failure.create(exception, self._queue, self._payload, self._worker)
        fail.save(self.resq)
        return fail
    
    @classmethod
    def reserve(cls, queue, res, worker=None):
        """Reserve a job on the queue. This marks this job so that other workers
        will not pick it up.
        
        """
        payload = res.pop(queue)
        if payload:
            return cls(queue, payload, res, worker)
