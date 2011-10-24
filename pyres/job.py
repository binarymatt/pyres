from datetime import timedelta
from pyres import ResQ, safe_str_to_class
from pyres import failure
from pyres.failure.redis import RedisBackend


class Job(object):
    """Every job on the ResQ is an instance of the *Job* class.

    The ``__init__`` takes these keyword arguments:

        ``queue`` -- A string defining the queue to which this Job will be
                     added.

        ``payload`` -- A dictionary which contains the string name of a class
                       which extends this Job and a list of args which will be
                       passed to that class.

        ``resq`` -- An instance of the ResQ class.

        ``worker`` -- The name of a specific worker if you'd like this Job to be
                      done by that worker. Default is "None".

    """

    safe_str_to_class = staticmethod(safe_str_to_class)
    
    def __init__(self, queue, payload, resq, worker=None):
        self._queue = queue
        self._payload = payload
        self.resq = resq
        self._worker = worker

        # Set the default back end, jobs can override when we import them
        # inside perform().
        failure.backend = RedisBackend

    def __str__(self):
        return "(Job{%s} | %s | %s)" % (
            self._queue, self._payload['class'], repr(self._payload['args']))

    def perform(self):
        """This method converts payload into args and calls the ``perform``
        method on the payload class.

        #@ add entry_point loading

        """
        payload_class_str = self._payload["class"]
        payload_class = self.safe_str_to_class(payload_class_str)
        payload_class.resq = self.resq
        args = self._payload.get("args")

        try:
            return payload_class.perform(*args)
        except:
            if not self.retry(payload_class, args):
                raise

    def fail(self, exception):
        """This method provides a way to fail a job and will use whatever
        failure backend you've provided. The default is the ``RedisBackend``.

        """
        fail = failure.create(exception, self._queue, self._payload,
                              self._worker)
        fail.save(self.resq)
        return fail

    def retry(self, payload_class, args):
        retry_every = getattr(payload_class, 'retry_every', None)
        retry_timeout = getattr(payload_class, 'retry_timeout', 0)

        if retry_every:
            now = ResQ._current_time()
            first_attempt = self._payload.get("first_attempt", now)
            retry_until = first_attempt + timedelta(seconds=retry_timeout)
            retry_at = now + timedelta(seconds=retry_every)
            if retry_at < retry_until:
                self.resq.enqueue_at(retry_at, payload_class, *args,
                        **{'first_attempt':first_attempt})
                return True
        return False

    @classmethod
    def reserve(cls, queues, res, worker=None, timeout=10):
        """Reserve a job on one of the queues. This marks this job so
        that other workers will not pick it up.

        """
        if isinstance(queues, basestring):
            queues = [queues]
        queue, payload = res.pop(queues, timeout=timeout)
        if payload:
            return cls(queue, payload, res, worker)
