import logging
import time
from datetime import timedelta
from pyres import ResQ, safe_str_to_class
from pyres import failure
from pyres.failure.redis import RedisBackend
from pyres.compat import string_types

class Job(object):
    """Every job on the ResQ is an instance of the :class:`Job` class."""

    safe_str_to_class = staticmethod(safe_str_to_class)

    def __init__(self, queue, payload, resq, worker=None):
        """
            :param queue: A string defining the queue to which this `Job` will
                          be added
            :type queue: str
            :param payload: A dictionary containing the name of a class that
                            extends this `Job` and a list of args which will
                            be passed to it's `perform` method.
            :type payload: dict
            :param resq: the :class:`ResQ` that this will be run on
            :type resq: :class:`ResQ`
            :param worker: The name of a specific worker for this Job to be
                           run with.
            :type worker: str
        """
        self._queue = queue
        self._payload = payload
        self.resq = resq
        self._worker = worker

        self.enqueue_timestamp = self._payload.get("enqueue_timestamp")

        # Set the default back end, jobs can override when we import them
        # inside perform().
        failure.backend = RedisBackend

    def __str__(self):
        return "(Job{%s} | %s | %s)" % (
            self._queue, self._payload['class'], repr(self._payload['args']))

    def perform(self):
        """This method converts payload into args and calls the ``perform``
        method on the payload class.

        Before calling ``perform``, a ``before_perform`` class method
        is called, if it exists.  It takes a dictionary as an argument;
        currently the only things stored on the dictionary are the
        args passed into ``perform`` and a timestamp of when the job
        was enqueued.

        Similarly, an ``after_perform`` class method is called after
        ``perform`` is finished.  The metadata dictionary contains the
        same data, plus a timestamp of when the job was performed, a
        ``failed`` boolean value, and if it did fail, a ``retried``
        boolean value.  This method is called after retry, and is
        called regardless of whether an exception is ultimately thrown
        by the perform method.


        """
        payload_class_str = self._payload["class"]
        payload_class = self.safe_str_to_class(payload_class_str)
        payload_class.resq = self.resq
        args = self._payload.get("args")

        metadata = dict(args=args)
        if self.enqueue_timestamp:
            metadata["enqueue_timestamp"] = self.enqueue_timestamp

        before_perform = getattr(payload_class, "before_perform", None)

        metadata["failed"] = False
        metadata["perform_timestamp"] = time.time()
        check_after = True
        try:
            if before_perform:
                payload_class.before_perform(metadata)
            return payload_class.perform(*args)
        except Exception as e:
            metadata["failed"] = True
            metadata["exception"] = e
            if not self.retry(payload_class, args):
                metadata["retried"] = False
                raise
            else:
                metadata["retried"] = True
                logging.exception("Retry scheduled after error in %s", self._payload)
        finally:
            after_perform = getattr(payload_class, "after_perform", None)

            if after_perform:
                payload_class.after_perform(metadata)

            delattr(payload_class,'resq')

    def fail(self, exception):
        """This method provides a way to fail a job and will use whatever
        failure backend you've provided. The default is the
        :class:`RedisBackend`.

        :param exception: The exception that caused the :class:`Job` to fail.
        :type exception: Exception

        :returns: The failure backend instance
        :rtype: An instance of a Failure Backend.
                Defaults to :class:`RedisBackend`
        """
        fail = failure.create(exception, self._queue, self._payload,
                              self._worker)
        fail.save(self.resq)
        return fail

    def retry(self, payload_class, args):
        """This method provides a way to retry a job after a failure.
        If the jobclass defined by the payload containes a ``retry_every``
        attribute then pyres will attempt to retry the job until successful
        or until timeout defined by ``retry_timeout`` on the payload class.

        :param payload_class: the :class:`Job`-like class that needs
                              to be retried
        :type payload_class: :class:`Job`-like

        :param args: The args to be passed to the `payload_class.perform`
                     method when it is retried.
        :type args: list
        """
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

        :param queues: The names of the queues to try and reserve from
        :type queues: str
        :param res: the redis instance to reserve from
        :type res: :class:`ResQ`
        :param worker: The name of worker to perform the job with
        :type worker: str
        :param timeout: How long to block while fetching a job before giving up
        :type timeout: int
        """
        if isinstance(queues, string_types):
            queues = [queues]
        queue, payload = res.pop(queues, timeout=timeout)
        if payload:
            return cls(queue, payload, res, worker)
