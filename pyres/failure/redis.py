import datetime, time
from base64 import b64encode

from .base import BaseBackend
from pyres import ResQ

class RedisBackend(BaseBackend):
    """Extends the :class:`BaseBackend` to provide a Redis backend for failed jobs."""

    def save(self, resq=None):
        """Saves the failed :class:`Job` in to a "failed" Redis queue,
        preserving all of its original enqueued information.

        :param resq: The redis queue instance to save to
        :type resq: :class:`ResQ`
        """

        if not resq:
            resq = ResQ()
        data = {
            'failed_at' : datetime.datetime.now().strftime('%Y/%m/%d %H:%M:%S'),
            'payload'   : self._payload,
            'exception' : self._exception.__class__.__name__,
            'error'     : self._parse_message(self._exception),
            'backtrace' : self._parse_traceback(self._traceback),
            'queue'     : self._queue
        }
        if self._worker:
            data['worker'] = self._worker
        data = ResQ.encode(data)
        resq.redis.rpush('resque:failed', data)

    @classmethod
    def count(cls, resq):
        """Gets the number of failed items in the queue

        :param resq: The redis queue instance to check
        :type resq: :class:`ResQ`

        :returns: The number of failed items in the queue
        :rtype: int
        """
        return int(resq.redis.llen('resque:failed'))

    @classmethod
    def all(cls, resq, start=0, count=1):
        """Get a list of the items in the failure queue.

        Redis' documentation: `LLEN <http://redis.io/commands/LLEN>`_

        :param resq: The redis queue instance to check
        :type resq: :class:`ResQ`
        :param start: The location in the queue to start checking at.
        :type start: int
        :param count: The number of items to retrieve
        :type count: int

        :returns: A list of items in the queue
        :rtype: `list` of `dict`
        """
        items = resq.redis.lrange('resque:failed', start, count) or []

        ret_list = []
        for i in items:
            failure = ResQ.decode(i)
            failure['redis_value'] = b64encode(i)
            ret_list.append(failure)
        return ret_list

    @classmethod
    def clear(cls, resq):
        """Clears the failure queue.

        Redis' documentation: `DEL <http://redis.io/commands/del>`_

        :param resq: The redis queue instance to clear on
        :type resq: :class:`ResQ`

        :returns: The number of items deleted
        :rtype: int
        """
        return resq.redis.delete('resque:failed')
