from pyres.failure.base import BaseBackend
from pyres.failure.redis import RedisBackend

class MultipleBackend(BaseBackend):
    """Extends :class:`BaseBackend` to provide support for delegating calls
    to multiple backends.

    .. note:: Queries are delegated to the first backend in the list

    .. note:: Defaults to only the RedisBackend

    To use::

        from pyres import failure

        from pyres.failure.base import BaseBackend
        from pyres.failure.multiple import MultipleBackend
        from pyres.failure.redis import RedisBackend

        class CustomBackend(BaseBackend):
            def save(self, resq):
                print('Custom backend')

        failure.backend = MultipleBackend
        failure.backend.classes = [RedisBackend, CustomBackend]
    """
    classes = []

    def __init__(self, *args):
        """Sets up the class to use a :class:`RedisBackend` by default
        """

        if not self.classes:
            self.classes = [RedisBackend]

        self.backends = [klass(*args) for klass in self.classes]
        BaseBackend.__init__(self, *args)

    @classmethod
    def count(cls, resq):
        """ Returns the result of a `count` call to the first backend

        :param resq: The redis queue to count on
        :type resq: :class:`ResQ`

        :returns: The number of items in the backend
        :rtype: int
        """
        first = MultipleBackend.classes[0]
        return first.count(resq)

    @classmethod
    def all(cls, resq, start=0, count=1):
        """ Returns the result of an `all` call to the first backend

        :param resq: The redis queue to count on
        :type resq: :class:`ResQ`
        :param start: The location to start fetching items
        :type start: int
        :param count: The number of items to fetch
        :type count:

        :returns: A list of items from the backend
        :rtype: `list` of `dict`
        """
        first = MultipleBackend.classes[0]
        return first.all(resq, start, count)

    @classmethod
    def clear(cls, resq):
        """ Returns the result of a `clear` call to the first backend

        :param resq: The redis queue to clear on
        :type resq: :class:`ResQ`

        :returns: The number of items cleared from the backend
        :rtype: int
        """
        first = MultipleBackend.classes[0]
        return first.clear(resq)

    def save(self, resq=None):
        """ Calls save on all of the backends

        :param resq: The redis queue to save to
        :type resq: :class:`ResQ`

        """
        map(lambda x: x.save(resq), self.backends)
