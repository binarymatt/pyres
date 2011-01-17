from base import BaseBackend
from redis import RedisBackend

class MultipleBackend(BaseBackend):
    """Extends ``BaseBackend`` to provide support for delegating calls to multiple
    backends. Queries are delegated to the first backend in the list. Defaults to
    only the RedisBackend.

    To use:

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
        if not self.classes:
            self.classes = [RedisBackend]

        self.backends = [klass(*args) for klass in self.classes]
        BaseBackend.__init__(self, *args)

    @classmethod
    def count(cls, resq):
        first = MultipleBackend.classes[0]
        return first.count(resq)

    @classmethod
    def all(cls, resq, start=0, count=1):
        first = MultipleBackend.classes[0]
        return first.all(resq, start, count)

    @classmethod
    def clear(cls, resq):
        first = MultipleBackend.classes[0]
        return first.clear(resq)

    def save(self, resq=None):
        map(lambda x: x.save(resq), self.backends)
