from base import BaseBackend
from redis import RedisBackend

class MultipleBackend(BaseBackend):
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
