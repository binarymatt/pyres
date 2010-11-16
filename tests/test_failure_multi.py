from tests import Basic
from tests.test_failure import FailureTests

from pyres import failure
from pyres.failure.base import BaseBackend
from pyres.failure.multiple import MultipleBackend
from pyres.failure.redis import RedisBackend

# Inner class for the failure backend
class TestBackend(BaseBackend):
    def save(self, resq):
        resq.redis.set('testbackend:called', 1)

failure.backend = MultipleBackend
failure.backend.classes = [RedisBackend, TestBackend]

class BasicMultiBackend(Basic):
    queue = 'basicmultibackend'

class MultipleFailureTests(FailureTests):
    def setUp(self):
        FailureTests.setUp(self)
        self.job_class = BasicMultiBackend
        self.queue_name = 'basicmultibackend'
