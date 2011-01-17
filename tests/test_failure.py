from tests import PyResTests, Basic
from pyres import failure
from pyres.job import Job

class FailureTests(PyResTests):
    def setUp(self):
        PyResTests.setUp(self)
        self.queue_name = 'basic'
        self.job_class = Basic

    def test_count(self):
        self.resq.enqueue(self.job_class,"test1")
        job = Job.reserve(self.queue_name,self.resq)
        job.fail("problem")
        assert failure.count(self.resq) == 1
        assert self.redis.llen('resque:failed') == 1

    def test_create(self):
        self.resq.enqueue(self.job_class,"test1")
        job = Job.reserve(self.queue_name,self.resq)
        e = Exception('test')
        fail = failure.create(e, self.queue_name, job._payload)
        assert isinstance(fail._payload, dict)
        fail.save(self.resq)
        assert failure.count(self.resq) == 1
        assert self.redis.llen('resque:failed') == 1

    def test_all(self):
        self.resq.enqueue(self.job_class,"test1")
        job = Job.reserve(self.queue_name,self.resq)
        e = Exception('problem')
        job.fail(e)
        assert len(failure.all(self.resq, 0, 20)) == 1

    def test_clear(self):
        self.resq.enqueue(self.job_class,"test1")
        job = Job.reserve(self.queue_name,self.resq)
        e = Exception('problem')
        job.fail(e)
        assert self.redis.llen('resque:failed') == 1
        failure.clear(self.resq)
        assert self.redis.llen('resque:failed') == 0

    def test_requeue(self):
        self.resq.enqueue(self.job_class,"test1")
        job = Job.reserve(self.queue_name,self.resq)
        e = Exception('problem')
        fail_object = job.fail(e)
        assert self.resq.size(self.queue_name) == 0
        failure.requeue(self.resq, fail_object)
        assert self.resq.size(self.queue_name) == 1
        job = Job.reserve(self.queue_name,self.resq)
        assert job._queue == self.queue_name
        mod_with_class = '{module}.{klass}'.format(
            module=self.job_class.__module__,
            klass=self.job_class.__name__)
        assert job._payload == {'class':mod_with_class,'args':['test1']}
