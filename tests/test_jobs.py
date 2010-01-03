from tests import PyResTests, Basic, TestProcess
from pyres.job import Job
class JobTests(PyResTests):
    def test_reserve(self):
        self.resq.enqueue(Basic,"test1")
        job = Job.reserve('basic', self.resq)
        assert job._queue == 'basic'
        assert job._payload
        print job._payload
        assert job._payload == {'class':'tests.Basic','args':['test1']}
    
    def test_perform(self):
        self.resq.enqueue(Basic,"test1")
        job = Job.reserve('basic',self.resq)
        self.resq.enqueue(TestProcess)
        job2 = Job.reserve('high', self.resq)
        assert job.perform() == "name:test1"
        assert job2.perform()
    
    def test_fail(self):
        self.resq.enqueue(Basic,"test1")
        job = Job.reserve('basic',self.resq)
        assert self.redis.llen('resque:failed') == 0
        job.fail("problem")
        assert self.redis.llen('resque:failed') == 1