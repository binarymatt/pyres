from tests import PyResTests, Basic, TestProcess, ErrorObject
from pyres import ResQ
from pyres.job import Job
from pyres.scheduler import Scheduler
import os
import datetime
import time
class ScheduleTests(PyResTests):
    def test_enqueue_at(self):
        d = datetime.datetime.now() + datetime.timedelta(days=1)
        d2 = d + datetime.timedelta(days=1)
        key = int(time.mktime(d.timetuple()))
        key2 = int(time.mktime(d2.timetuple()))
        self.resq.enqueue_at(d, Basic,"test1")
        self.resq.enqueue_at(d, Basic,"test2")
        assert self.redis.llen("resque:delayed:%s" % key) == 2
        assert len(self.redis.zrange('resque:delayed_queue_schedule',0,20)) == 1
        self.resq.enqueue_at(d2, Basic,"test1")
        assert self.redis.llen("resque:delayed:%s" % key2) == 1
        assert len(self.redis.zrange('resque:delayed_queue_schedule',0,20)) == 2
    
    def test_delayed_queue_schedule_size(self):
        d = datetime.datetime.now() + datetime.timedelta(days=1)
        d2 = d + datetime.timedelta(days=1)
        key = int(time.mktime(d.timetuple()))
        key2 = int(time.mktime(d2.timetuple()))
        self.resq.enqueue_at(d, Basic,"test1")
        self.resq.enqueue_at(d2, Basic,"test1")
        assert self.resq.delayed_queue_schedule_size() == 2
    
    def test_delayed_timestamp_size(self):
        d = datetime.datetime.now() + datetime.timedelta(days=1)
        d2 = d + datetime.timedelta(days=1)
        key = int(time.mktime(d.timetuple()))
        key2 = int(time.mktime(d2.timetuple()))
        self.resq.enqueue_at(d, Basic,"test1")
        assert self.resq.delayed_timestamp_size(key) == 1
        self.resq.enqueue_at(d, Basic,"test1")
        assert self.resq.delayed_timestamp_size(key) == 2
    
    def test_next_delayed_timestamp(self):
        d = datetime.datetime.now() + datetime.timedelta(days=-1)
        d2 = d + datetime.timedelta(days=-2)
        key = int(time.mktime(d.timetuple()))
        key2 = int(time.mktime(d2.timetuple()))
        self.resq.enqueue_at(d, Basic,"test1")
        self.resq.enqueue_at(d2, Basic,"test1")
        item = self.resq.next_delayed_timestamp()
        assert  int(item) == key2
    
    def test_next_item_for_timestamp(self):
        d = datetime.datetime.now() + datetime.timedelta(days=-1)
        d2 = d + datetime.timedelta(days=-2)
        #key = int(time.mktime(d.timetuple()))
        #key2 = int(time.mktime(d2.timetuple()))
        self.resq.enqueue_at(d, Basic,"test1")
        self.resq.enqueue_at(d2, Basic,"test1")
        timestamp = self.resq.next_delayed_timestamp()
        item = self.resq.next_item_for_timestamp(timestamp)
        assert isinstance(item, dict)
        assert self.redis.zcard('resque:delayed_queue_schedule') == 1
    
    def test_scheduler_init(self):
        scheduler = Scheduler(self.resq)
        assert not scheduler._shutdown
        scheduler = Scheduler('localhost:6379')
        assert not scheduler._shutdown
        self.assertRaises(Exception, Scheduler, Basic)
    
    def test_schedule_shutdown(self):
        scheduler = Scheduler(self.resq)
        scheduler.schedule_shutdown(19,'')
        assert scheduler._shutdown
        