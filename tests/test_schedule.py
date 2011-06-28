import calendar
from tests import PyResTests, Basic
from pyres.scheduler import Scheduler
from datetime import datetime, timedelta, tzinfo

class GMT1(tzinfo):
    def __init__(self):         # DST starts last Sunday in March
        utc_now = datetime.utcnow()
        d = datetime(utc_now.year, 4, 1)   # ends last Sunday in October
        self.dston = d - timedelta(days=d.weekday() + 1)
        d = datetime(utc_now.year, 11, 1)
        self.dstoff = d - timedelta(days=d.weekday() + 1)
    def utcoffset(self, dt):
        return timedelta(hours=1) + self.dst(dt)
    def dst(self, dt):
        if self.dston <=  dt.replace(tzinfo=None) < self.dstoff:
            return timedelta(hours=1)
        else:
            return timedelta(0)
    def tzname(self,dt):
         return "GMT +1"

class ScheduleTests(PyResTests):
    def test_enqueue_at(self):
        d = datetime.utcnow() + timedelta(days=1)
        d2 = d + timedelta(days=1)
        key = int(calendar.timegm(d.utctimetuple()))
        key2 = int(calendar.timegm(d2.utctimetuple()))
        self.resq.enqueue_at(d, Basic,"test1")
        self.resq.enqueue_at(d, Basic,"test2")
        assert self.redis.llen("resque:delayed:%s" % key) == 2
        assert len(self.redis.zrange('resque:delayed_queue_schedule',0,20)) == 1
        self.resq.enqueue_at(d2, Basic,"test1")
        assert self.redis.llen("resque:delayed:%s" % key2) == 1
        assert len(self.redis.zrange('resque:delayed_queue_schedule',0,20)) == 2

    def test_enqueue_at_with_GMT1_aware_datetime_not_in_DST(self):
        utc_now = datetime.utcnow()
        gmt1_dt_not_in_dst = utc_now.replace(month=2, tzinfo=GMT1())
        d = gmt1_dt_not_in_dst + timedelta(days=1)
        d2 = d + timedelta(days=1)
        key = int(calendar.timegm(d.utctimetuple()))
        key2 = int(calendar.timegm(d2.utctimetuple()))
        self.resq.enqueue_at(d, Basic,"test1")
        self.resq.enqueue_at(d, Basic,"test2")
        assert self.redis.llen("resque:delayed:%s" % key) == 2
        assert len(self.redis.zrange('resque:delayed_queue_schedule',0,20)) == 1
        self.resq.enqueue_at(d2, Basic,"test1")
        assert self.redis.llen("resque:delayed:%s" % key2) == 1
        assert len(self.redis.zrange('resque:delayed_queue_schedule',0,20)) == 2

    def test_enqueue_at_with_GMT1_aware_datetime_in_DST(self):
        utc_now = datetime.utcnow()
        gmt1_dt_in_dst = utc_now.replace(month=5, tzinfo=GMT1())
        d = gmt1_dt_in_dst + timedelta(days=1)
        d2 = d + timedelta(days=1)
        key = int(calendar.timegm(d.utctimetuple()))
        key2 = int(calendar.timegm(d2.utctimetuple()))
        self.resq.enqueue_at(d, Basic,"test1")
        self.resq.enqueue_at(d, Basic,"test2")
        assert self.redis.llen("resque:delayed:%s" % key) == 2
        assert len(self.redis.zrange('resque:delayed_queue_schedule',0,20)) == 1
        self.resq.enqueue_at(d2, Basic,"test1")
        assert self.redis.llen("resque:delayed:%s" % key2) == 1
        assert len(self.redis.zrange('resque:delayed_queue_schedule',0,20)) == 2

    def test_delayed_queue_schedule_size(self):
        d = datetime.utcnow() + timedelta(days=1)
        d2 = d + timedelta(days=1)
        d3 = d
        self.resq.enqueue_at(d, Basic,"test1")
        self.resq.enqueue_at(d2, Basic,"test1")
        self.resq.enqueue_at(d3, Basic,"test1")
        assert self.resq.delayed_queue_schedule_size() == 3
    
    def test_delayed_timestamp_size(self):
        d = datetime.utcnow() + timedelta(days=1)
        key = int(calendar.timegm(d.utctimetuple()))
        self.resq.enqueue_at(d, Basic,"test1")
        assert self.resq.delayed_timestamp_size(key) == 1
        self.resq.enqueue_at(d, Basic,"test1")
        assert self.resq.delayed_timestamp_size(key) == 2
    
    def test_next_delayed_timestamp(self):
        d = datetime.utcnow() + timedelta(days=-1)
        d2 = d + timedelta(days=-2)
        key2 = int(calendar.timegm(d2.utctimetuple()))
        self.resq.enqueue_at(d, Basic,"test1")
        self.resq.enqueue_at(d2, Basic,"test1")
        item = self.resq.next_delayed_timestamp()
        assert  int(item) == key2
    
    def test_next_item_for_timestamp(self):
        d = datetime.utcnow() + timedelta(days=-1)
        d2 = d + timedelta(days=-2)
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
        