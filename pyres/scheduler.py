import signal
import time

from pyres import ResQ

class Scheduler(object):
    
    def __init__(self, server="localhost:6379", password=None):
        self._shutdown = False
        if isinstance(server,basestring):
            self.resq = ResQ(server=server, password=password)
        elif isinstance(server, ResQ):
            self.resq = server
        else:
            raise Exception("Bad server argument")
    
    def register_signal_handlers(self):
        signal.signal(signal.SIGTERM, self.schedule_shutdown)
        signal.signal(signal.SIGINT, self.schedule_shutdown)
        signal.signal(signal.SIGQUIT, self.schedule_shutdown)
    
    def schedule_shutdown(self):
        self._shutdown = True
    
    def run(self):
        self.register_signal_handlers()
        #self.load_schedule()
        while True:
            if self._shutdown is True:
                break
            self.handle_delayed_items()
            time.sleep(5)
    
    def next_timestamp(self):
        while True:
            timestamp = self.resq.next_delayed_timestamp()
            if timestamp:
                yield timestamp
            else:
                break
            
    
    def next_item(self, timestamp):
        while True:
            item = self.resq.next_item_for_timestamp(timestamp)
            if item:
                yield item
            else:
                break
    
    def handle_delayed_items(self):
        for timestamp in self.next_timestamp():
            for item in self.next_item(timestamp):
                print 'queueing item'
                klass = item['class']
                queue = item['queue']
                args = item['args']
                self.resq.enqueue_from_string(klass, queue, args)
            
        
    
