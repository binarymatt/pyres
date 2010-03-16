import signal
import time
import logging

from pyres import ResQ

class Scheduler(object):
    
    def __init__(self, server="localhost:6379", password=None):
        """
        >>> from pyres.scheduler import Scheduler
        >>> scheduler = Scheduler('localhost:6379')
        """
        self._shutdown = False
        if isinstance(server,basestring):
            self.resq = ResQ(server=server, password=password)
        elif isinstance(server, ResQ):
            self.resq = server
        else:
            raise Exception("Bad server argument")
    
    def register_signal_handlers(self):
        logging.info('registering signals')
        signal.signal(signal.SIGTERM, self.schedule_shutdown)
        signal.signal(signal.SIGINT, self.schedule_shutdown)
        signal.signal(signal.SIGQUIT, self.schedule_shutdown)
    
    def schedule_shutdown(self, signal, frame):
        logging.info('shutting down started')
        self._shutdown = True
    
    def __call__(self):
        logging.info('starting up')
        self.register_signal_handlers()
        #self.load_schedule()
        logging.info('looking for delayed items')
        while True:
            if self._shutdown is True:
                break
            self.handle_delayed_items()
            logging.debug('sleeping')
            time.sleep(5)
        logging.info('shutting down complete')
    
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
            logging.info('handling timestamp: %s' % timestamp)
            for item in self.next_item(timestamp):
                logging.debug('queueing item %s' % item)
                klass = item['class']
                queue = item['queue']
                args = item['args']
                kwargs = {}
                if 'first_attempt' in item:
                    kwargs['first_attempt'] = item['first_attempt']
                self.resq.enqueue_from_string(klass, queue, *args, **kwargs)
            
        
    @classmethod
    def run(cls, server, password=None):
        sched = cls(server=server, password=password)
        sched()
    
    
