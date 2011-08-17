import signal
import time
import logging

from pyres import ResQ, __version__

logger = logging.getLogger(__name__)

class Scheduler(object):

    def __init__(self, server="localhost:6379", password=None):
        """
        >>> from pyres.scheduler import Scheduler
        >>> scheduler = Scheduler('localhost:6379')
        """
        self._shutdown = False
        if isinstance(server, basestring):
            self.resq = ResQ(server=server, password=password)
        elif isinstance(server, ResQ):
            self.resq = server
        else:
            raise Exception("Bad server argument")

    def register_signal_handlers(self):
        logger.info('registering signals')
        signal.signal(signal.SIGTERM, self.schedule_shutdown)
        signal.signal(signal.SIGINT, self.schedule_shutdown)
        signal.signal(signal.SIGQUIT, self.schedule_shutdown)

    def schedule_shutdown(self, signal, frame):
        logger.info('shutting down started')
        self._shutdown = True

    def __call__(self):
        _setproctitle("Starting")
        logger.info('starting up')
        self.register_signal_handlers()
        #self.load_schedule()
        logger.info('looking for delayed items')
        while True:
            if self._shutdown:
                break
            self.handle_delayed_items()
            _setproctitle("Waiting")
            logger.debug('sleeping')
            time.sleep(5)
        logger.info('shutting down complete')

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
            _setproctitle('Handling timestamp %s' % timestamp)
            logger.info('handling timestamp: %s' % timestamp)
            for item in self.next_item(timestamp):
                logger.debug('queueing item %s' % item)
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


try:
    from setproctitle import setproctitle
except ImportError:
    def setproctitle(name):
        pass

def _setproctitle(msg):
    setproctitle("pyres_scheduler-%s: %s" % (__version__, msg))
