try:
    import multiprocessing
except:
    import sys
    sys.exit("multiprocessing was not available")
import os, datetime, time, signal, sys
from pyres import ResQ
    
from pyres.exceptions import NoQueueError
from pyres.worker import Worker
class Minion(multiprocessing.Process):
    def __init__(self, conn, queue):
        self.conn = conn
        self.q = queue
        super(Minion,self).__init__(name='Minion')
    
    def run(self):
        while True:
            job = self.q.get()
            print 'pid: %s is running %s ' % (self.pid,job)
    
class Khan(object):
    _workers = {}
    def __init__(self, pool_size=5):
        self.pool_size = pool_size
    def run():
        q = multiprocessing.Queue()
        for i in range(pool_size):
            parent_conn, child_conn = multiprocessing.Pipe()
            m = Minion(child_conn, q)
            print m.pid
            m.start()
            print m.pid
            self._workers[m.pid] = parent_conn


if __name__ == "__main__":
    k = Khan()
    k.run()