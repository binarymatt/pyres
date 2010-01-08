try:
    import multiprocessing
except:
    import sys
    sys.exit("multiprocessing was not available")
import os, datetime, time, signal, sys
from pyres import ResQ
from pyres.exceptions import NoQueueError
from pyres.worker import Worker
class Minion(multiprocessing.Process, Worker):
    def __init__(self, conn, queues=[]):
        self.conn = conn
        self.queues = queues
        super(Minion,self).__init__(name='Minion')
    
    def _check_message(self):
        if self.conn.poll():
            message = self.conn.recv()
            self.process_message(message)
    
    def process_message(self, message):
        pass
    
    def work(self, interval=5):
        self.startup()
        while True:
            self._check_messages()
            if self._shutdown:
                print 'shutdown scheduled'
                break
            job = self.reserve()
            if job:
                self.process(job)
            else:
                if interval == 0:
                    break
                time.sleep(interval)
        self.unregister_worker()
    
    def run(self):
        self.work()
        #while True:
        #    job = self.q.get()
        #    print 'pid: %s is running %s ' % (self.pid,job)
    
class Khan(Worker):
    _command_map = {
        'ADD': '_add_minion',
        'REMOVE': '_remove_minion',
        'SHUTDOWN': '_schedule_shutdown'
    }
    _workers = {}
    def __init__(self, pool_size=5, queue_list=[], server='localhost:6379', password=None):
        self.pool_size = pool_size
        self.resq = ResQ(server, password=password)
        self._workers = list()
    
    def _check_command(self):
        if not self._shutdown:
            command = self.resq.redis.pop('resque:khan:%s' % self)
            if command:
                self.process_command(command)
                self._check_command()
    
    def process_command(self, command):
        #available commands, shutdown, add 1, remove 1
        command = self._command_map.get(command, None)
        if command:
            fn = getattr(self, command)
            if fn:
                fn()
    
    def _add_minion(self):
        parent_conn, child_conn = multiprocessing.Pipe()
        m = Minion(child_conn, q)
        m.start()
        self._workers.append((parent_conn, m))
    
    def _remove_minion(self):
        self._workers
    def run():
        self.startup()
        q = multiprocessing.Queue()
        for i in range(pool_size):
            self._add_minion()
        while True:
            self._check_commands()
            if self._shutdown:
                #send signals to each child
                break
            time.sleep(5)
        self.unregister_worker()


#if __name__ == "__main__":
#    k = Khan()
#    k.run()

if __name__ == "__main__":
    from optparse import OptionParser
    parser = OptionParser(usage="%prog [options] queue list")
    parser.add_option("-s", dest="server", default="localhost:6379")
    (options,args) = parser.parse_args()
    if len(args) < 1:
        parser.print_help()
        parser.error("Please give the horde at least one queue.")
    khan = Khan(queue_list=args, server=options.dest)
    khan.run()
    #Worker.run(queues, options.server)