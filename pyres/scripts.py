import logging

from optparse import OptionParser

from pyres.horde import Khan
from pyres import setup_logging, setup_pidfile
from pyres.scheduler import Scheduler
from pyres.worker import Worker


def pyres_manager():
    usage = "usage: %prog [options] arg1"
    parser = OptionParser(usage=usage)
    #parser.add_option("-q", dest="queue_list")
    parser.add_option("--host", dest="host", default="localhost")
    parser.add_option("--port", dest="port",type="int", default=6379)
    parser.add_option("--password", dest="password", default=None)
    parser.add_option("-i", '--interval', dest='manager_interval', default=None, help='the default time interval to sleep between runs - manager')
    parser.add_option("--minions_interval", dest='minions_interval', default=None, help='the default time interval to sleep between runs - minions')
    parser.add_option('-l', '--log-level', dest='log_level', default='info', help='log level.  Valid values are "debug", "info", "warning", "error", "critical", in decreasing order of verbosity. Defaults to "info" if parameter not specified.')
    parser.add_option("--pool", type="int", dest="pool_size", default=1, help="Number of minions to spawn under the manager.")
    parser.add_option("-j", "--process_max_jobs", dest="max_jobs", type=int, default=0, help='how many jobs should be processed on worker run.')
    parser.add_option('-f', dest='logfile', help='If present, a logfile will be used.  "stderr", "stdout", and "syslog" are all special values.')
    parser.add_option('-p', dest='pidfile', help='If present, a pidfile will be used.')
    parser.add_option("--concat_minions_logs", action="store_true", dest="concat_minions_logs", help='Concat all minions logs on same file.')
    (options,args) = parser.parse_args()

    if len(args) != 1:
        parser.print_help()
        parser.error("Argument must be a comma seperated list of queues")

    log_level = getattr(logging, options.log_level.upper(), 'INFO')
    #logging.basicConfig(level=log_level, format="%(asctime)s: %(levelname)s: %(message)s")
    concat_minions_logs = options.concat_minions_logs
    setup_pidfile(options.pidfile)

    manager_interval = options.manager_interval
    if manager_interval is not None:
        manager_interval = float(manager_interval)

    minions_interval = options.minions_interval
    if minions_interval is not None:
        minions_interval = float(minions_interval)

    queues = args[0].split(',')
    server = '%s:%s' % (options.host,options.port)
    password = options.password
    Khan.run(pool_size=options.pool_size, queues=queues, server=server, password=password, interval=manager_interval,
            logging_level=log_level, log_file=options.logfile, minions_interval=minions_interval,
            concat_minions_logs=concat_minions_logs, max_jobs=options.max_jobs)


def pyres_scheduler():
    usage = "usage: %prog [options] arg1"
    parser = OptionParser(usage=usage)
    #parser.add_option("-q", dest="queue_list")
    parser.add_option("--host", dest="host", default="localhost")
    parser.add_option("--port", dest="port",type="int", default=6379)
    parser.add_option("--password", dest="password", default=None)
    parser.add_option('-l', '--log-level', dest='log_level', default='info', help='log level.  Valid values are "debug", "info", "warning", "error", "critical", in decreasing order of verbosity. Defaults to "info" if parameter not specified.')
    parser.add_option('-f', dest='logfile', help='If present, a logfile will be used.  "stderr", "stdout", and "syslog" are all special values.')
    parser.add_option('-p', dest='pidfile', help='If present, a pidfile will be used.')
    (options,args) = parser.parse_args()
    log_level = getattr(logging, options.log_level.upper(),'INFO')
    #logging.basicConfig(level=log_level, format="%(module)s: %(asctime)s: %(levelname)s: %(message)s")
    setup_logging(procname="pyres_scheduler", log_level=log_level, filename=options.logfile)
    setup_pidfile(options.pidfile)
    server = '%s:%s' % (options.host, options.port)
    password = options.password
    Scheduler.run(server, password)


def pyres_worker():
    usage = "usage: %prog [options] arg1"
    parser = OptionParser(usage=usage)

    parser.add_option("--host", dest="host", default="localhost")
    parser.add_option("--port", dest="port",type="int", default=6379)
    parser.add_option("--password", dest="password", default=None)
    parser.add_option("-i", '--interval', dest='interval', default=None, help='the default time interval to sleep between runs')
    parser.add_option('-l', '--log-level', dest='log_level', default='info', help='log level.  Valid values are "debug", "info", "warning", "error", "critical", in decreasing order of verbosity. Defaults to "info" if parameter not specified.')
    parser.add_option('-f', dest='logfile', help='If present, a logfile will be used.  "stderr", "stdout", and "syslog" are all special values.')
    parser.add_option('-p', dest='pidfile', help='If present, a pidfile will be used.')
    parser.add_option("-t", '--timeout', dest='timeout', default=None, help='the timeout in seconds for this worker')
    (options,args) = parser.parse_args()

    if len(args) != 1:
        parser.print_help()
        parser.error("Argument must be a comma seperated list of queues")

    log_level = getattr(logging, options.log_level.upper(), 'INFO')
    setup_logging(procname="pyres_worker", log_level=log_level, filename=options.logfile)
    setup_pidfile(options.pidfile)

    interval = options.interval
    if interval is not None:
        interval = int(interval)

    timeout = options.timeout and int(options.timeout)

    queues = args[0].split(',')
    server = '%s:%s' % (options.host,options.port)
    password = options.password
    Worker.run(queues, server, password, interval, timeout=timeout)
