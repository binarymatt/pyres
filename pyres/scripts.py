import logging

from optparse import OptionParser

from itty import run_itty

from pyres.horde import Khan
from pyres import setup_logging, setup_pidfile
from pyres.scheduler import Scheduler
from resweb import server as resweb_server
from pyres.worker import Worker


def pyres_manager():
    usage = "usage: %prog [options] arg1"
    parser = OptionParser(usage=usage)
    #parser.add_option("-q", dest="queue_list")
    parser.add_option("--host", dest="host", default="localhost")
    parser.add_option("--port",dest="port",type="int", default=6379)
    parser.add_option("-i", '--interval', dest='interval', default=None, help='the default time interval to sleep between runs')
    parser.add_option('-l', '--log-level', dest='log_level', default='info', help='log level.  Valid values are "debug", "info", "warning", "error", "critical", in decreasing order of verbosity. Defaults to "info" if parameter not specified.')
    parser.add_option("--pool", type="int", dest="pool_size", default=1, help="Number of minions to spawn under the manager.")
    parser.add_option('-f', dest='logfile', help='If present, a logfile will be used.  "stderr", "stdout", and "syslog" are all special values.')
    parser.add_option('-p', dest='pidfile', help='If present, a pidfile will be used.')
    (options,args) = parser.parse_args()

    if len(args) != 1:
        parser.print_help()
        parser.error("Argument must be a comma seperated list of queues")

    log_level = getattr(logging, options.log_level.upper(), 'INFO')
    #logging.basicConfig(level=log_level, format="%(asctime)s: %(levelname)s: %(message)s")

    setup_pidfile(options.pidfile)

    interval = options.interval
    if interval is not None:
        interval = float(interval)

    queues = args[0].split(',')
    server = '%s:%s' % (options.host,options.port)
    Khan.run(pool_size=options.pool_size, queues=queues, server=server, logging_level=log_level, log_file=options.logfile)


def pyres_scheduler():
    usage = "usage: %prog [options] arg1"
    parser = OptionParser(usage=usage)
    #parser.add_option("-q", dest="queue_list")
    parser.add_option("--host", dest="host", default="localhost")
    parser.add_option("--port",dest="port",type="int", default=6379)
    parser.add_option('-l', '--log-level', dest='log_level', default='info', help='log level.  Valid values are "debug", "info", "warning", "error", "critical", in decreasing order of verbosity. Defaults to "info" if parameter not specified.')
    parser.add_option('-f', dest='logfile', help='If present, a logfile will be used.  "stderr", "stdout", and "syslog" are all special values.')
    parser.add_option('-p', dest='pidfile', help='If present, a pidfile will be used.')
    (options,args) = parser.parse_args()
    log_level = getattr(logging, options.log_level.upper(),'INFO')
    #logging.basicConfig(level=log_level, format="%(module)s: %(asctime)s: %(levelname)s: %(message)s")
    setup_logging(procname="pyres_scheduler", log_level=log_level, filename=options.logfile)
    setup_pidfile(options.pidfile)
    server = '%s:%s' % (options.host, options.port)
    Scheduler.run(server)


def pyres_web():
    usage = "usage: %prog [options]"
    parser = OptionParser(usage)
    parser.add_option("--host",
                    dest="host",
                    default="localhost",
                    metavar="HOST")
    parser.add_option("--port",
                    dest="port",
                    type="int",
                    default=8080)
    parser.add_option("--dsn",
                      dest="dsn",
                      help="Redis server to display")
    parser.add_option("--server",
                      dest="server",
                      help="Server for itty to run under.",
                      default='wsgiref')
    (options,args) = parser.parse_args()

    if options.dsn:
        from pyres import ResQ
        resweb_server.HOST = ResQ(options.dsn)
    run_itty(host=options.host, port=options.port, server=options.server)


def pyres_worker():
    usage = "usage: %prog [options] arg1"
    parser = OptionParser(usage=usage)

    parser.add_option("--host", dest="host", default="localhost")
    parser.add_option("--port",dest="port",type="int", default=6379)
    parser.add_option("-i", '--interval', dest='interval', default=None, help='the default time interval to sleep between runs')
    parser.add_option('-l', '--log-level', dest='log_level', default='info', help='log level.  Valid values are "debug", "info", "warning", "error", "critical", in decreasing order of verbosity. Defaults to "info" if parameter not specified.')
    parser.add_option('-f', dest='logfile', help='If present, a logfile will be used.  "stderr", "stdout", and "syslog" are all special values.')
    parser.add_option('-p', dest='pidfile', help='If present, a pidfile will be used.')
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

    queues = args[0].split(',')
    server = '%s:%s' % (options.host,options.port)
    Worker.run(queues, server, interval)
