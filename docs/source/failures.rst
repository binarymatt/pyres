Failures
===============

Pyres provides a ``BaseBackend`` for handling failed jobs. You can subclass
this backend to store failed jobs in any system you like.

Currently, the only provided backend is a ``RedisBackend`` which will store
your failed jobs into a special *failed* queue for later processing or reenqueueing.

Here's a simple example::

	>>> from pyres import failure
	>>> from pyres.job import Job
	>>> from pyres import ResQ
	>>> r = Resq()
	>>> job = Job.reserve('basic',r)
	>>> job.fail("problem")