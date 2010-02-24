Tests
=======

Pyres comes with a test suite which connects to a local Redis server and 
creates a couple of *queues* and *jobs*. 

Make sure you have nose_ installed::

    $ pip install nose

Also make sure your Redis server is running::

	$ cd path_to_redis_installation
	$ ./redis-server [PATH_TO_YOUR_REDIS_CONFIG]
	
If you don't give the ``./redis-server`` command the config path, it will use a default config, which should run the tests just fine.

Now, we're ready to run the tests. From the pyres install directory::

	$ nosetests
	............................................
	----------------------------------------------------------------------
	Ran 44 tests in 0.901s

	OK

Add **-v** flag if you want verbose output.

.. _nose: http://somethingaboutorange.com/mrl/projects/nose/0.11.1/