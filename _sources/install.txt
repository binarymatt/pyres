Installation
===============

Pyres is most easily installed using pip and can be found on PyPI as pyres_.

Using ``pip install pyres`` will install the required versions of the above packages/modules. 
Those requirements are currently:

::

	simplejson==2.0.9 
	itty==0.6.4
	redis==1.34.1 
	pystache==0.2.0
	
If you'd rather install from the git repository, that's easy too::

    $ git clone git://github.com/binarydud/pyres.git
    $ cd pyres
    $ python setup.py build
    $ python setup.py install

Of course, you'll need to install the Redis server as well. Below is a simple example, but 
please read `Redis's own documentation`_ for more details.

::

	$ wget http://redis.googlecode.com/files/redis-1.2.2.tar.gz
	$ tar -xvf redis-1.2.2.tar.gz
	$ cd redis-1.2.2
	$ make 
	$ ./redis-server
	
This will install and start a Redis server with the default config running on port 6379. 
This default config is good enough for you to run the pyres tests.

.. _pyres: http://pypi.python.org/pypi/pyres/
.. _Redis's own documentation: http://code.google.com/p/redis/wiki/index?tm=6



