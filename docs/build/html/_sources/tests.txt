Tests
=======

PyRes comes with a test suite which connects to a local redis server and 
creates a couple of *Queues* and *jobs*. 

To run tests make sure you have nose_ installed.::

    $ easy_install nose
    $ redis-server [PATH_TO_YOUR_REDIS_CONFIG]
    $ nosetests

Add **-v** flag if you want verbose output.

.. _nose: http://somethingaboutorange.com/mrl/projects/nose/0.11.1/