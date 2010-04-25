Prefork Manager
===============

Sometimes the fork for every job method of processing can be a bit too slow and
take up too many resources. Pyres provides an alternative to the pyres_worker through
the pyres_manager script and the horde module.

The pyres_manager script is very similar to the pyres_worker. However, instead
of forking a child for every job, the manager takes advantage of the multiprocessing
module in python 2.6 (backported to 2.5 as well) and forks off a pool of children
at startup time. These children then query the redis queue and perform the necessary work.
It is the managers job to manage the pool via signals or a command queue on the redis
server.

ex::

    pyres_manager --pool=5 queue_one,queue_two
    
