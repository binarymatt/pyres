Introduction
============

Pyres is a resque_ clone built in python. Resque is used by Github as their 
message queue. Both use Redis_ as the queue backend and provide a web-based
monitoring application. 

Read_ the blog post from github about how they use resque in production. 

:synopsis: Put jobs (which can be any kind of class) on a queue and process them while watching the progress via your browser.

Read our :doc:`example implementation </example>` of how pyres can be used to spam check comments.


.. _resque: http://github.com/defunkt/resque#readme
.. _Read: http://github.com/blog/542-introducing-resque
.. _Redis: http://code.google.com/p/redis/
