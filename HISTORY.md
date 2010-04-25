## 0.8 (2010-04-24)
* added the pyres_manager and the horde module. This allows a more prefork like model for processing jobs.
* setproctitle usage. Allows better process titles when viewing via ps
* ability to delete and requeue failed items

## 0.7.5.1 (2010-03-18)
* fixed the pyres_scheduler script
* changed download link to remove v from version number

## 0.7.5 (2010-03-18)
* added feature to retry jobs based on a class attribute

## 0.7.1 (2010-03-16)
* bug fix for pruning workers.

## 0.7.0 (2010-03-05)
* delayed tasks
* resweb pagination
* switch stored timestamps to a unix timestamp
* updated documentation
* upgraded to redis-py 1.34.1
* switched from print statements to the logging module
* import errors on jobs are now reported in the failed queue
* prune dead workers
* small bugfixes in the resweb package
* improved failure formatting
* datetime json parser

## 0.5.0 (2010-0114)

* added new documentation to the project
* update setup.py
* preparing for semantic versioning

## 0.4.1 (2010-01-06)

* fixed issue with new failure package in distutils sdist
* changed setup.py to remove camel case, because it's ugly

## 0.4.0 (2010-01-06)

* added the basics of failure backend support

## 0.3.1 (2009-12-16)

* minor bug fix in worker.py
* merged in some setup.py niceties from dsc fork
* merged in better README info from dsc fork

## 0.3.0 (2009-12-10)

* updated setup.py
* refactored package for better testing
* resque namespacing by fakechris
* smarter import from string by fakechris

## 0.2.0 (2009-12-09)

* Better web interface via resweb
* Updated the api to be more inline with resque
* More tests.

## 0.1.0 (2009-12-01)

* First release.