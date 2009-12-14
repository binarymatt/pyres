PyRes - a Resque clone
======================

[Resque](http://github.com/defunkt/resque) is a great implementation of a job queue by the people at github. It's written in ruby, which is great, but I primarily work in python. So I took on the task of porting over the code to python and PyRes was the result


## Project Goals

Because of some differences between ruby and python, there are a couple of places where I chose speed over correctness. The goal will be to eventually take the application and make it more pythonic without sacrificing the awesome functionality found in resque. At the same time, I hope to stay within the bounds of the original api and web interface.


## Running Tests

 1. Install nose: `$ easy_install nose`
 2. Start redis: `$ redis-server [PATH_TO_YOUR_REDIS_CONFIG]`
 3. Run nose: `$ nosetests` Or more verbosely: `$ nosetests -v`

	$ nosetests -v
	tests.test_str_to_class ... ok
	test_fail (tests.test_jobs.JobTests) ... ok
	test_perform (tests.test_jobs.JobTests) ... ok
	test_reserve (tests.test_jobs.JobTests) ... ok
	test_enqueue (tests.test_resq.ResQTests) ... ok
	test_enqueue_from_string (tests.test_resq.ResQTests) ... ok
	test_info (tests.test_resq.ResQTests) ... ok
	test_keys (tests.test_resq.ResQTests) ... ok
	test_peek (tests.test_resq.ResQTests) ... ok
	test_pop (tests.test_resq.ResQTests) ... ok
	test_push (tests.test_resq.ResQTests) ... ok
	test_redis_property (tests.test_resq.ResQTests) ... ok
	test_remove_queue (tests.test_resq.ResQTests) ... ok
	test_size (tests.test_resq.ResQTests) ... ok
	test_workers (tests.test_resq.ResQTests) ... ok
	test_clear (tests.test_stats.StatTests) ... ok
	test_decr (tests.test_stats.StatTests) ... ok
	test_get (tests.test_stats.StatTests) ... ok
	test_incr (tests.test_stats.StatTests) ... ok
	test_failed (tests.test_worker.WorkerTests) ... ok
	test_get_job (tests.test_worker.WorkerTests) ... ok
	test_job_failure (tests.test_worker.WorkerTests) ... ok
	test_process (tests.test_worker.WorkerTests) ... ok
	test_processed (tests.test_worker.WorkerTests) ... ok
	test_register (tests.test_worker.WorkerTests) ... ok
	test_signals (tests.test_worker.WorkerTests) ... ok
	test_started (tests.test_worker.WorkerTests) ... ok
	test_startup (tests.test_worker.WorkerTests) ... ok
	test_unregister (tests.test_worker.WorkerTests) ... ok
	test_worker_init (tests.test_worker.WorkerTests) ... ok
	test_working (tests.test_worker.WorkerTests) ... ok
	test_working_on (tests.test_worker.WorkerTests) ... ok
	
	----------------------------------------------------------------------
	Ran 32 tests in 0.794s
	
	OK


## TODO

Stabalize the api.

Flesh out a python version of the web interface. Currently, there is a resweb module that uses the itty micro framework and the jinja2 templating engine to display basics. I'd like to get this as close to the resque web interface as possible.

Better test coverage.

Better documentation.