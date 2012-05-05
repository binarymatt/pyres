class NoQueueError(Exception):
    pass

class JobError(RuntimeError):
    pass

class TimeoutError(JobError):
    pass

class CrashError(JobError):
    pass