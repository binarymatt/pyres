from pyres import ResQ

def job(queue, resq=ResQ(),debug=False):
    def wrapper(func):
        def enqueue(*args):
            if not debug:
                class_name = '%s.%s' % (func.__module__, func.__name__)
                resq.enqueue_from_string(class_name, queue, *args)
            else:
                return func(*args)

        def __call__(self, *args):
            return func(*args)

        new_class = type('Job',(),{
            'queue': queue,
            'perform': staticmethod(func),
            'enqueue': staticmethod(enqueue),
            '__call__': __call__,
            '__name__': func.__name__
        })
        return new_class()
    return wrapper



