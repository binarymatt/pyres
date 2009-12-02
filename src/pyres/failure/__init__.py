import datetime
from pyres import ResQ 
import sys, traceback
from pyres.failure.redis import RedisBackend
_backend = RedisBackend
def create(options={}):
    return _backend(*options).save()


    
    
