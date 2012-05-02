from datetime import datetime
from dateutil.tz import tzutc
from tests import PyResTests
import pyres.json_parser as json

class JSONTests(PyResTests):
    def test(self):
        naive_now = datetime.now()
        aware_now = datetime.now(tzutc())
        for now in [naive_now, aware_now]:
            for obj in [now, now.date(), now.timetz(), now.time()]:
                encoded = json.dumps(obj)
                decoded = json.loads(encoded)
                assert obj == decoded

    def test_in_list(self):
        naive_now = datetime.now()
        aware_now = datetime.now(tzutc())
        for now in [naive_now, aware_now]:
            for obj in [now, now.date(), now.timetz(), now.time()]:
                to_encode = [1, obj]
                encoded = json.dumps(to_encode)
                decoded = json.loads(encoded)
                assert to_encode == decoded

    def test_in_dict(self):
        naive_now = datetime.now()
        aware_now = datetime.now(tzutc())
        for now in [naive_now, aware_now]:
            for obj in [now, now.date(), now.timetz(), now.time()]:
                to_encode = dict(a=1, b=obj)
                encoded = json.dumps(to_encode)
                decoded = json.loads(encoded)
                assert to_encode == decoded

    def test_complex(self):
        naive_now = datetime.now()
        aware_now = datetime.now(tzutc())
        for now in [naive_now, aware_now]:
            for obj in [now, now.date(), now.timetz(), now.time()]:
                to_encode = dict(a=1, b=obj, c=dict(c1=2, c2=obj), d=[3, obj])
                encoded = json.dumps(to_encode)
                decoded = json.loads(encoded)
                assert to_encode == decoded
