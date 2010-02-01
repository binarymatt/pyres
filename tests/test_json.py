from datetime import datetime
from tests import PyResTests
import pyres.json_parser as json

class JSONTests(PyResTests):
    def test_encode_decode_date(self):
        dt = datetime(1972, 1, 22);
        encoded = json.dumps({'dt': dt})
        decoded = json.loads(encoded)
        assert decoded['dt'] == dt

    def test_dates_in_lists(self):
        dates = [datetime.now() for i in range(50)]
        decoded = json.loads(json.dumps(dates))
        for value in dates:
            assert isinstance(value, datetime)

    def test_dates_in_dict(self):
        dates = dict((i, datetime.now()) for i in range(50))
        decoded = json.loads(json.dumps(dates))
        for i, value in dates.items():
            assert isinstance(i, int)
            assert isinstance(value, datetime)

