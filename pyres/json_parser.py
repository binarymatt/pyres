from datetime import datetime
try:
    #import simplejson as json
    import json
except ImportError:
    import simplejson as json


DATE_FORMAT = '%Y-%m-%dT%H:%M:%S'
DATE_PREFIX = '@D:'


class CustomJSONEncoder(json.JSONEncoder):

    def default(self, o):
        if isinstance(o, datetime):
            return o.strftime(DATE_PREFIX + DATE_FORMAT)
        return json.JSONEncoder.default(self, o)


class CustomJSONDecoder(json.JSONDecoder):

    def decode(self, json_string):
        decoded = json.loads(json_string)
        return self.convert(decoded)

    def convert(self, value):
        if isinstance(value, basestring) and value.startswith(DATE_PREFIX):
            try:
                return datetime.strptime(value[len(DATE_PREFIX):], DATE_FORMAT)
            except ValueError:
                return value
        elif isinstance(value, dict):
            for k, v in value.iteritems():
                new = self.convert(v)
                if new != v:
                    value[k] = new
        elif isinstance(value, list):
            for k, v in enumerate(value):
                new = self.convert(v)
                if new != v:
                    value[k] = new
        return value


def dumps(values):
    return json.dumps(values, cls=CustomJSONEncoder)


def loads(string):
    return json.loads(string, cls=CustomJSONDecoder)
