__author__ = 'Shahars'


def build_schema(header, types):
    temp_schema = dict(zip(header, types))
    schema = [{"name": key, "type": val} for key, val in temp_schema.iteritems()]
    return schema
