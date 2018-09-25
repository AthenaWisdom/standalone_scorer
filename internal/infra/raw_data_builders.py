__author__ = 'izik'


def build_raw_file_string_from_collections(rows, delimiter=',', header=None):
    return delimiter.join(header) + "\n" + "\n".join([delimiter.join([str(y) for y in x]) for x in rows])
