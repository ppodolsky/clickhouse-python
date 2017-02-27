import codecs
import re

from six import PY3, binary_type, string_types, text_type

SPECIAL_CHARS = {
    "\b": "\\b",
    "\f": "\\f",
    "\r": "\\r",
    "\n": "\\n",
    "\t": "\\t",
    "\0": "\\0",
    "\\": "\\\\",
    "'": "\\'"
}


def prepend_if_not(prep, str):
    if not str.startswith(prep):
        return prep + str


def derive_relative_topology(topology, your_dc):
    '''
    Accepts topology in format {'DC 1': ['host1', 'host2'], 'DC 2': ['host3']} and
    transforms it to relative topology suitable for passing to Database class.
    :param topology: dict<str, list<str>>
    :param your_dc: str
    :return: relative topology
    '''
    relative_topology = {}
    for dc_name, hosts_list in topology.items():
        if dc_name == your_dc:
            relative_topology[1] = list(hosts_list)
        else:
            relative_topology.setdefault(2, []).extend(hosts_list)
    return relative_topology


def escape(value, quote=True):
    '''
    If the value is a string, escapes any special characters and optionally
    surrounds it with single quotes. If the value is not a string (e.g. a number),
    converts it to one.
    '''
    if isinstance(value, string_types):
        chars = (SPECIAL_CHARS.get(c, c) for c in value)
        value = "'" + "".join(chars) + "'" if quote else "".join(chars)
    return text_type(value)


def unescape(value):
    return codecs.escape_decode(value)[0].decode('utf-8')


def parse_tsv(line):
    if PY3 and isinstance(line, binary_type):
        line = line.decode()
    if line[-1] == '\n':
        line = line[:-1]
    return [unescape(value) for value in line.split('\t')]


def parse_array(array_string):
    '''
    Parse an array string as returned by clickhouse. For example:
        "['hello', 'world']" ==> ["hello", "world"]
        "[1,2,3]"            ==> [1, 2, 3]
    '''
    # Sanity check
    if len(array_string) < 2 or array_string[0] != '[' or array_string[-1] != ']':
        raise ValueError('Invalid array string: "%s"' % array_string)
    # Drop opening brace
    array_string = array_string[1:]
    # Go over the string, lopping off each value at the beginning until nothing is left
    values = []
    while True:
        if array_string == ']':
            # End of array
            return values
        elif array_string[0] in ', ':
            # In between values
            array_string = array_string[1:]
        elif array_string[0] == "'":
            # Start of quoted value, find its end
            match = re.search(r"[^\\]'", array_string)
            if match is None:
                raise ValueError('Missing closing quote: "%s"' % array_string)
            values.append(array_string[1:match.start() + 1])
            array_string = array_string[match.end():]
        else:
            # Start of non-quoted value, find its end
            match = re.search(r",|\]", array_string)
            values.append(array_string[1:match.start() + 1])
            array_string = array_string[match.end():]


def import_submodules(package_name):
    '''
    Import all submodules of a module.
    '''
    import importlib
    import pkgutil
    package = importlib.import_module(package_name)
    return {
        name: importlib.import_module(package_name + '.' + name)
        for _, name, _ in pkgutil.iter_modules(package.__path__)
    }
