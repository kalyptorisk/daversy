import os, re, stat, subprocess
from UserDict import DictMixin

class Property(object):
    def __init__(self, name, default=None, translator=None, exclude=False, cdata=False):
        self.name       = name
        self.default    = default
        self.translator = translator or (lambda value: value)
        self.exclude    = exclude
        self.cdata      = cdata

    def __call__(self, object, value):
        if value:
            raw_str = self.translator(value)

            try:
                new_value = unicode(raw_str)
            except UnicodeDecodeError:
                new_value = raw_str.decode('utf-8')

            object[self.name] = new_value.replace('\x00','').strip()
        elif self.default is not None:
            object[self.name] = str(self.default)
        else:
            object[self.name] = None

def render(template, keys, **args):
    variables = keys.copy()
    variables.update(args)
    return template % variables

def sql_escape(string):
    if not string:
        return ""
    return string.replace("'", "''")

def trim_spaces(string):
    if not string:
        return string
    return re.sub('[ \t]+\n', '\n', string.strip())

# Ordered dictionary implementation (with minor tweaks) from
# http://aspn.activestate.com/ASPN/Cookbook/Python/Recipe/496761
#
class odict(DictMixin):
    def __init__(self, *initial):
        self._keys = []
        self._data = {}
        for key, value in initial:
            self[key] = value

    def __setitem__(self, key, value):
        if key not in self._data:
            self._keys.append(key)
        self._data[key] = value

    def __getitem__(self, key):
        return self._data[key]

    def __delitem__(self, key):
        del self._data[key]
        self._keys.remove(key)

    def keys(self):
        return list(self._keys)

    def copy(self):
        copyDict = odict()
        copyDict._data = self._data.copy()
        copyDict._keys = self._keys[:]
        return copyDict

def execute(cmd, cwd=None, env=None):
    command = subprocess.Popen(cmd, cwd=cwd, env=env,
                               stderr=subprocess.STDOUT,
                               stdout=subprocess.PIPE)
    result = command.wait()
    output = command.stdout.read()
    del command
    return result, output

def remove_recursive(location):
    try:
        for root, dirs, files in os.walk(location, topdown=False):
            for name in files:
                os.chmod(os.path.join(root, name), stat.S_IWRITE)
                os.remove(os.path.join(root, name))
            for name in dirs:
                os.rmdir(os.path.join(root, name))
        os.rmdir(location)
    except:
        # we don't care if there is leftover stuff
        pass

try:
    import uuid
    def get_uuid4():
        return str(uuid.uuid4())
except:
    # based on http://zesty.ca/python/uuid.py
    def get_uuid4():
        number = long(('%02x'*16) % tuple(map(ord, os.urandom(16))), 16)
        # mark it as a RFC 4122 variant
        number &= ~(0xc000 << 48L)
        number |= 0x8000 << 48L
        # mark it as a version 4 UUID
        number &= ~(0xf000 << 64L)
        number |= 4 << 76L

        hex = '%032x' % number
        return '%s-%s-%s-%s-%s' % (hex[:8], hex[8:12], hex[12:16], hex[16:20], hex[20:])

