import os
from cStringIO import StringIO
from tempfile  import mkdtemp
from daversy.utils     import *

def available():
    try:
        status, output = execute(['ss'])
        return (status == 0)
    except:
        pass
    return False

def load_file(params, comment):
    if len(params) < 4:
        return None

    user, password, database, path = params
    if not path.startswith('$/'):
        path = '$/' + path

    temp_dir = mkdtemp()
    filename = path[path.rindex('/')+1:]
    location = os.path.join(temp_dir, filename)

    environment = os.environ.copy()
    environment['SSDIR'] = database

    status, output = execute(['ss', 'get', path, '-GL%s' % temp_dir, '-W',
                               '-Y%s,%s' % (user, password)], env=environment)
    result = None
    if status == 0:
        stream = file(location, 'r')
        result = StringIO(stream.read())
        stream.close()

    remove_recursive(temp_dir)
    return result

def save_file(params, file_location, comment):
    if len(params) < 4:
        return None

    user, password, database, path = params
    if not path.startswith('$/'):
        path = '$/' + path

    temp_dir = mkdtemp()
    filename = path[path.rindex('/')+1:]
    location = os.path.join(temp_dir, filename)

    environment = os.environ.copy()
    environment['SSDIR'] = database

    status, output = execute(['ss', 'cp', path[:path.rindex('/')],
                              '-Y%s,%s' % (user, password)], env=environment)

    if not status == 0:
        remove_recursive(temp_dir)
        return False

    status, output = execute(['ss', 'checkout', filename, '-GL%s' % temp_dir,
                               '-Y%s,%s' % (user, password)], env=environment)

    if not status == 0:
        os.rename(file_location, location)
        status, output = execute(['ss', 'add', location,
                                   '-C%s' % comment,
                                   '-Y%s,%s' % (user, password)], env=environment)
    else:
        os.remove(location)
        os.rename(file_location, location)
        status, output = execute(['ss', 'checkin', filename,
                                   '-GL%s' % temp_dir, '-C%s' % comment,
                                   '-Y%s,%s' % (user, password)], env=environment)

    remove_recursive(temp_dir)
    return status == 0
