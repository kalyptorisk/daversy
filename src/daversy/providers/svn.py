import os, re
from cStringIO import StringIO
from tempfile  import mkdtemp
from daversy.utils     import *

REVISION_REGEX = re.compile('([^@]+)@(\d+)$')

def available():
    try:
        status, output = execute(['svn', '--version'])
        return (status == 0)
    except:
        pass
    return False

def load_file(location, comment):
    if len(location) < 3:
        return None

    temp_dir = mkdtemp()

    user, password = location[:2]
    url = ':'.join(location[2:])

    basedir  = url[:url.rindex('/')]
    filename = os.path.join(temp_dir, url[url.rindex('/')+1:])
    m = REVISION_REGEX.match(filename)
    if m:
        filename, revision = m.groups()
        status, output = execute(['svn', 'co', basedir, temp_dir, '--non-interactive', '-r',
                                  revision, '-N', '--username', user, '--password', password])
    else:
        status, output = execute(['svn', 'co', basedir, temp_dir, '--non-interactive',
                                  '-N', '--username', user, '--password', password])

    result = None
    if status == 0 and os.path.exists(filename):
        stream = file(filename, 'r')
        result = StringIO(stream.read())
        stream.close()

    remove_recursive(temp_dir)
    return result

def save_file(location, file_location, comment):
    if len(location) < 3:
        return None

    temp_dir = mkdtemp()

    user, password = location[:2]
    url = ':'.join(location[2:])

    basedir  = url[:url.rindex('/')]
    filename = os.path.join(temp_dir, url[url.rindex('/')+1:])

    status, output = execute(['svn', 'co', basedir, temp_dir, '--non-interactive',
                              '-N', '--username', user, '--password', password])

    if status != 0:
        remove_recursive(temp_dir)
        return False

    if not os.path.exists(filename):
        os.rename(file_location, filename)
        status, output = execute(['svn', 'add', filename])

        if status != 0:
                remove_recursive(temp_dir)
                return None
    else:
        os.remove(filename)
        os.rename(file_location, filename)

    status, output = execute(['svn', 'commit', filename,
                              '-m', comment, '--non-interactive',
                              '--username', user, '--password', password])
    remove_recursive(temp_dir)
    return status == 0
