#!/usr/bin/env python

import os, sys
from daversy         import VERSION
from daversy.command import Command


def main():
    cmd = os.path.basename(sys.argv[0])
    if len(sys.argv) == 1:
        print "Type '%s help' for usage." % (cmd)
        return

    if sys.argv[1] == 'help' and len(sys.argv) > 2:
        command = Command.get(sys.argv[2])
        if command:
            command.parser().print_help()
            return
    else:
        command = Command.get(sys.argv[1])
        if command:
            command(cmd, sys.argv[2:])
            return

    print DVS_HEADER % dict(cmd=cmd, version=VERSION)
    for command in Command.list():
        names = command.__names__
        print '  %s' % names[0],
        if len(names) > 1:
            print '(%s)' % ', '.join(names[1:])
        else:
            print
    print DVS_FOOTER

DVS_HEADER = """\
usage: %(cmd)s <subcommand> [options] [args]
Daversy command-line client, version %(version)s.
Type '%(cmd)s help <subcommand>' for help on a specific subcommand.

Available subcommands:
"""

DVS_FOOTER = """
Daversy is a source control tool for relational databases."""

if __name__ == '__main__':
    sys.exit(main())