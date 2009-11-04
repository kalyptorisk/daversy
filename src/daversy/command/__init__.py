import os, sys

from textwrap import fill
from optparse import OptionParser

from daversy.plugins import Plugin

class Command(Plugin):
    __plugins__ = 'daversy.command'

    @classmethod
    def get(cls, name):
        for command in cls.list():
            if name in command.__names__:
                return command
        return None

    @classmethod
    def parser(cls):
        if hasattr(cls, '_parser'):
            return cls._parser

        wrap = lambda x: fill(x, initial_indent='  ', subsequent_indent='  ')

        args = ' '.join(cls.__args__)
        desc = '\n\n'.join([wrap(x) for x in cls.__usage__])
        usage = '%s %s %s\n\n%s' % (os.path.basename(sys.argv[0]),
                                    cls.__names__[0], args, desc)

        cls._parser = OptionParser(usage=usage, option_list=cls.__options__)
        return cls._parser

    def __init__(self, cmd, argv):
        (options, args) = self.parser().parse_args(argv)

        if not len(args) == len(self.__args__):
            self.parser().print_help()
            sys.exit(1)

        self.execute(args, options)

__all__ = ['copy', 'generate', 'compare', 'name']
