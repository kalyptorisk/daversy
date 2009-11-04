import os, sys, optparse, textwrap

class Plugin(object):
    """A very simple plugin system."""
    @classmethod
    def _load_plugins(cls):
        cls._list = []
        if not hasattr(cls, '__plugins__'):
            return
        try:
            module = __import__(cls.__plugins__, {}, {}, [''])
            for name in module.__all__:
                __import__('%s.%s' % (cls.__plugins__, name), {}, {}, ['*'])
            for subclass in cls.__subclasses__():
                cls._list.append(subclass)
        except Exception, e:
            print 'Exception', e
            pass

    @classmethod
    def list(cls):
        if not hasattr(cls, '_list'):
            cls._load_plugins()
        return cls._list
