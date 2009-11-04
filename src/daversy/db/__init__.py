import os, sys

from daversy.plugins import Plugin

class Database(Plugin):
    __plugins__ = 'daversy.db'

    @classmethod
    def get(cls, name):
        for database in cls.list():
            if name == database.__adapter__:
                return database
        return None

class Builder(object):
    pass

__all__ = ['oracle']