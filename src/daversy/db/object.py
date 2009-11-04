from daversy.utils import odict

class DbObject(dict):
    """A dictionary that allows its elements to be accessed as attributes.

       If any sub-elements are defined, then they are initialized to an
       empty list."""

    def __init__(self):
        if hasattr(self, 'SubElements'):
            for name, type in self.SubElements.items():
                self[name] = odict()

    def __setattr__(self, key, value):
        self[key] = value

    def __getattr__(self, key):
        return self[key]

# -----------------------------------------------------------------------

class Sequence(DbObject):
    """ A class that represents a database sequence that is typically used
        to generate a unique ID for a primary key."""

class Trigger(DbObject):
    """ A class that represents a database trigger."""

class TableColumn(DbObject):
    """ A class that represents a column in a table. """

class ViewColumn(DbObject):
    """ A class that represents a column in a view. """

class PrimaryKeyColumn(DbObject):
    """ A class that represents a column in a primary key. """

class UniqueKeyColumn(DbObject):
    """ A class that represents a column in a unique key. """

class ForeignKeyColumn(DbObject):
    """ A class that represents a column reference in a foreign key."""

class IndexColumn(DbObject):
    """ A class that represents a column in a index. """

class PrimaryKey(DbObject):
    """ A class that represents a primary key for a table. """
    SubElements = odict(('columns',  PrimaryKeyColumn))

class UniqueKey(DbObject):
    """ A class that represents a unique key for a table. """
    SubElements = odict(('columns',  UniqueKeyColumn))

class CheckConstraint(DbObject):
    """ A class that represents a check constraint for a table. """

class Table(DbObject):
    """ A class that represents a database table. """
    SubElements = odict( ('columns',      TableColumn),
                         ('primary_keys', PrimaryKey),
                         ('unique_keys',  UniqueKey),
                         ('constraints',  CheckConstraint) )

class Index(DbObject):
    """ A class that represents a index on a table. """
    SubElements = odict(('columns',  IndexColumn))

class ForeignKey(DbObject):
    """ A class that represents a a foreign key referential constraint. """
    SubElements = odict(('columns',  ForeignKeyColumn))

class View(DbObject):
    """ A class that represents a database view. """
    SubElements = odict(('columns',  ViewColumn))

class Function(DbObject):
    """ A class that represents a database function. """

class StoredProcedure(DbObject):
    """ A class that represents a database function. """

