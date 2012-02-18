from daversy.utils      import *
from daversy.db.object  import UniqueKey, UniqueKeyColumn

class UniqueKeyColumnBuilder(object):
    """ Represents a builder for a column in a unique key. """

    DbClass = UniqueKeyColumn
    XmlTag  = 'constraint-column'

    Query = """
        SELECT cols.column_name, c.constraint_name, c.table_name, cols.position
        FROM   sys.user_constraints c, sys.user_cons_columns cols
        WHERE  c.constraint_name = cols.constraint_name
        AND    c.constraint_type = 'U'
        ORDER BY c.constraint_name, cols.position
    """

    PropertyList = odict(
        ('COLUMN_NAME',     Property('name')),
        ('CONSTRAINT_NAME', Property('key-name', exclude=True)),
        ('TABLE_NAME',      Property('table-name', exclude=True)),
        ('POSITION',        Property('position', exclude=True))
    )

    @staticmethod
    def addToState(state, column):
        table = state.tables.get(column['table-name'])
        if table:
            key = table.unique_keys.get(column['key-name'])
            if key:
                key.columns[column.name] = column

class UniqueKeyBuilder(object):
    """ Represents a builder for a unique key. """

    DbClass = UniqueKey
    XmlTag  = 'unique-key'

    Query = """
        SELECT c.constraint_name AS name, c.table_name,
               DECODE(c.deferrable, 'DEFERRABLE', lower(c.deferred)) AS defer_type,
               DECODE(i.compression, 'ENABLED', i.prefix_length) AS "COMPRESS"
        FROM   sys.user_constraints c
        LEFT JOIN sys.user_indexes i ON c.index_name = i.index_name
        WHERE  c.constraint_type = 'U'
        ORDER BY c.constraint_name
    """

    PropertyList = odict(
        ('NAME',       Property('name')),
        ('DEFER_TYPE', Property('defer-type')),
        ('COMPRESS',   Property('compress')),
        ('TABLE_NAME', Property('table-name', exclude=True))
    )

    @staticmethod
    def addToState(state, key):
        table = state.tables.get(key['table-name'])
        if table:
            table.unique_keys[key.name] = key

    @staticmethod
    def sql(key):
        definition = "CONSTRAINT %(name)s UNIQUE ( %(columns)s )"
        if key['defer-type']:
            definition += " DEFERRABLE INITIALLY %(defer-type)s"

        columns = ", ".join([column.name for column in key.columns.values()])

        return render(definition, key, columns=columns)
