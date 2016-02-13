from daversy.utils      import *
from daversy.db.object  import ForeignKey, ForeignKeyColumn

class ForeignKeyColumnBuilder(object):
    """ Represents a builder for a column in a foreign key reference."""

    DbClass = ForeignKeyColumn
    XmlTag  = 'foreign-key-column'

    Query = """
        SELECT k.constraint_name, kc.position, kc.column_name AS key_column, rc.column_name AS reference_column
        FROM   sys.user_constraints k, sys.user_constraints r,
               sys.user_cons_columns kc, sys.user_cons_columns rc
        WHERE  k.constraint_type = 'R'
        AND    r.constraint_type IN ('P', 'U')
        AND    k.r_constraint_name = r.constraint_name
        AND    k.constraint_name = kc.constraint_name
        AND    r.constraint_name = rc.constraint_name
        AND    kc.position       = rc.position
        ORDER BY k.constraint_name, kc.position
    """

    PropertyList = odict(
        ('CONSTRAINT_NAME',  Property('constraint-name', exclude=True)),
        ('POSITION',         Property('position',        exclude=True)),
        ('KEY_COLUMN',       Property('name')),
        ('REFERENCE_COLUMN', Property('reference'))
    )

    @staticmethod
    def addToState(state, column):
        foreign_key = state.foreign_keys.get(column['constraint-name'])
        if foreign_key:
            foreign_key.columns[column.name] = column

class ForeignKeyBuilder(object):
    """ Represents a builder for a foreign key. """

    DbClass = ForeignKey
    XmlTag  = 'foreign-key'

    Query = """
        SELECT k.constraint_name, k.table_name AS key_table, r.table_name AS reference_table,
               lower(k.delete_rule) AS delete_rule,
               DECODE(k.deferrable, 'DEFERRABLE', lower(k.deferred)) AS defer_type
        FROM   sys.user_constraints k, sys.user_constraints r
        WHERE  k.constraint_type = 'R'
        AND    r.constraint_type IN ('P', 'U')
        AND    k.r_constraint_name = r.constraint_name
        ORDER BY k.constraint_name
    """

    PropertyList = odict(
        ('CONSTRAINT_NAME',  Property('name')),
        ('KEY_TABLE',        Property('table')),
        ('REFERENCE_TABLE',  Property('reference-table')),
        ('DELETE_RULE',      Property('delete-rule')),
        ('DEFER_TYPE',       Property('defer-type'))
    )

    @staticmethod
    def addToState(state, foreign_key):
        exists = state.tables.get
        if exists(foreign_key['table']) and exists(foreign_key['reference-table']):
            state.foreign_keys[foreign_key.name] = foreign_key

    @staticmethod
    def isAllowed(state, foreign_key):
        exists = state.tables.get
        return exists(foreign_key['table']) and exists(foreign_key['reference-table'])

    @staticmethod
    def createSQL(fk):
        sql = "ALTER TABLE %(table)s ADD CONSTRAINT %(name)s\n" \
              "FOREIGN KEY ( %(key_columns)s )\n"   \
              "REFERENCES  %(reference-table)s ( %(ref_columns)s ) %(on_delete)s %(defer_sql)s;\n\n"

        on_delete = ''
        if fk['delete-rule'] != 'no action':
            on_delete = 'ON DELETE ' + fk['delete-rule']

        defer_sql = ''
        if fk['defer-type']:
            defer_sql += " DEFERRABLE INITIALLY "+fk['defer-type']

        key_sql = ", ".join([rel.name      for rel in fk.columns.values()])
        ref_sql = ", ".join([rel.reference for rel in fk.columns.values()])

        return render(sql, fk, key_columns=key_sql, ref_columns=ref_sql,
                       on_delete=on_delete, defer_sql=defer_sql)

