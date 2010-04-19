from daversy.utils      import *
from daversy.db.object  import CheckConstraint
import re

COL_NOT_NULL = re.compile(r'^\s*("?)([\w$#]+)\1\s+IS\s+NOT\s+NULL\s*$', re.I)

class CheckConstraintBuilder(object):
    """ Represents a builder for a check constraint. """

    DbClass = CheckConstraint
    XmlTag  = 'check-constraint'

    Query = """
        WITH cons_cols AS (
            SELECT owner, constraint_name,
                   DECODE(COUNT(column_name), 1, MIN(column_name)) AS column_name
            FROM   sys.user_cons_columns
            GROUP BY owner, constraint_name
        )
        SELECT c.constraint_name, c.search_condition AS condition, c.table_name,
               DECODE(c.deferrable, 'DEFERRABLE', lower(c.deferred)) AS defer_type,
               DECODE(c.generated, 'GENERATED NAME', l.column_name) AS column_name
        FROM   sys.user_constraints c, cons_cols l
        WHERE  c.constraint_type = 'C'
        AND    c.owner = l.owner AND c.constraint_name = l.constraint_name
        ORDER BY c.table_name, c.constraint_name
    """

    PropertyList = odict(
        ('CONSTRAINT_NAME', Property('name')),
        ('DEFER_TYPE',      Property('defer-type')),
        ('CONDITION',       Property('condition', cdata=True)),
        ('TABLE_NAME',      Property('table-name', exclude=True)),
        ('COLUMN_NAME',     Property('column', exclude=True))
    )

    @staticmethod
    def addToState(state, constraint):
        table = state.tables.get(constraint['table-name'])
        if table:
            # check if it is a column specific unnamed constraint
            if constraint.column:
                column = table.columns.get(constraint.column)
                match  = COL_NOT_NULL.match(constraint.condition)
                if match and column.name == match.group(2):
                    # in case it is deferred, the view incorrect reports
                    # it as nullable, so need to fix the definition
                    if constraint.get('defer-type'):
                        column['nullable'] = 'false'
                        column['notnull-defer-type'] = constraint['defer-type']
                        return
                    elif column.nullable == 'false':
                        return
                else:
                    column['check']            = constraint.get('condition' )
                    column['check-defer-type'] = constraint.get('defer-type')
                    return

            # it's not an unnamed column-specific constraint
            table.constraints[constraint.name] = constraint

    @staticmethod
    def sql(constraint):
        if not constraint['defer-type']:
            return "CONSTRAINT %(name)s CHECK ( %(condition)s )" % constraint

        return "CONSTRAINT %(name)s CHECK ( %(condition)s ) DEFERRABLE INITIALLY %(defer-type)s" % constraint
