from daversy.utils      import *
from daversy.db.object  import CheckConstraint
import re

COL_NOT_NULL = re.compile(r'^\s*("?)([\w$#]+)\1\s+IS\s+NOT\s+NULL\s*$', re.I)
SYSTEM_NAME  = re.compile(r'^SYS_C\d+$')

class CheckConstraintBuilder(object):
    """ Represents a builder for a check constraint. """

    DbClass = CheckConstraint
    XmlTag  = 'check-constraint'

    Query = """
        SELECT c.constraint_name, c.search_condition AS condition, c.table_name
        FROM   sys.user_constraints c
        WHERE  c.constraint_type = 'C'
        AND    c.constraint_name not like '%$%'
        ORDER BY c.table_name, c.constraint_name
    """

    PropertyList = odict(
        ('CONSTRAINT_NAME', Property('name')),
        ('CONDITION',       Property('condition')),
        ('TABLE_NAME',      Property('table-name', exclude=True))
    )

    @staticmethod
    def addToState(state, constraint):
        table = state.tables.get(constraint['table-name'])
        if table:
            # check that it does not match a system generated NOT NULL constraint
            if SYSTEM_NAME.match(constraint.name):
                match = COL_NOT_NULL.match(constraint.condition)
                if match:
                    column = table.columns.get( match.group(2) )
                    if column and column.nullable == 'false':
                        return
            table.constraints[constraint.name] = constraint

    @staticmethod
    def sql(constraint):
        return "CONSTRAINT %(name)s CHECK ( %(condition)s )" % constraint
