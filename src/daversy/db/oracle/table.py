from daversy.utils       import *
from daversy.db.object   import TableColumn, Table, DbObject
from column      import TableColumnBuilder
from primary_key import PrimaryKeyBuilder
from unique_key  import UniqueKeyBuilder
from constraint  import CheckConstraintBuilder

YESNO_MAPPING    = {'Y':'true', 'N': 'false'}

class TableBuilder(object):
    """Represents a builder for a database table."""

    DbClass = Table
    XmlTag  = 'table'

    Query = """
        SELECT t.table_name, t.temporary, c.comments,
               decode(t.duration,
                      'SYS$SESSION',     'true',
                      'SYS$TRANSACTION', 'false') AS preserve
        FROM   sys.user_tables t, sys.user_tab_comments c
        WHERE  t.table_name = c.table_name
        AND    c.table_type = 'TABLE'
        ORDER BY t.table_name
    """
    PropertyList = odict(
        ('TABLE_NAME',  Property('name')),
        ('TEMPORARY',   Property('temporary', 'false', lambda flag: YESNO_MAPPING[flag])),
        ('COMMENTS',    Property('comment')),
        ('PRESERVE',    Property('on-commit-preserve-rows'))
    )

    @staticmethod
    def addToState(state, table):
        table.comment = trim_spaces(table.comment)
        state.tables[table.name] = table

    @staticmethod
    def createSQL(table):
        sql = "CREATE %(temp1)sTABLE %(name)s (\n  %(table_sql)s\n)\n%(temp2)s/\n"

        definition = []
        for col in table.columns.values():
            definition.append(TableColumnBuilder.sql(col))
        for key in table.primary_keys.values():
            definition.append(PrimaryKeyBuilder.sql(key))
        for key in table.unique_keys.values():
            definition.append(UniqueKeyBuilder.sql(key))
        for constraint in table.constraints.values():
            definition.append(CheckConstraintBuilder.sql(constraint))

        table_sql = ",\n  ".join(definition)
        t1, t2 = '', ''
        if table.temporary == 'true':
          t1 = 'GLOBAL TEMPORARY '
          if table.get('on-commit-preserve-rows') == 'true':
            t2 = 'ON COMMIT PRESERVE ROWS\n'
          else:
            t2 = 'ON COMMIT DELETE ROWS\n'

        return render(sql, table, temp1=t1, temp2=t2, table_sql=table_sql)

    @staticmethod
    def commentSQL(table):
        comments = []
        comments.append("COMMENT ON TABLE %s IS '%s';" % (table.name,
                                                            sql_escape(table.comment)))
        for column in table.columns.values():
            comments.append(TableColumnBuilder.commentSQL(table, column))

        return comments
