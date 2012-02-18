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
        SELECT t.table_name, t.temporary, NVL2(t.iot_type, 'Y', 'N') AS iot, c.comments,
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
        ('IOT',         Property('iot',       'false', lambda flag: YESNO_MAPPING[flag])),
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
        elif table.iot == 'true':
            t2 = 'ORGANIZATION INDEX'
            pk = table.primary_keys.values()[0]
            if pk.compress:
                t2 += ' COMPRESS '+pk.compress
            t2 += '\n'
        else:
            for key in table.primary_keys.values():
                if key.compress:
                    t2 += '/\nALTER INDEX %(name)s REBUILD COMPRESS %(compress)s\n' % key
            for key in table.unique_keys.values():
                if key.compress:
                    t2 += '/\nALTER INDEX %(name)s REBUILD COMPRESS %(compress)s\n' % key

        return render(sql, table, temp1=t1, temp2=t2, table_sql=table_sql)

    @staticmethod
    def commentSQL(table):
        template = "COMMENT ON TABLE %(name)s IS '%(comment)s';"
        result = []
        if table.comment:
            result.append(render(template, table, 
                                 comment = sql_escape(table.comment)))
        
        for column in table.columns.values():
            col_comment = TableColumnBuilder.commentSQL(table, column)
            if col_comment:
                result.append(col_comment)

        return result
