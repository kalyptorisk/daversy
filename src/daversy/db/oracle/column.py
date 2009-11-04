from daversy.utils import *
from daversy.db.object import TableColumn, ViewColumn

YESNO_MAPPING    = {'Y':'true', 'N': 'false'}

def correctTimestamps(type):
    """ Timestamps are returned as 'timestamp(6)', while we want it as 'timestamp'"""
    return type.startswith('timestamp') and 'timestamp' or type

class TableColumnBuilder(object):
    """ Represents a builder for a column in a table."""

    DbClass = TableColumn
    XmlTag  = 'column'

    PropertyList = odict(
        ('COLUMN_NAME',     Property('name')),
        ('DATA_TYPE',       Property('type', None, correctTimestamps)),
        ('CUSTOM_TYPE',     Property('custom-type')),
        ('DATA_LENGTH',     Property('length')),
        ('DATA_PRECISION',  Property('precision')),
        ('DATA_SCALE',      Property('scale')),
        ('NULLABLE',        Property('nullable', 'true', lambda flag: YESNO_MAPPING[flag])),
        ('DATA_DEFAULT',    Property('default-value')),
        ('COMMENTS',        Property('comment')),
        ('PARENT_NAME',     Property('parent-name', exclude=True)),
        ('COLUMN_ID',       Property('sequence',   exclude=True))
    )

    Query = """
        SELECT tc.column_name, tc.table_name AS parent_name, tc.column_id,
               nvl2(tc.data_type_owner, null, lower(tc.data_type)) AS data_type,
               nvl2(tc.data_type_owner, tc.data_type, null) AS custom_type,
               nvl(tc.char_col_decl_length, tc.data_length) AS data_length,
               tc.data_precision, tc.data_scale, tc.nullable, tc.data_default,
               c.comments
        FROM   sys.user_tab_columns tc, sys.user_col_comments c
        WHERE  tc.table_name  = c.table_name
        AND    tc.column_name = c.column_name
        AND    tc.data_type_owner IS NULL
        AND EXISTS (SELECT table_name FROM sys.user_tables
                    WHERE  table_name = tc.table_name)
        ORDER BY tc.table_name, tc.column_id
    """

    @staticmethod
    def addToState(state, column):
        custom_type = column['custom-type']
        if custom_type and not state.types.get(custom_type):
          raise LookupError('Cannot find the type %s' % custom_type)

        table = state.tables.get(column['parent-name'])
        if table:
            column.comment = trim_spaces(column.comment)
            table.columns[column.name] = column
        elif state.ext_tables.get(column['parent-name']):
            ext_table = state.ext_tables.get(column['parent-name'])
            ext_table.columns[column.name] = column

    @staticmethod
    def sql(column):
        if column['custom-type']:
          return "%(name)-30s %(custom-type)s" % column

        definition = "%(name)-30s %(type)s"

        if column.type in ('number', 'float'):
            if not column.precision and column.scale:
                # this could be an integer...
                raise TypeError("Can't specify scale without precision for a column.")
            elif column.precision and not column.scale:
                definition += '(%(precision)s)'
            elif column.precision and column.scale:
                definition += '(%(precision)s, %(scale)s)'
        elif column.type in ('varchar2', 'nvarchar2', 'char'):
            if int(column.length) < 1:
                raise TypeError("%s can't be of zero length." % (type, ))
            definition += '(%(length)s)'

        result = definition % column

        if column['default-value']:
            result += ' default %s' % (column['default-value'].strip())
        result += column.nullable == 'false' and ' not null' or ''
        return result

    @staticmethod
    def commentSQL(parent, col):
        return "COMMENT ON COLUMN %s.%s IS '%s';" % (parent.name, col.name,
                                                       sql_escape(col.comment))


class ViewColumnBuilder(object):
    """ Represents a builder for a column in a view."""

    DbClass = ViewColumn
    XmlTag  = 'column'

    PropertyList = odict(
        ('COLUMN_NAME',     Property('name')),
        ('COMMENTS',        Property('comment')),
        ('PARENT_NAME',     Property('parent-name', exclude=True)),
        ('COLUMN_ID',       Property('sequence',   exclude=True))
    )


    Query = """
        SELECT vc.column_name, vc.table_name AS parent_name, vc.column_id,
               c.comments
        FROM   sys.user_tab_columns vc, sys.user_col_comments c
        WHERE  vc.table_name  = c.table_name
        AND    vc.column_name = c.column_name
        AND EXISTS (SELECT view_name FROM sys.user_views
                    WHERE  view_name = vc.table_name)
        ORDER BY vc.table_name, vc.column_id
    """

    @staticmethod
    def addToState(state, column):
        view = state.views.get(column['parent-name'])
        if view:
            column.comment = trim_spaces(column.comment)
            view.columns[column.name] = column

    @staticmethod
    def commentSQL(parent, col):
        return "COMMENT ON COLUMN %s.%s IS '%s';" % (parent.name, col.name,
                                                       sql_escape(col.comment))

