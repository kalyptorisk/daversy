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
        AND    t.table_name NOT LIKE '%$%'
        AND    NOT EXISTS (
                  SELECT table_name
                  FROM   sys.user_external_tables
                  WHERE  table_name = t.table_name
               )
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

class OracleExternalTableLocation(DbObject):
    """ A class that represents a location for an oracle external table. """

class OracleExternalTable(DbObject):
    """ A class that represents an oracle external table. """
    SubElements = odict( ('locations',    OracleExternalTableLocation),
                         ('columns',      TableColumn)                  )

class OracleExternalTableBuilder(object):
    """Represents a builder for an external table."""

    DbClass = OracleExternalTable
    XmlTag  = 'external-table'

    Query = """
        SELECT t.table_name, t.default_directory_name AS default_dir,
               t.reject_limit, t.access_parameters AS param
        FROM   sys.user_external_tables t
        WHERE  t.table_name NOT LIKE '%$%'
        ORDER BY t.table_name
    """
    PropertyList = odict(
        ('TABLE_NAME',    Property('name')),
        ('DEFAULT_DIR',   Property('default-dir')),
        ('REJECT_LIMIT',  Property('reject-limit')),
        ('PARAM',         Property('parameters'))
    )

    @staticmethod
    def addToState(state, ext_table):
        ext_table.parameters = trim_spaces(ext_table.parameters)
        state.ext_tables[ext_table.name] = ext_table

    @staticmethod
    def createSQL(ext_table):
        sql  = "CREATE TABLE %(name)s (\n  %(col_sql)s\n)\n"
        sql += "ORGANIZATION EXTERNAL (\n  DEFAULT DIRECTORY %(default-dir)s"
        sql += "\n  ACCESS PARAMETERS (\n    %(parameters)s\n  )"
        sql += "\n  LOCATION (\n    %(loc_sql)s\n  )\n)\n"
        sql += "REJECT LIMIT %(reject-limit)s\n/\n"

        col_def, loc_def = [], []

        for col in ext_table.columns.values():
            col_def.append(TableColumnBuilder.sql(col))
        for loc in ext_table.locations.values():
            loc_def.append(OracleExternalTableLocationBuilder.sql(loc))

        col_sql = ",\n  ".join(col_def)
        loc_sql = ",\n  ".join(loc_def)

        return render(sql, ext_table, col_sql=col_sql, loc_sql=loc_sql)

class OracleExternalTableLocationBuilder(object):
    """Represents a builder for a location in an external table. """

    DbClass = OracleExternalTableLocation
    XmlTag  = 'location'

    Query = """
        SELECT l.table_name, l.directory_name AS base_dir, l.location
        FROM   sys.user_external_locations l
        WHERE  l.table_name NOT LIKE '%$%'
    """
    PropertyList = odict(
        ('TABLE_NAME',  Property('table-name', exclude=True)),
        ('LOCATION',    Property('name')),
        ('BASE_DIR',    Property('base-dir'))
    )

    @staticmethod
    def addToState(state, location):
        table = state.ext_tables.get(location['table-name'])
        if table:
            table.locations[location.name] = location

    @staticmethod
    def sql(location):
        return "%s: '%s'" % (location['base-dir'],
                             sql_escape(location['name']))
