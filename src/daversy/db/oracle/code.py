from daversy.utils import *
from daversy.db.object import DbObject, Function, StoredProcedure

class CodeBuilder(object):
    @staticmethod
    def customQuery(cursor, state, builder):
        cursor.execute("""
            SELECT name, DECODE(line, 1, '/'||chr(10)||'CREATE OR REPLACE ', '')||text
            FROM   user_source
            WHERE  type IN ('%s')
            ORDER BY name, type, line""" % "', '".join(builder.DbType))

        name = None
        text = []
        for row in cursor:
            if name != row[0]:
                if name is not None:
                    builder.getObject(state, name).source = '\n'.join(text).lstrip('\n\t/ ')+'\n/'
                name = row[0]
                text = []
            text.append(row[1].rstrip())

        if text:
            builder.getObject(state, name).source = '\n'.join(text).lstrip('\n\t/ ')+'\n/'

        cursor.close()

class StoredProcedureBuilder(CodeBuilder):
    """Represents a builder for a stored procedure."""

    DbClass = StoredProcedure
    XmlTag  = 'stored-procedure'
    DbType  = ['PROCEDURE']

    Query = """
        SELECT object_name,
               decode(status, 'INVALID', 'true') AS invalid,
               NULL AS source
        FROM   sys.user_objects
        WHERE  object_type = 'PROCEDURE'
        AND    object_name NOT LIKE '%$%'
        ORDER BY object_name
    """
    PropertyList = odict(
        ('OBJECT_NAME', Property('name')),
        ('INVALID',     Property('invalid')),
        ('SOURCE',      Property('source', cdata=True))
    )

    @staticmethod
    def getObject(state, name):
        return state.procedures[name]

    @staticmethod
    def addToState(state, procedure):
        procedure.source = trim_spaces(procedure.source)
        state.procedures[procedure.name] = procedure

    @staticmethod
    def createSQL(procedure):
        return procedure.source + '\n\n'

class FunctionBuilder(CodeBuilder):
    """Represents a builder for a database function."""

    DbClass = Function
    XmlTag  = 'function'
    DbType  = ['FUNCTION']

    Query = """
        SELECT object_name,
               decode(status, 'INVALID', 'true') AS invalid,
               NULL AS source
        FROM   sys.user_objects
        WHERE  object_type = 'FUNCTION'
        AND    object_name NOT LIKE '%$%'
        ORDER BY object_name
    """
    PropertyList = odict(
        ('OBJECT_NAME', Property('name')),
        ('INVALID',     Property('invalid')),
        ('SOURCE',      Property('source', cdata=True))
    )

    @staticmethod
    def getObject(state, name):
        return state.functions[name]

    @staticmethod
    def addToState(state, function):
        function.source = trim_spaces(function.source)
        state.functions[function.name] = function

    @staticmethod
    def createSQL(function):
        return function.source + '\n\n'

#############################################################################

class OraclePackage(DbObject):
    """ A class that represents an oracle package. """

class OracleObjectType(DbObject):
    """ A class that represents an oracle object type. """

class OracleMaterializedView(DbObject):
    """ A class that represents an oracle materialized view. """

#############################################################################

class OraclePackageBuilder(CodeBuilder):
    """Represents a builder for an oracle package."""

    DbClass = OraclePackage
    XmlTag  = 'package'
    DbType  = ['PACKAGE', 'PACKAGE BODY']

    Query = """
        SELECT object_name,
               decode(status, 'INVALID', 'true') AS invalid,
               NULL AS source
        FROM   sys.user_objects
        WHERE  object_type = 'PACKAGE'
        AND    object_name NOT LIKE '%$%'
        ORDER BY object_name
    """
    PropertyList = odict(
        ('OBJECT_NAME', Property('name')),
        ('INVALID',     Property('invalid')),
        ('SOURCE',      Property('source', cdata=True))
    )

    @staticmethod
    def getObject(state, name):
        return state.packages[name]

    @staticmethod
    def addToState(state, package):
        package.source = trim_spaces(package.source)
        state.packages[package.name] = package

    @staticmethod
    def createSQL(package):
        return package.source + '\n\n'

class OracleObjectTypeBuilder(CodeBuilder):
    """Represents a builder for an oracle object type."""

    DbClass = OracleObjectType
    XmlTag  = 'type'
    DbType  = ['TYPE', 'TYPE BODY']

    Query = """
        SELECT type_name,
               NULL AS source
        FROM   sys.user_types
        WHERE  type_name NOT LIKE '%$%'
        ORDER BY typecode DESC, type_name
    """
    PropertyList = odict(
        ('TYPE_NAME', Property('name')),
        ('SOURCE',    Property('source', cdata=True))
    )

    @staticmethod
    def getObject(state, name):
        return state.types[name]

    @staticmethod
    def addToState(state, type):
        type.source = trim_spaces(type.source)
        state.types[type.name] = type

    @staticmethod
    def createSQL(type):
        return type.source + '\n\n'


class OracleMaterializedViewBuilder(object):
    """Represents a builder for an oracle package."""

    DbClass = OracleMaterializedView
    XmlTag  = 'materialized-view'

    Query = """
        SELECT mview_name,
               decode(compile_state, 'INVALID', 'true') AS invalid,
               lower(refresh_mode) AS refresh_mode,
               lower(refresh_method) AS refresh_method,
               lower(build_mode) AS build_mode,
               query AS source
        FROM   sys.user_mviews
        WHERE  mview_name NOT LIKE '%$%'
        ORDER BY mview_name
    """
    PropertyList = odict(
        ('MVIEW_NAME',     Property('name')),
        ('INVALID',        Property('invalid')),
        ('REFRESH_MODE',   Property('refresh-mode')),
        ('REFRESH_METHOD', Property('refresh-method')),
        ('BUILD_MODE',     Property('build-mode')),
        ('SOURCE',         Property('source', cdata=True))
    )

    @staticmethod
    def addToState(state, mview):
        mview.source = trim_spaces(mview.source)
        state.mviews[mview.name] = mview

    @staticmethod
    def createSQL(mview):
        definition =  "CREATE MATERIALIZED VIEW %(name)s BUILD %(build-mode)s REFRESH %(refresh-method)s ON %(refresh-mode)s AS\n%(source)s\n/\n"
        return definition % mview
