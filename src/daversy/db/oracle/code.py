from daversy.utils import *
from daversy.db.object import DbObject, Function, StoredProcedure

class StoredProcedureBuilder(object):
    """Represents a builder for a stored procedure."""

    DbClass = StoredProcedure
    XmlTag  = 'stored-procedure'

    Query = """
        SELECT object_name,
               decode(status, 'INVALID', 'true') AS invalid,
               replace(dbms_metadata.get_ddl(object_type, object_name),
                       '"' || user || '".') AS source
        FROM   sys.user_objects
        WHERE  object_type = 'PROCEDURE'
        AND    object_name NOT LIKE '%$%'
        ORDER BY object_name
    """
    PropertyList = odict(
        ('OBJECT_NAME', Property('name')),
        ('INVALID',     Property('invalid')),
        ('SOURCE',      Property('source', None, lambda x: x.read(), cdata=True))
    )

    @staticmethod
    def addToState(state, procedure):
        procedure.source = trim_spaces(procedure.source)
        state.procedures[procedure.name] = procedure

    @staticmethod
    def createSQL(procedure):
        return procedure.source + '\n\n'

class FunctionBuilder(object):
    """Represents a builder for a database function."""

    DbClass = Function
    XmlTag  = 'function'

    Query = """
        SELECT object_name,
               decode(status, 'INVALID', 'true') AS invalid,
               replace(dbms_metadata.get_ddl(object_type, object_name),
                       '"' || user || '".') AS source
        FROM   sys.user_objects
        WHERE  object_type = 'FUNCTION'
        AND    object_name NOT LIKE '%$%'
        ORDER BY object_name
    """
    PropertyList = odict(
        ('OBJECT_NAME', Property('name')),
        ('INVALID',     Property('invalid')),
        ('SOURCE',      Property('source', None, lambda x: x.read(), cdata=True))
    )

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

class OraclePackageBuilder(object):
    """Represents a builder for an oracle package."""

    DbClass = OraclePackage
    XmlTag  = 'package'

    Query = """
        SELECT object_name,
               decode(status, 'INVALID', 'true') AS invalid,
               replace(dbms_metadata.get_ddl('PACKAGE', object_name),
                       '"' || user || '".') AS source
        FROM   sys.user_objects
        WHERE  object_type = 'PACKAGE'
        AND    object_name NOT LIKE '%$%'
        ORDER BY object_name
    """
    PropertyList = odict(
        ('OBJECT_NAME', Property('name')),
        ('INVALID',     Property('invalid')),
        ('SOURCE',      Property('source', None, lambda x: x.read(), cdata=True))
    )

    @staticmethod
    def addToState(state, package):
        package.source = trim_spaces(package.source)
        state.packages[package.name] = package

    @staticmethod
    def createSQL(package):
        return package.source + '\n\n'

class OracleObjectTypeBuilder(object):
    """Represents a builder for an oracle object type."""

    DbClass = OracleObjectType
    XmlTag  = 'type'

    Query = """
        SELECT type_name,
               replace(dbms_metadata.get_ddl('TYPE', type_name),
                       '"' || user || '".') AS source
        FROM   sys.user_types
        WHERE  type_name NOT LIKE '%$%'
        ORDER BY typecode DESC, type_name
    """
    PropertyList = odict(
        ('TYPE_NAME', Property('name')),
        ('SOURCE',    Property('source', None, lambda x: x.read(), cdata=True))
    )

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
