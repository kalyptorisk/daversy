from daversy.db        import Database, Builder
from daversy.db.object import *
from daversy.utils     import *

from table       import TableBuilder
from column      import TableColumnBuilder, ViewColumnBuilder
from primary_key import PrimaryKeyBuilder, PrimaryKeyColumnBuilder
from unique_key  import UniqueKeyBuilder, UniqueKeyColumnBuilder
from constraint  import CheckConstraintBuilder
from sequence    import SequenceBuilder
from index       import IndexBuilder, IndexColumnBuilder
from foreign_key import ForeignKeyBuilder, ForeignKeyColumnBuilder
from view        import ViewBuilder
from code        import StoredProcedureBuilder, FunctionBuilder
from code        import OraclePackage, OraclePackageBuilder
from code        import OracleObjectType, OracleObjectTypeBuilder
from code        import OracleMaterializedView, OracleMaterializedViewBuilder
from trigger     import TriggerBuilder

import state_xsd, connection


class OracleBuilder(Builder):
    pass

class OracleState(DbObject):
    """This represents the elements that are available in an oracle database."""
    SubElements = odict( ('types',        OracleObjectType),
                         ('tables',       Table),
                         ('sequences',    Sequence),
                         ('indexes',      Index),
                         ('foreign_keys', ForeignKey),
                         ('views',        View),
                         ('procedures',   StoredProcedure),
                         ('functions',    Function),
                         ('packages',     OraclePackage),
                         ('triggers',     Trigger ),
                         ('mviews',       OracleMaterializedView)
                        )

class OracleStateBuilder(OracleBuilder):
    DbClass = OracleState
    XmlTag  = 'dvs-state'

    PropertyList = odict(
        ('NAME',  Property('name'))
    )

class OracleDatabase(Database):
    __adapter__  = 'oracle'
    __xmlns__    = 'http://www.daversy.org/schemas/state/oracle'
    __xsd__      = state_xsd.schema
    __conn__     = connection.OracleConnection
    __state__    = OracleState
    __builders__ = [OracleStateBuilder(),
                    OracleObjectTypeBuilder(),
                    TableBuilder(),
                    TableColumnBuilder(),
                    PrimaryKeyBuilder(),
                    PrimaryKeyColumnBuilder(),
                    UniqueKeyBuilder(),
                    UniqueKeyColumnBuilder(),
                    CheckConstraintBuilder(),
                    SequenceBuilder(),
                    IndexBuilder(),
                    IndexColumnBuilder(),
                    ForeignKeyBuilder(),
                    ForeignKeyColumnBuilder(),
                    OracleMaterializedViewBuilder(),
                    ViewBuilder(),
                    ViewColumnBuilder(),
                    StoredProcedureBuilder(),
                    FunctionBuilder(),
                    OraclePackageBuilder(),
                    TriggerBuilder()
                   ]
