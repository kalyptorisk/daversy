from daversy.utils import *
from daversy.db.object import DbObject

class OracleSynonym(DbObject):
    """ A class that represents an oracle synonym. """

class OracleSynonymBuilder(object):
    """Represents a builder for an oracle synonym."""

    DbClass = OracleSynonym
    XmlTag  = 'synonym'

    Query = """
        SELECT synonym_name, table_name AS target,
               table_owner AS schema, db_link
        FROM   sys.user_synonyms
        ORDER BY synonym_name
    """
    PropertyList = odict(
        ('SYNONYM_NAME',    Property('name')),
        ('TARGET',          Property('target')),
        ('SCHEMA',          Property('schema')),
        ('DB_LINK',         Property('db-link'))
    )

    @staticmethod
    def addToState(state, synonym):
        state.synonyms[synonym.name] = synonym

    @staticmethod
    def createSQL(syn):
        sql =  "CREATE OR REPLACE SYNONYM %(name)-30s FOR "
        if syn.schema:
            sql += "%(schema)s.%(target)s;\n"
        else:
            sql += "%(target)s@%(db-link)s;\n"

        return sql % syn
