from daversy.utils import *
from daversy.db.object import Trigger

class TriggerBuilder(object):
    """Represents a builder for a trigger."""

    DbClass = Trigger
    XmlTag  = 'trigger'

    Query = """
        SELECT trigger_name, table_name, lower(base_object_type) AS type,
               replace(dbms_metadata.get_ddl('TRIGGER', trigger_name),
                       '"' || user || '".') AS definition
        FROM   sys.user_triggers
        WHERE  trigger_name NOT LIKE 'BIN$%'
        ORDER BY trigger_name
    """
    PropertyList = odict(
        ('TRIGGER_NAME', Property('name')),
        ('TYPE',         Property('object-type')),
        ('TABLE_NAME',   Property('object-name')),
        ('DEFINITION',   Property('definition', None, lambda x: x.read(), cdata=True))
    )

    @staticmethod
    def addToState(state, trigger):
        trigger.definition = trim_spaces(trigger.definition)
        state.triggers[trigger.name] = trigger

    @staticmethod
    def createSQL(trigger):
        return trigger.definition + '\n\n'
