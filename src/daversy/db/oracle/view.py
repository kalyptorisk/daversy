from daversy.utils     import *
from daversy.db.object import ViewColumn, View
from column    import ViewColumnBuilder

class ViewBuilder(object):
    """Represents a builder for a database view."""

    DbClass = View
    XmlTag  = 'view'

    Query = """
        SELECT v.view_name, v.text, c.comments
        FROM   sys.user_views v, sys.user_tab_comments c
        WHERE  v.view_name = c.table_name
        AND    c.table_type = 'VIEW'
        ORDER BY v.view_name
    """

    PropertyList = odict(
        ('VIEW_NAME', Property('name')),
        ('TEXT',      Property('definition', cdata=True)),
        ('COMMENTS',  Property('comment'))
    )

    @staticmethod
    def addToState(state, view):
        view.definition = trim_spaces(view.definition)
        view.comment = trim_spaces(view.comment)
        state.views[view.name] = view

    @staticmethod
    def createSQL(view):
        template = "\nCREATE OR REPLACE FORCE VIEW %(name)s (" \
                   "\n  %(column_sql)s\n)\nAS\n%(definition)s\n/\n"

        column_sql = ",\n  ".join([column.name for column in view.columns.values()])

        return render(template, view, column_sql=column_sql)

    @staticmethod
    def commentSQL(view):
        template = "COMMENT ON TABLE %(name)s IS '%(comment)s';"
        result = []
        result.append( render(template, view,
                              comment = sql_escape(view.comment)) )

        for column in view.columns.values():
            result.append( ViewColumnBuilder.commentSQL(view, column) )

        return result
