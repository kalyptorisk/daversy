import os, sys

from optparse import make_option

from daversy.command import Command
from daversy.state   import create_filter, PROVIDERS
from daversy         import difflib_ext
from daversy.db      import Database

class Compare(Command):
    __names__   = ['compare', 'diff']
    __usage__   = ['Compare the SOURCE state with the TARGET state.']

    __args__    = ['SOURCE', 'TARGET']
    __options__ = [
        make_option('-f', dest='filter',
                    help='apply a FILTER while reading the state'),
        make_option('-i', dest='include_tags', default='all', metavar='TAGS',
                    help='include objects matching specified TAGS from filter (default: "all")'),
        make_option('-x', dest='exclude_tags', default='ignore', metavar='TAGS',
                    help='exclude objects matching specified TAGS from filter (default: "ignore")'),
        make_option('--html', dest='html', action='store_true',
                    help='generate a HTML inline difference report'),
        make_option('--context', dest='lines', default=None, type='int',
                    help='number of context lines (default: all lines included).')
    ]

    def execute(self, args, options):
        filters = {}
        if options.filter:
            if not os.path.exists(options.filter):
                self.parser().error('filter: unable to open for reading')
            filters = create_filter(options.filter, options.include_tags, options.exclude_tags)

        source_location, target_location = args
        source = target = None

        # load the source state
        for provider in PROVIDERS:
            if provider.can_load(source_location):
                source = provider.load(source_location, filters)
                break
        else:
            self.parser().error('source: unable to open for reading')

        # load the target state
        for provider in PROVIDERS:
            if provider.can_load(target_location):
                target = provider.load(target_location, filters)
                break
        else:
            self.parser().error('target: unable to open for reading')

        if not source.__class__ == target.__class__:
            self.parser().error('source and target represent different databases')

        source.setdefault('name')
        target.setdefault('name')

        self.source_version, self.target_version = source.name, target.name
        source.name = target.name = None
        if source == target:
            return

        for db in Database.list():
            if db.__state__ == source.__class__:
                self.db = db

        self.builders = {}
        self.tags = {}
        for builder in self.db.__builders__:
            self.builders[builder.DbClass] = builder
            self.tags[builder.DbClass] = builder.XmlTag

        self.diff = []
        self.compute_diff([], source, target)
        if not options.html:
            self.print_simple_diff()
        else:
            self.print_html_diff(source, target, options.lines)

    def compute_diff(self, location, source, target):
        builder = self.builders[source.__class__]

        if source == target:
            return

        if hasattr(source, 'SubElements'):
            for key in source.SubElements.keys():
                src, tgt = source[key], target[key]
                if not src == tgt:
                    src_names = [elem.name for elem in src.values()]
                    tgt_names = [elem.name for elem in tgt.values()]
                    common    = []
                    for elem in src.values():
                        if not elem.name in tgt_names:
                            path = location[:]
                            path.append( (elem, None, elem.name) )
                            self.diff.append( ('D', path) )
                        elif common.count(elem.name) == 0:
                            common.append(elem.name)
                    for elem in tgt.values():
                        if not elem.name in src_names:
                            path = location[:]
                            path.append( (None, elem, elem.name) )
                            self.diff.append( ('A', path) )
                        elif common.count(elem.name) == 0:
                            common.append(elem.name)
                    for name in common:
                        path = location[:]
                        path.append( (src[name], tgt[name], name) )
                        self.compute_diff(path, src[name], tgt[name])

        if hasattr(builder, 'PropertyList'):
            for prop in builder.PropertyList.values():
                if not prop.exclude:
                    if source[prop.name] != target[prop.name]:
                        path = location[:]
                        path.append( (None, None, prop.name) )
                        self.diff.append( ('M', path) )

    def print_simple_diff(self):
        for op, path in self.diff:
            sys.stdout.write('%s' % op)
            for src, target, name in path:
                if src is None and target is None:
                    sys.stdout.write(' @%s' % name)
                else:
                    elem = src or target
                    sys.stdout.write(' %s[%s]' % (self.tags[elem.__class__], name))
            sys.stdout.write('\n')

    def print_html_diff(self, source, target, context):
        get_id = lambda x: 'ref__%s__%s' % (x.__class__.__name__, x.name)
        get_link = lambda type, x: '<li><div class="%s"></div>%s <a href="#%s">%s</a></li>' % \
                                   (type, x.__class__.__name__, get_id(x), x.name)

        seen  = {}
        diffs = []
        headers = []
        for op, path in self.diff:
            src, target, name = path[0]
            if len(path) == 1 and op == 'A':
                table = difflib_ext.make_table(get_id(target), name, [],
                                               self.get_sql(target), context)
                if table:
                    headers.append( get_link('add', target) )
                    diffs.append(table)
            elif len(path) == 1 and op == 'D':
                table = difflib_ext.make_table(get_id(src), name,
                                               self.get_sql(src), [], context)
                if table:
                    headers.append( get_link('rem', src) )
                    diffs.append(table)
            elif not seen.has_key( (src.__class__, name) ):
                table = difflib_ext.make_table(get_id(target), name,
                                               self.get_sql(src), self.get_sql(target),
                                               context)
                seen [ (src.__class__, name) ] = True
                if table:
                    headers.append( get_link('mod', target) )
                    diffs.append(table)


        output = difflib_ext.HTML_HEADER
        output += '<dl id="overview">'
        output += '<dt>Source:</dt><dd><span class="ver">%s</span></dd></dt>' % self.source_version
        output += '<dt>Target:</dt><dd><span class="ver">%s</span></dd></dt>' % self.target_version
        output += '<dt class="files">Changes:</dt><dd class="files"><ul>' + ''.join(headers) + '</ul></dd>'
        output += '</dl><div class="diff"><ul class="entries">' + ''.join(diffs) + '</ul></div>'
        output += difflib_ext.HTML_FOOTER
        print output.encode('utf-8')

    def get_sql(self, elem):
        builder = self.builders[elem.__class__]
        sql = builder.createSQL(elem).splitlines()
        if hasattr(builder, 'commentSQL'):
            sql.extend( builder.commentSQL(elem) )
        return sql
