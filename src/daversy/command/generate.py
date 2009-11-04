import os
from optparse import make_option

from daversy.command import Command
from daversy.state   import create_filter, PROVIDERS

class Generate(Command):
    __names__   = ['generate', 'gen']
    __usage__   = ['Generate an SQL script from an existing STATE.']

    __args__    = ['STATE', 'SQL']
    __options__ = [
        make_option('-f', dest='filter',
                    help='apply a FILTER while reading the state'),
        make_option('-i', dest='include_tags', default='all', metavar='TAGS',
                    help='include objects matching specified TAGS from filter (default: "all")'),
        make_option('-x', dest='exclude_tags', default='ignore', metavar='TAGS',
                    help='exclude objects matching specified TAGS from filter (default: "ignore")'),
        make_option('-s', dest='type', choices=('create', 'comment', 'all'),
                    help='generate SQL of the specified type'),
        make_option('-c', dest='comment', default='** dvs **',
                    help='use the given check-in comment (if applicable)'),
    ]

    def execute(self, args, options):
        filters = {}
        if options.filter:
            if not os.path.exists(options.filter):
                self.parser().error('filter: unable to open for reading')
            filters = create_filter(options.filter, options.include_tags, options.exclude_tags)

        input, output = args
        # load the source state
        saved_state = None
        for provider in PROVIDERS:
            if provider.can_load(input):
                saved_state = provider.load(input, filters)
                break
        else:
            self.parser().error('state: unable to open for reading')

        # save the generated SQL
        for provider in PROVIDERS:
            if provider.can_save(output) and hasattr(provider, 'save_sql'):
                provider.save_sql(saved_state, output, options.comment,
                                  options.type)
                break
        else:
            self.parser().error('output: unable to generate SQL')
