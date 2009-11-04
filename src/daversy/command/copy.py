import os
from optparse import make_option

from daversy.command import Command
from daversy.state   import create_filter, PROVIDERS
from daversy.utils   import get_uuid4

class Copy(Command):
    __names__   = ['copy', 'cp']
    __usage__   = ['Copy the state from SOURCE to TARGET.']

    __args__    = ['SOURCE', 'TARGET']
    __options__ = [
        make_option('-f', dest='filter',
                    help='apply a FILTER while reading the state'),
        make_option('-i', dest='include_tags', default='all', metavar='TAGS',
                    help='include objects matching specified TAGS from filter (default: "all")'),
        make_option('-x', dest='exclude_tags', default='ignore', metavar='TAGS',
                    help='exclude objects matching specified TAGS from filter (default: "ignore")'),
        make_option('-n', dest='name',
                    help='rename the target state to specified NAME.'),
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
            self.parser().error('source: unable to open for reading')

        # (re)name it if applicable
        if options.name:
            saved_state.name = options.name
        elif saved_state.setdefault('name') is None:
            saved_state.name = get_uuid4()

        # save it to target state
        for provider in PROVIDERS:
            if provider.can_save(output):
                provider.save(saved_state, output, options.comment)
                break
        else:
            self.parser().error('target: unable to open for writing')
