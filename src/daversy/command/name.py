from daversy.command import Command
from daversy.state   import PROVIDERS

class Name(Command):
    __names__   = ['name', 'nm']
    __usage__   = ['Display the name of the given STATE.']

    __args__    = ['STATE']
    __options__ = []

    def execute(self, args, options):
        input = args[0]
        saved_state = None
        for provider in PROVIDERS:
            if provider.can_load(input):
                saved_state = provider.load(input, {})
                break

        if not saved_state:
            self.parser().error('STATE: could not open for reading')

        print saved_state.name
