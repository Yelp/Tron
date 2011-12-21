"""Command Context is how we construct the command line for a command which may
have variables that need to be rendered.
"""


class CommandContext(object):

    def __init__(self, base, next=None):
        """Initialize

        Args
          base   Object to look for attributes in
          next     Next place to look for more pieces of context
                    Generally this will be another instance of CommandContext
        """
        self.base = base
        self.next = next or dict()

    def get(self, name, default=None):
        try:
            return self.__getitem__(name)
        except KeyError:
            return default

    def __getitem__(self, name):
        try:
            return self.base[name]
        except (KeyError, TypeError):
            pass

        try:
            return getattr(self.base, name)
        except AttributeError:
            pass

        try:
            return self.next[name]
        except (TypeError, KeyError):
            pass

        try:
            return getattr(self.next, name)
        except AttributeError:
            pass

        raise KeyError(name)
