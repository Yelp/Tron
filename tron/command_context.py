"""Command Context is how we construct the command line for a command which may
have variables that need to be rendered.
"""


class CommandContext(object):
    """A CommandContext object is a wrapper around any object which has values
    to be used to render a command for execution.  It looks up values by name.

    It's lookup order is:
        base[name],
        base.__getattr__(name),
        next[name],
        next.__getattr(name)
    """

    def __init__(self, base=None, next=None):
        """Initialize

        Args
          base   Object to look for attributes in
          next     Next place to look for more pieces of context
                    Generally this will be another instance of CommandContext
        """
        self.base = base or {}
        self.next = next or {}

    def get(self, name, default=None):
        try:
            return self.__getitem__(name)
        except KeyError:
            return default

    def __getitem__(self, name):
        for target in [self.base, self.next]:
            try:
                return target[name]
            except (KeyError, TypeError):
                pass

            try:
                return getattr(target, name)
            except AttributeError:
                pass

        raise KeyError(name)
