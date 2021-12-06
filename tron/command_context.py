"""Command Context is how we construct the command line for a command which may
have variables that need to be rendered.
"""
import operator
import re
from functools import reduce

from tron.utils import timeutils


def build_context(object, parent):
    """Construct a CommandContext for object. object must have a property
    'context_class'.
    """
    return CommandContext(object.context_class(object), parent)


def build_filled_context(*context_objects):
    """Create a CommandContext chain from context_objects, using a Filler
    object to pass to each CommandContext. Can be used to validate a format
    string.
    """
    if not context_objects:
        return CommandContext()

    filler = Filler()

    def build(current, next):
        return CommandContext(next(filler), current)

    return reduce(build, context_objects, None)


class CommandContext:
    """A CommandContext object is a wrapper around any object which has values
    to be used to render a command for execution.  It looks up values by name.

    It's lookup order is:
        base[name],
        base.__getattr__(name),
        next[name],
        next.__getattr__(name)
    """

    def __init__(self, base=None, next=None):
        """
          base - Object to look for attributes in
          next - Next place to look for more pieces of context
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
        getters = [operator.itemgetter(name), operator.attrgetter(name)]
        for target in [self.base, self.next]:
            for getter in getters:
                try:
                    return getter(target)
                except (KeyError, TypeError, AttributeError):
                    pass

        raise KeyError(name)

    def __eq__(self, other):
        return self.base == other.base and self.next == other.next

    def __ne__(self, other):
        return not self == other


class JobContext:
    """A class which exposes properties for rendering commands."""

    def __init__(self, job):
        self.job = job

    @property
    def name(self):
        return self.job.name

    def __getitem__(self, item):
        date_name, date_spec = self._get_date_spec_parts(item)
        if not date_spec:
            raise KeyError(item)

        if date_name == "last_success":
            last_success = self.job.runs.last_success
            last_success = last_success.run_time if last_success else None

            time_value = timeutils.DateArithmetic.parse(date_spec, last_success,)
            if time_value:
                return time_value

        raise KeyError(item)

    def _get_date_spec_parts(self, name):
        parts = name.rsplit("#", 1)
        if len(parts) != 2:
            return name, None
        return parts

    @property
    def namespace(self):
        return self.job.name.split(".")[0]


class JobRunContext:
    def __init__(self, job_run):
        self.job_run = job_run

    @property
    def runid(self):
        return self.job_run.id

    @property
    def manual(self):
        return str(self.job_run.manual).lower()

    @property
    def cleanup_job_status(self):
        """Provide 'SUCCESS' or 'FAILURE' to a cleanup action context based on
        the status of the other steps
        """
        if self.job_run.action_runs.is_failed:
            return "FAILURE"
        elif self.job_run.action_runs.is_complete_without_cleanup:
            return "SUCCESS"
        return "UNKNOWN"

    def __getitem__(self, name):
        """
        This function attempts to parse any command context variable expressions
        that use shortdate or runid in the following order:
        1) Attempt to parse date arithmetic syntax and apply to run_time unconditionally
           and, if unsuccessful falls to the next case
        2) Attempts to parse a delta to apply to the current job runid - this is mostly
           meant to be used for jobs that rely on the output of the previous run, but
           this is not enforced in case someone can dream up another scenario where they
           want to do arbitrary deltas here.
        """
        run_time = self.job_run.run_time
        time_value = timeutils.DateArithmetic.parse(name, run_time)
        if time_value:
            return time_value

        # this is a little weird, but enumerating the cases that should be parsed by timeutils is hard,
        # so we just unconditionally attempt to parse the name and then fallback to the runid special cases
        # rather than attempt to enumerate the timeutils cases
        elif name == "runid":
            # we could expand the logic below to handle this with the regex, but that
            # would make the code a little more complex for not much gain
            return self.runid
        elif "runid" in name:
            # we're really only expecting runid-1 for now but, as described in the docstring,
            # we're allowing arbitrary addition/subtration in case someone dreams up a use for
            # them
            match = re.match(r"^runid([+-]\d+)$", name)
            if match:
                # self.runid here will be the job runid (e.g., NAMESPACE.SERVICE.RUN_NUMBER) - it will not
                # include an action name.
                # that said - all we need math-wise here is the run number, so we split on . and store the job name
                # so that we can re-consistitute the runid after doing math on the run number
                job_name, run_num = self.runid.rsplit(".", maxsplit=1)
                # NOTE: this will potentially return a runid for a job that will never exist - e.g., if you setup an
                # action that should only run after the previous jobrun's action has run for a job that has never run
                # before) - normally this will only be a problem for the very first run and users can easily tronctl start
                # the action to bootstrap things so we don't do any checking to see if the returned runid is valid
                return f"{job_name}.{int(run_num) + int(match.groups()[0])}"

        raise KeyError(name)


class ActionRunContext:
    """Context object that gives us access to data about the action run."""

    def __init__(self, action_run):
        self.action_run = action_run

    @property
    def actionname(self):
        return self.action_run.action_name

    @property
    def node(self):
        return self.action_run.node.hostname


class Filler:
    """Filler object for using CommandContext during config parsing. This class
    is used as a substitute for objects that would be passed to Context objects.
    This allows the Context objects to be used directly for config validation.
    """

    def __getattr__(self, _):
        return self

    def __str__(self):
        return "%(...)s"

    def __mod__(self, _):
        return self

    def __nonzero__(self):
        return False

    def __bool__(self):
        return False
