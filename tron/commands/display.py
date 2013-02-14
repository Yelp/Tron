"""
Format and color output for tron commands.
"""
from functools import partial
from operator import itemgetter
import os


class Color(object):

    enabled = None
    colors = {
        'gray':                 '\033[90m',
        'red':                  '\033[91m',
        'green':                '\033[92m',
        'yellow':               '\033[93m',
        'blue':                 '\033[94m',
        'purple':               '\033[95m',
        'cyan':                 '\033[96m',
        'white':                '\033[99m',
        # h is for highlighted
        'hgray':                '\033[100m',
        'hred':                 '\033[101m',
        'hgreen':               '\033[102m',
        'hyellow':              '\033[103m',
        'hblue':                '\033[104m',
        'hcyan':                '\033[106m',
        'end':                  '\033[0m',
    }

    @classmethod
    def set(cls, color_name, text):
        if not cls.enabled or not color_name:
            return unicode(text)
        return cls.colors[color_name.lower()] + unicode(text) + cls.colors['end']


class TableDisplay(object):
    """Base class for displaying columns of data.  This class takes a list
    of dict objects and formats it so that it displays properly in fixed width
    columns.  Overlap is truncated.

    This class provides many hooks for customizing the output, including:
        - sorting of rows
        - building composite values from more then one field
        - custom formatting of a columns values
        - adding additional data after each row
        - coloring of header, columns, or rows

    The default output is:

        Banner
        Header
        Row
        (optional post row)
        Row
        (optional post row)
        ...
        Footer
    """

    columns = None
    fields = None
    widths = None
    colors = None
    title = None

    header_color = 'hgray'

    @property
    def max_first_col_width(self):
        return max(self.num_cols - sum(self.widths[1:]), 10)

    def __init__(self, options=None):
        self.out = []
        self.options = options
        self.num_cols = self.console_width()

    def console_width(self):
        console_sizes = os.popen('stty size', 'r').read().split()
        if not console_sizes or len(console_sizes) != 2:
            return 80
        return int(console_sizes[1])

    def banner(self):
        if not self.title:
            return
        title = self.title.capitalize()
        self.out.append("\n%s:" % title)
        if not self.rows():
            self.out.append("No %s\n" % title)

    def header(self):
        row = [
            self.format_header(i, label)
            for i, label in enumerate(self.columns)
        ]
        self.out.append(
            Color.set(self.header_color, "".join(row))
        )

    def footer(self):
        pass

    def color(self, col, field):
        return None

    def sorted_fields(self, values):
        return [values[name] for name in self.fields]

    def format_row(self, fields):
        row = []
        for i, value in enumerate(self.sorted_fields(fields)):
            row.append(
                Color.set(self.color(i, value), self.format_value(i, value))
            )
        return Color.set(self.row_color(fields), "".join(row))

    def format_value(self, field, value):
        length = self.widths[field]
        value = unicode(value)
        if len(value) > length:
            return (value[:length - 3] + '...').ljust(length)
        return value.ljust(length)

    format_header = format_value

    def calculate_col_widths(self):
        padding = 3
        label_width = len(self.columns[0]) + 1
        if not self.data:
            max_name_width = 1
        else:
            max_name_width = max(len(item[self.fields[0]]) for item in self.data)
            max_name_width += padding
        self.widths[0] = min(
            self.max_first_col_width,
            max(label_width, max_name_width)
        )

    def output(self):
        out = "\n".join(self.out)
        self.out = []
        return out

    def post_row(self, row):
        pass

    def row_color(self, row):
        return None

    def rows(self):
        return sorted(self.data, key=itemgetter(self.fields[0]))

    def store_data(self, data):
        self.data = data

    def format(self, data):
        self.store_data(data)
        self.banner()

        self.calculate_col_widths()
        if not self.rows():
            return self.output()

        self.header()
        for row in self.rows():
            self.out.append(self.format_row(row))
            self.post_row(row)

        self.footer()
        return self.output()


def add_color_for_state(state):
    if state.upper() in ('FAILED', 'FAIL'):
        return Color.set('red', state)
    if state.upper() in ('UP', 'ENABLED', 'RUNNING', 'SUCC'):
        return Color.set('green', state)
    if state.upper() in ('DISABLED', 'DOWN'):
        return Color.set('blue', state)
    return state


def format_fields(display_obj, content):
    """Format fields with some color."""
    def add_color(field, field_value):
        if field not in display_obj.colors:
            return field_value
        return display_obj.colors[field](field_value)

    def build_field(label, field):
        field_value = '' if content[field] is None else content[field]
        return "%-20s: %s" % (label, add_color(field, field_value))

    return "\n".join(build_field(*item) for item in display_obj.detail_labels)


def format_service_details(service_content):
    """Format details about a service."""

    def format_instances(service_instances):
        format_str = "    %s : %-30s %s%s"
        def get_failure_messages(failures):
            if not failures:
                return ""
            header = Color.set("red", "\n    stderr: ")
            return header + Color.set("red", "\n".join(failures))

        def format(inst):
            state = add_color_for_state(inst['state'])
            failures = get_failure_messages(inst['failures'])
            return format_str % (inst['id'], inst['node'], state, failures)
        return [format(instance) for instance in service_instances]

    details     = format_fields(DisplayServices, service_content)
    instances   = format_instances(service_content['instances'])
    return details + '\n\nInstances:\n' + '\n'.join(instances)


def format_job_details(job_content, options):
    details = format_fields(DisplayJobs, job_content)
    job_runs = DisplayJobRuns(options).format(job_content['runs'])
    actions = "\n\nList of Actions:\n%s" % '\n'.join(job_content['action_names'])
    return details + actions + "\n" + job_runs


def format_action_run_details(content, stdout=True, stderr=True):
    out = ["Requirements:"] + content['requirements'] + ['']
    if stdout:
        out.append("Stdout:\n%s\n" % '\n'.join(content['stdout']))

    if stderr:
        out.append("Stderr:\n%s\n" % '\n'.join(content['stderr']))

    details = format_fields(DisplayActionRuns, content)
    return details + '\n' + '\n'.join(out)


class DisplayServices(TableDisplay):

    columns = ['Name',  'State',    'Count'      ]
    fields  = ['name',  'state',    'live_count' ]
    widths  = [None,    12,          5           ]
    title   = 'services'

    detail_labels = [
        ('Service',             'name'              ),
        ('Enabled',             'enabled'           ),
        ('State',               'state'             ),
        ('Max instances',       'count'             ),
        ('Command',             'command'           ),
        ('Node Pool',           'node_pool'         ),
        ('Restart interval',    'restart_interval'  ),
        ('Monitor interval',    'monitor_interval'  ),
    ]

    colors = {
        'name':     partial(Color.set, 'yellow'),
        'state':    add_color_for_state
    }


class DisplayJobRuns(TableDisplay):
    """Format Job runs."""

    columns = ['Run ID', 'State',    'Node', 'Scheduled Time']
    fields  = ['id',     'state',    'node', 'scheduled_time']
    widths  = [None,     6,          20,     25              ]
    title = 'job runs'

    detail_labels = [
        ('Job Run',             'id'),
        ('State',               'state'),
        ('Node',                'node'),
        ('Scheduled time',      'run_time'),
        ('Start time',          'start_time'),
        ('End time',            'end_time'),
        ('Manual run',          'manual'),
    ]

    colors = {
        'id':        partial(Color.set, 'yellow'),
        'state':     add_color_for_state,
        'manual':    lambda value: Color.set('cyan' if value else None, value),
    }

    def rows(self):
        data_rows = self.data
        if self.options.warn:
            warn_only_func = lambda r: r['state'] in ['FAIL', 'UNKWN', 'QUE']
            data_rows = filter(warn_only_func, self.data)
        return data_rows[:self.options.num_displays]

    def format_value(self, field, value):
        if self.fields[field] == 'id':
            value = '.' + value.rsplit('.', 1)[-1]

        return super(DisplayJobRuns, self).format_value(field, value)

    def sorted_fields(self, values):
        """Build constructed fields and return fields in order."""
        values['scheduled_time'] = values['run_time'] or "-"

        return [values[name] for name in self.fields]

    def row_color(self, fields):
        return 'red' if fields['state'] == 'FAIL' else 'white'

    def post_row(self, row):
        start = row['start_time'] or "-"
        end =   row['end_time']   or "-"
        duration = row['duration'][:-7] if row['duration'] else "-"

        row_data = "%sStart: %s  End: %s  (%s)" % (
            ' ' * self.widths[0], start, end, duration
        )
        self.out.append(Color.set('gray', row_data))

        if self.options.warn:
            display_action = DisplayActionRuns(self.options)
            self.out.append(display_action.format(row))


class DisplayJobs(TableDisplay):

    columns = ['Name',  'State',    'Scheduler',    'Last Success']
    fields  = ['name',  'status',   'scheduler',    'last_success']
    widths  = [None,    10,         20,             20            ]
    title = 'jobs'

    detail_labels = [
        ('Job',                 'name'              ),
        ('State',               'status'            ),
        ('Scheduler',           'scheduler'         ),
        ('Node Pool',           'node_pool'         ),
        ('Run on all nodes',    'all_nodes'         ),
        ('Allow overlapping',   'allow_overlap'     ),
        ('Queue overlapping',   'queueing'          ),
    ]

    colors = {
        'name':      partial(Color.set, 'yellow'),
        'status':    add_color_for_state
    }


class DisplayActionRuns(TableDisplay):

    columns = ['Action', 'State', 'Start Time', 'End Time', 'Duration']
    fields  = ['id',     'state', 'start_time', 'end_time', 'duration']
    widths  = [None,     7,        22,          22,         10        ]
    title = 'actions'

    detail_labels = [
        ('Action Run',          'id'),
        ('State',               'state'),
        ('Node',                'node'),
        ('Command',             'command'),
        ('Bare command',        'raw_command'),
        ('Start time',          'start_time'),
        ('End time',            'end_time'),
        ('Exit status',         'exit_status'),
    ]

    colors = {
        'id':           partial(Color.set, 'yellow'),
        'state':        add_color_for_state,
        'command':      partial(Color.set, 'gray'),
    }

    def banner(self):
        if self.options.display_preface:
            self.out.append(format_fields(DisplayJobRuns, self.job_run))
        super(DisplayActionRuns, self).banner()

    def footer(self):
        if len(self.rows()) < len(self.data) and not self.options.warn:
            self.out.append('...')

    def format_value(self, field, value):
        if self.fields[field] == 'id':
            value = '.' + value.rsplit('.', 1)[-1]
        if self.fields[field] in ('start_time', 'end_time'):
            value = value or "-"
        if self.fields[field] == 'duration':
            # Strip microseconds
            value = value[:-7] if value else "-"

        return super(DisplayActionRuns, self).format_value(field, value)

    def row_color(self, fields):
        return 'red' if fields['state'] == 'FAIL' else 'white'

    def store_data(self, data):
        self.data = data['runs']
        self.job_run = data

    def rows(self):
        data_rows = self.data
        if self.options.warn:
            warn_only_func = lambda r: r['state'] in ['FAIL', 'UNKWN', 'QUE']
            data_rows = filter(warn_only_func, self.data)
        return data_rows[:self.options.num_displays]



class DisplayEvents(TableDisplay):

    columns = ['Time', 'Level', 'Entity', 'Name']
    fields  = ['time', 'level', 'entity', 'name']
    widths  = [22,     12,       35,      20    ]
    title = 'events'

    def calculate_col_widths(self):
        # No need to calculate, it's fixed width.
        pass


def view_with_less(content, color=True):
    """Send `content` through less."""
    import subprocess
    cmd = ['less']
    if color:
        cmd.append('-r')

    less_proc = subprocess.Popen(cmd, stdin=subprocess.PIPE)
    less_proc.stdin.write(content)
    less_proc.stdin.close()
    less_proc.wait()