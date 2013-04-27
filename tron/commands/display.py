"""
Format and color output for tron commands.
"""
import contextlib
from functools import partial
from operator import itemgetter
from tron.core import actionrun, job, service


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
    @contextlib.contextmanager
    def enable(cls):
        old_val = cls.enabled
        try:
            cls.enabled = True
            yield
        finally:
            cls.enabled = old_val

    @classmethod
    def set(cls, color_name, text):
        if not cls.enabled or not color_name:
            return unicode(text)
        return cls.colors[color_name.lower()] + unicode(text) + cls.colors['end']

    @classmethod
    def toggle(cls, enable):
        cls.enabled = enable


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
    resize_fields = set()
    reversed = False

    header_color = 'hgray'

    def __init__(self):
        self.out = []

    def banner(self):
        if not self.title:
            return
        title = self.title.capitalize()
        self.out.append("\n%s:" % title)
        if not self.rows():
            self.out.append("No %s\n" % title)

    def header(self):
        row = [label.ljust(self.get_field_width(i))
               for i, label in enumerate(self.columns)]
        self.out.append(Color.set(self.header_color, "".join(row)))

    def footer(self):
        pass

    def color(self, col, field):
        return None

    def sorted_fields(self, values):
        return [values[name] for name in self.fields]

    def format_row(self, fields):
        row = [
            Color.set(self.color(i, value), self.trim_value(i, value))
            for i, value in enumerate(self.sorted_fields(fields))
        ]
        return Color.set(self.row_color(fields), "".join(row))

    def get_field_width(self, field_idx):
        return self.widths[field_idx]

    def trim_value(self, field_idx, value):
        length = self.get_field_width(field_idx)
        value = self.format_value(field_idx, value)
        if len(value) > length:
            return (value[:length - 3] + '...').ljust(length)
        return value.ljust(length)

    def format_value(self, field_idx, value):
        return unicode(value)

    def output(self):
        out = "\n".join(self.out)
        self.out = []
        return out

    def post_row(self, row):
        pass

    def row_color(self, row):
        return None

    def rows(self):
        return sorted(self.data,
            key=itemgetter(self.fields[0]), reverse=self.reversed)

    def store_data(self, data):
        self.data = data

    def update_column_widths(self):
        """Update column widths to fit the data."""
        for field_idx, field in enumerate(self.fields):
            if field in self.resize_fields:
                self.widths[field_idx] = self.calculate_width(field_idx)

    def calculate_width(self, field_idx):
        default_width = self.widths[field_idx]
        column = [
            self.format_value(field_idx, row[self.fields[field_idx]])
            for row in self.data]
        if not column:
            return default_width
        max_value_width = max(len(value) for value in column)
        return max(max_value_width + 1, default_width)

    def format(self, data):
        self.store_data(data)
        self.update_column_widths()
        self.banner()

        if not self.rows():
            return self.output()

        self.header()
        for row in self.rows():
            self.out.append(self.format_row(row))
            self.post_row(row)

        self.footer()
        return self.output()


def add_color_for_state(state):
    if state == actionrun.ActionRun.STATE_FAILED.name:
        return Color.set('red', state)
    if state in set((
        actionrun.ActionRun.STATE_RUNNING.name,
        actionrun.ActionRun.STATE_SUCCEEDED.name,
        job.Job.STATUS_ENABLED,
        service.ServiceState.UP
    )):
        return Color.set('green', state)
    if state in set((job.Job.STATUS_DISABLED, service.ServiceState.DISABLED)):
        return Color.set('blue', state)
    return state


def format_fields(display_obj, content):
    """Format fields with some color."""
    def add_color(field, field_value):
        if field not in display_obj.colors:
            return field_value
        return display_obj.colors[field](field_value)

    def format_field(field):
        value = content[field]
        if value is None:
            return ''
        return field_display_mapping.get(field, lambda f: f)(value)

    def build_field(label, field):
        return "%-20s: %s" % (label, add_color(field, format_field(field)))

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
            node = display_node(inst['node'])
            return format_str % (inst['id'], node, state, failures)
        return [format(instance) for instance in service_instances]

    details     = format_fields(DisplayServices, service_content)
    instances   = format_instances(service_content['instances'])
    return details + '\n\nInstances:\n' + '\n'.join(instances)


def format_job_details(job_content):
    details = format_fields(DisplayJobs, job_content)
    job_runs = DisplayJobRuns().format(job_content['runs'])
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
    widths  = [50,      12,          5           ]
    title   = 'services'
    resize_fields = ['name']

    detail_labels = [
        ('Service',             'name'              ),
        ('Enabled',             'enabled'           ),
        ('State',               'state'             ),
        ('Max instances',       'count'             ),
        ('Command',             'command'           ),
        ('Pid Filename',        'pid_filename'      ),
        ('Node Pool',           'node_pool'         ),
        ('Monitor interval',    'monitor_interval'  ),
        ('Restart delay',       'restart_delay'     ),
    ]

    colors = {
        'name':     partial(Color.set, 'yellow'),
        'state':    add_color_for_state
    }


class DisplayJobRuns(TableDisplay):
    """Format Job runs."""

    columns = ['Run ID',    'State',    'Node', 'Scheduled Time']
    fields  = ['run_num',   'state',    'node', 'run_time']
    widths  = [10,          12,         30,     25]
    title = 'job runs'
    reversed = True

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

    def format_value(self, field_idx, value):
        if self.fields[field_idx] == 'run_num':
            value = '.' + str(value)

        if self.fields[field_idx] == 'scheduled_time':
            value = value or '-'

        if self.fields[field_idx] == 'node':
            value = display_node(value)

        return super(DisplayJobRuns, self).format_value(field_idx, value)

    def row_color(self, fields):
        return 'red' if fields['state'] == 'FAIL' else 'white'

    def post_row(self, row):
        start = row['start_time'] or "-"
        end =   row['end_time']   or "-"
        duration = row['duration'][:-7] if row['duration'] else "-"

        row_data = "%sStart: %s  End: %s  (%s)" % (
            ' ' * self.widths[0], start, end, duration)
        self.out.append(Color.set('gray', row_data))


class DisplayJobs(TableDisplay):

    columns = ['Name',  'State',    'Scheduler',    'Last Success']
    fields  = ['name',  'status',   'scheduler',    'last_success']
    widths  = [50,       10,         20,             20           ]
    title = 'jobs'
    resize_fields = ['name']

    detail_labels = [
        ('Job',                 'name'              ),
        ('State',               'status'            ),
        ('Scheduler',           'scheduler'         ),
        ('Max runtime',         'max_runtime'       ),
        ('Node Pool',           'node_pool'         ),
        ('Run on all nodes',    'all_nodes'         ),
        ('Allow overlapping',   'allow_overlap'     ),
        ('Queue overlapping',   'queueing'          ),
    ]

    colors = {
        'name':      partial(Color.set, 'yellow'),
        'status':    add_color_for_state
    }

    def format_value(self, field_idx, value):
        if self.fields[field_idx] == 'scheduler':
            value = display_scheduler(value)

        return super(DisplayJobs, self).format_value(field_idx, value)


class DisplayActionRuns(TableDisplay):

    columns = ['Action', 'State', 'Start Time', 'End Time', 'Duration']
    fields  = ['id',     'state', 'start_time', 'end_time', 'duration']
    widths  = [40,       12,      22,           22,         10     ]
    title = 'actions'
    resize_fields = ['id']

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
        self.out.append(format_fields(DisplayJobRuns, self.job_run))
        super(DisplayActionRuns, self).banner()

    def format_value(self, field_idx, value):
        if self.fields[field_idx] == 'id':
            value = '.' + value.rsplit('.', 1)[-1]
        if self.fields[field_idx] in ('start_time', 'end_time'):
            value = value or "-"
        if self.fields[field_idx] == 'duration':
            # Strip microseconds
            value = value[:-7] if value else "-"

        return super(DisplayActionRuns, self).format_value(field_idx, value)

    def row_color(self, fields):
        return 'red' if fields['state'] == 'FAIL' else 'white'

    def store_data(self, data):
        self.data = data['runs']
        self.job_run = data


class DisplayEvents(TableDisplay):

    columns = ['Time', 'Level', 'Entity', 'Name']
    fields  = ['time', 'level', 'entity', 'name']
    widths  = [22,     12,       35,      20    ]
    title = 'events'
    resize_fields = ['entity']


def display_node(source):
    return '%s@%s' % (source['username'], source['hostname'])


def display_node_pool(source):
    return "%s (%d node(s))" % (source['name'], len(source['nodes']))


def display_scheduler(source):
    return "%s %s%s" % (source['type'], source['value'], source['jitter'])


field_display_mapping = {
    'node':             display_node,
    'node_pool':        display_node_pool,
    'scheduler':        display_scheduler,
}


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
