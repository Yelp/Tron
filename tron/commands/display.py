"""
Format and color output for tron commands.
"""
from operator import itemgetter


class Color(object):

    enabled = display_color = None
    colors = {
        'gray': '\033[90m',
        'cyan': '\033[91m',
        'green': '\033[92m',
        'yellow': '\033[93m',
        'blue': '\033[94m',
        'red': '\033[91m',
        'white': '\033[99m',
        # h is for highlighted
        'hgray': '\033[100m',
        'hcyan': '\033[101m',
        'hgreen': '\033[102m',
        'hyellow': '\033[103m',
        'hblue': '\033[104m',
        'hred': '\033[106m',
        'end': '\033[0m',
    }

    @classmethod
    def set(cls, color_name, text):
        if not cls.enabled or not color_name:
            return unicode(text)
        return cls.colors[color_name.lower()] + unicode(text) + cls.colors['end']


class FormatDisplay(object):
    """Base class for displaying columns of data.  This class takes a list
    of dict objects and formats it so that it displays properly in fixed width
    columns.  Overlap is truncated.
    
    This class provides many hooks for contomizing the output, include:
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
    max_first_col_width = None

    def __init__(self, num_cols, options=None):
        self.out = []
        self.options = options
        self.num_cols = num_cols

    def banner(self):
        if not self.title:
            return
        title = self.title.capitalize()
        self.out.append("\n%s:" % title)
        if not self.data:
            self.out.append("No %s" % title)

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
        if len(value) > length:
            return (value[:length - 3] + '...').ljust(length)
        return value.ljust(length)

    format_header = format_value

    def calculate_col_widths(self):
        label_width = len(self.columns[0]) + 1
        if not self.data:
            max_name_width = 1
        else:
            max_name_width = max(len(item[self.fields[0]]) for item in self.data) + 1
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

    def format(self, data):
        self.data = data
        self.banner()

        self.calculate_col_widths()
        if not self.data:
            return self.output()

        self.header()
        for row in self.rows():
            self.out.append(self.format_row(row))
            self.post_row(row)

        self.footer()
        return self.output()


class DisplayServices(FormatDisplay):

    columns = ['Name',  'State',    'Count' ]
    fields  = ['name',  'state',    'count' ]
    widths  = [None,    10,         10      ]
    title = 'services'

    max_first_col_width = 30


class DisplayJobRuns(FormatDisplay):
    """Format Job runs."""
   
    columns = ['Run ID', 'State',    'Node', 'Scheduled Time']
    fields  = ['id',     'state',    'node', 'scheduled_time']
    widths  = [None,     6,          20,     25              ]
    title = 'job runs'

    time_size = len("0000-00-00 00:00")

    @property
    def max_first_col_width(self):
        return max(self.num_cols - 64, 5)
        
    def rows(self):
        data_rows = self.data
        if self.options.warn:
            warn_only_func = lambda r: r['state'] in ['FAIL', 'UNKWN', 'QUE']
            data_rows = filter(warn_only_func, self.data)
        return data_rows[:self.options.num_displays]
    
    def format_value(self, field, value):
        if self.fields[field] == 'id':
            value = '.' + '.'.join(value.split('.')[1:])

        return super(DisplayJobRuns, self).format_value(field, value)

    def sorted_fields(self, values):
        """Build constructed fields and return fields in order."""
        run = (values['run_time'] and values['run_time'][:self.time_size]) or "-"
        values['scheduled_time'] = run

        return [values[name] for name in self.fields]

    def row_color(self, fields):
        return 'red' if fields['state'] == 'FAIL' else 'white'

    def post_row(self, row):
        time_size = self.time_size
        start = (row['start_time'] and row['start_time'][:time_size]) or "-"
        end = (row['end_time'] and row['end_time'][:time_size]) or "-"
        duration = (row['duration'] and row['duration'].split('.')[0]) or "-"

        row_data = "%sStart: %s  End: %s  (%s)" % (
            ' ' * self.widths[0], start, end, duration
        )
        self.out.append(Color.set('gray', row_data))

        if self.options.warn:
            # view_job_run
            display_action = DisplayActions(self.num_cols, self.options)
            self.out.append(display_action.format_action_run(row['action_runs']))
        

class DisplayJobs(FormatDisplay):

    columns = ['Name',  'State',    'Scheduler',    'Last Success']
    fields  = ['name',  'state',    'scheduler',    'last_success']
    widths  = [None,    10,         20,             20            ]
    title = 'jobs'


    @property
    def max_first_col_width(self):
        return max(self.num_cols - 54, 5)
        
    def post_row(self, row):
        if self.options.warn:
            self.out.extend(self.do_format_job(row['details'], True))

    def format_job(self, job_details):
        self.out = self.do_format_job(job_details)
        return self.output()

    def do_format_job(self, job_details, supress_preface=False):
        # was view_job
        out = []
        if self.options.display_preface and not supress_preface:
            out.extend([
                job_details['name'] + ":",
                "Scheduler: %s" % job_details['scheduler'],
                "\nList of Actions (topological):",
            ] + job_details['action_names'] + [
                "\nNode Pool:"
            ] + job_details['node_pool'] + [
                "Run History: (%d total)" % len(job_details['runs'])
            ])
        job_runs = self.format_job_runs(job_details['runs'])
        out.append(job_runs)
        return out

    def format_job_runs(self, run_details):
        # war print_job_run
        return DisplayJobRuns(self.num_cols, self.options).format(run_details)


class DisplayActions(FormatDisplay):

    columns = ['Action', 'State', 'Start Time', 'End Time', 'Duration']
    fields  = ['id',     'state', 'start_time', 'end_time', 'duration']
    widths  = [None,     6,        20,          20,         10        ]
    title = 'actions'


    @property
    def max_first_col_width(self):
        return max(self.num_cols - 60, 5)

    def footer(self):
        # TODO: may display ... if filtering for warn only
        if len(self.rows()) < len(self.data):
            self.out.append('...')

    def format_value(self, field, value):
        if self.fields[field] == 'id':
            value = '.' + '.'.join(value.split('.')[2:])
        if self.fields[field] in ('start_time', 'end_time', 'duration'):
            value = (value and value[:-7]) or "-"

        return super(DisplayActions, self).format_value(field, value)

    def row_color(self, fields):
        return 'red' if fields['state'] == 'FAIL' else 'white'

    def rows(self):
        data_rows = self.data
        if self.options.warn:
            warn_only_func = lambda r: r['state'] in ['FAIL', 'UNKWN', 'QUE']
            data_rows = filter(warn_only_func, self.data)
        return data_rows[:self.options.num_displays]

    def post_row(self, row):
        if self.options.warn:
            self.out.extend(self.do_format_action_run(row['details'], True))

    def format_action_run(self, content):
        self.out = self.do_format_action_run(content)
        return self.output()

    def do_format_action_run(self, content, supress_preface=False):
        # was view_action_run
        out = []
        if self.options.stdout:
            out.extend(["Stdout: "] + content['stdout'])
            return out
    
        if self.options.stderr or self.options.warn:
            out.extend(["Stderr: "] + content['stderr'])
            return out
    
        if self.options.display_preface and not supress_preface:
            out.extend([
                "Action Run: %s" % content['id'],
                "State: %s" % content['state'],
                "Node: %s" % content['node'],
                ''
            ])
            return out
    
        # a raw command is without command context
        if content['command'] != content['raw_command']:
            if content['command'] == 'false':
                out.append(Color.set("red", "Bad Command"))
            else:
                out.append(Color.set("gray", content['command']))
   
        out.extend(
            [Color.set("gray", content['raw_command'])] +
            ["\nRequirements:"] + content['requirements'] +
            ["\nStdout:"] + content['stdout'] + 
            ["Stderr:"] + content['stderr']
        )
        return out


