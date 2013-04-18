
# Jobs


class window.Job extends Backbone.Model

    initialize: (options) =>
        super options
        options = options || {}
        @refreshModel = options.refreshModel

    idAttribute: "name"

    urlRoot: "/jobs"

    url: ->
        super() + "?include_action_graph=1"


class window.JobCollection extends Backbone.Collection

    initialize: (options) =>
        super options
        options = options || {}
        @refreshModel = options.refreshModel
        @filterModel = options.filterModel

    model: Job

    url: "/jobs?include_job_runs=1"

    parse: (resp, options) =>
        resp['jobs']

    comparator: (job) =>
        job.get('name')


class window.JobRun extends Backbone.Model

    initialize: (options) =>
        super options
        options = options || {}
        @refreshModel = options.refreshModel

    idAttribute: "run_num"

    urlRoot: ->
        "/jobs/" + @get('name')

    url: =>
        super() + "?include_action_graph=1&include_action_runs=1"

    parse: (resp, options) =>
        resp['job_url'] = "#job/" + resp['job_name']
        resp


class window.JobListFilterModel extends FilterModel

    filterTypes:
        name:       buildMatcher(fieldGetter('name'), matchAny)
        node_pool:  buildMatcher(nestedName('node_pool'), _.str.startsWith)
        status:      buildMatcher(fieldGetter('status'), _.str.startsWith)


class window.JobListView extends Backbone.View

    initialize: (options) =>
        @listenTo(@model, "sync", @render)
        @refreshView = new RefreshToggleView(model: @model.refreshModel)
        @filterView = new FilterView(model: @model.filterModel)
        @listenTo(@refreshView, 'refreshView', => @model.fetch())
        @listenTo(@filterView, "filter:change", @renderList)

    tagName: "div"

    className: "span12"

    template: _.template """
        <h1>
            Scheduled Jobs
            <span id="refresh"></span>
        </h1>
        <div id="filter-bar"></div>
        <div class="outline-block">
        <table class="table table-hover table-outline table-striped">
            <thead class="header">
                <tr>
                    <th class="span4">Name</th>
                    <th>Status</th>
                    <th>Schedule</th>
                    <th>Node Pool</th>
                    <th>Last Success</th>
                    <th>Next Run</th>
                </tr>
            </thead>
            <tbody>
            </tbody>
        </table>
        </div>
        """

    render: ->
        @$el.html @template()
        @renderFilter()
        @$('#refresh').html(@refreshView.render().el)
        @renderList()
        @

    renderList: =>
        models = @model.filter(@model.filterModel.createFilter())
        entry = (model) -> new JobListEntryView(model: model).render().el
        @$('tbody').html(entry(model) for model in models)

    renderFilter: =>
        @$('#filter-bar').html(@filterView.render().el)


class JobListEntryView extends ClickableListEntry

    initialize: (options) =>
        @listenTo(@model, "change", @render)

    tagName: "tr"

    className: "clickable"

    template: _.template """
        <td><a href="#job/<%= name %>"><% print(formatName(name)) %></a></td>
        <td><% print(formatState(status)) %></td>
        <td><% print(formatScheduler(scheduler)) %></td>
        <td><% print(displayNodePool(node_pool)) %></td>
        <td><% print(dateFromNow(last_success, 'never')) %></td>
        <td><% print(dateFromNow(next_run, 'none')) %></td>
        """

    render: ->
        @$el.html @template(@model.attributes)
        makeTooltips(@$el)
        @


class window.JobView extends Backbone.View

    initialize: (options) =>
        @listenTo(@model, "change", @render)
        @refreshView = new RefreshToggleView(model: @model.refreshModel)
        @listenTo(@refreshView, 'refreshView', => @model.fetch())

    tagName: "div"

    className: "span12"

    template: _.template """
        <div class="row">
            <div class="span12">
                <h1>
                    <small>Job</small>
                    <% print(formatName(name)) %>
                    <span id="refresh"></span>
                </h1>
            </div>
            <div class="span5 outline-block">
                <h2>Details</h2>
                <table class="table details">
                    <tbody>
                    <tr><td>Status</td>
                        <td><% print(formatState(status)) %></td></tr>
                    <tr><td>Node pool</td>
                        <td><% print(displayNodePool(node_pool)) %></td></tr>
                    <tr><td>Schedule</td>
                        <td><% print(formatScheduler(scheduler)) %></td></tr>
                    <tr><td>Settings</td>
                        <td><%= settings %></td></tr>
                    <tr><td>Last success</td>
                        <td><% print(dateFromNow(last_success)) %></td></tr>
                    <tr><td>Next run</td>
                        <td><% print(dateFromNow( next_run)) %></td></tr>
                    </tbody>
                </table>
            </div>
            <div class="span7 outline-block">
                <h2>Action Graph</h2>
                <div id="action-graph" class="graph job-view"></div>
            </div>

            <div class="span12 outline-block">
                <h2>Job Runs</h2>
                <table class="table table-hover table-outline table-striped">
                    <thead class="sub-header">
                        <tr>
                            <th>Id</th>
                            <th>State</th>
                            <th>Node</th>
                            <th>Start</th>
                            <th>End</th>
                        </tr>
                    </thead>
                    <tbody class="jobruns">
                    </tbody>
                </table>
            </div>

        </div>
        """

    renderGraph: =>
        new GraphView(
            model: @model.get('action_graph')
            buildContent: (d) -> """<code class="command">#{d.command}</code>"""
            height: $('table.details').height() - 5 # TODO: why -5 to get it flush?
        ).render()

    formatSettings: (attrs) =>
        template = _.template """
            <span class="label-icon tt-enable" title="<%= title %>">
                <i class="web-icon-<%= icon %>"></i>
            </span>
            """

        [icon, title] = if attrs.allow_overlap
            ['overlap', "Allow overlapping runs"]
        else if attrs.queueing
            ['queue', "Queue overlapping runs"]
        else
            ['cancel', "Cancel overlapping runs"]

        content = if attrs.all_nodes
            template(icon: 'all-nodes', title: "Run on all nodes")
        else
            ""
        template(icon: icon, title: title) + content

    render: ->
        @$el.html @template _.extend {},
            @model.attributes,
            settings: @formatSettings(@model.attributes)

        entry = (jobrun) -> new JobRunListEntryView(model:new JobRun(jobrun)).render().el
        @$('tbody.jobruns').append(entry(model) for model in @model.get('runs'))
        @$('#refresh').html(@refreshView.render().el)
        @renderGraph()
        makeTooltips(@$el)
        @


window.formatManualRun = (manual) ->
    if ! manual then "" else """
        <span class="label label-manual">
            <i class="icon-hand-down icon-white tt-enable" title="Manual run"></i>
        </span>
    """

formatInterval = (interval) ->
    humanized = getDuration(interval).humanize()
    """
        <span class="tt-enable" title="#{interval}">
         #{humanized}
        </span>
    """

window.formatScheduler = (scheduler) ->
    [icon, value] = switch scheduler.type
        when 'constant' then ['web-icon-repeat', 'constant']
        when 'interval' then ['icon-align-justify', formatInterval(scheduler.value)]
        when 'groc'     then ['web-icon-calendar', scheduler.value]
        when 'daily'    then ['icon-calendar', scheduler.value]
        when 'cron'     then ['icon-time', scheduler.value]

    _.template("""
            <i class="<%= icon %> tt-enable"
                title="<%= type %> scheduler"></i>
        <span class="scheduler">
            <%= value %>
        </span>
        <% if (jitter) { %>
            <i class="icon-random tt-enable" title="Jitter<%= jitter %>"></i>
        <% } %>
    """)(
         icon: icon
         type: scheduler.type
         value: value
         jitter: scheduler.jitter)


class JobRunListEntryView extends ClickableListEntry

    initialize: (options) =>
        @listenTo(@model, "change", @render)

    tagName: "tr"

    className: "clickable"

    # TODO: add icon for manual run flag
    template: _.template """
        <td>
            <a href="#job/<%= job_name %>/<%= run_num %>"><%= run_num %></a>
            <% print(formatManualRun(manual)) %>
        </td>
        <td><% print(formatState(state)) %></td>
        <td><% print(displayNode(node)) %></td>
        <td><% print(dateFromNow(start_time || run_time, "Unknown")) %></td>
        <td><% print(dateFromNow(end_time, "")) %></td>
        """

    render: ->
        @$el.html @template(@model.attributes)
        makeTooltips(@$el)
        @


class window.JobRunView extends Backbone.View

    initialize: (options) =>
        @listenTo(@model, "change", @render)
        @refreshView = new RefreshToggleView(model: @model.refreshModel)
        @listenTo(@refreshView, 'refreshView', => @model.fetch())

    tagName: "div"

    className: "span12"

    template: _.template """
         <div class="row">
            <div class="span12">
                <h1>
                    <small>Job Run</small>
                    <a href="<%= job_url %>">
                        <% print(formatName(job_name)) %></a>.<%= run_num %>
                    <span id="filter"</span>
                </h1>

            </div>
            <div class="span5 outline-block">
                <h2>Details</h2>
                <table class="table details">
                    <tr><td class="span2">State</td>
                        <td><% print(formatState(state)) %></td></tr>
                    <tr><td>Node</td>
                        <td><% print(displayNode(node)) %></td></tr>
                    <tr><td>Manual</td>         <td><%= manual %></td></tr>
                    <tr><td>Scheduled</td>      <td><%= run_time %></td></tr>
                    <tr><td>Start</td>
                        <td><% print(dateFromNow(start_time, '')) %></td>
                    </tr>
                    <tr><td>End</td>
                        <td><% print(dateFromNow(end_time, '')) %></td>
                    </tr>
                </table>
            </div>
            <div class="span7 outline-block">
                <h2>Action Graph</h2>
                <div id="action-graph" class="graph job-view"></div>
            </div>

            <div class="span12 outline-block">
                <h2>Action Runs</h2>
                <table class="table table-hover table-outline">
                    <thead class="sub-header">
                        <tr>
                            <th>Name</th>
                            <th>State</th>
                            <th class="span3">Command</th>
                            <th>Node</th>
                            <th>Start</th>
                            <th>End</th>
                        </tr>
                    </thead>
                    <tbody class="actionruns">
                    </tbody>
                </table>
            </div>

        </div>
        """

    renderList: =>
        entry = (run) =>
            run['job_name'] = @model.get('job_name')
            run['run_num'] =  @model.get('run_num')
            new ActionRunListEntryView(model:new ActionRun(run)).render().el
        @$('tbody.actionruns').html(entry(model) for model in @model.get('runs'))

    # TODO: add class for state
    popupTemplate: _.template """
        <ul class="unstyled">
            <li><% print(formatState(state)) %></li>
            <li><code class="command"><% print(command || raw_command) %></code></li>
        </ul>
        """

    renderGraph: =>
        new GraphView(
            model: @model.get('action_graph')
            buildContent: @popupTemplate
            nodeClass: (d) -> "node #{d.state}"
        ).render()

    render: =>
        @$el.html @template(@model.attributes)
        @$('#filter').html(@refreshView.render().el)
        @renderList()
        @renderGraph()
        makeTooltips(@$el)
        @
