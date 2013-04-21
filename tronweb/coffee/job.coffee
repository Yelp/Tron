
# Jobs
window.modules = window.modules || {}
window.modules.job = module = {}


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

    initialize: (models, options) =>
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
        @jobRunListView = new module.JobRunListView(model: @model)
        @listenTo(@refreshView, 'refreshView', => @model.fetch())
        sliderModel = new JobRunListSliderModel(@model)
        @sliderView = new modules.views.SliderView(model: sliderModel)
        @listenTo(@sliderView, "slider:change", @renderTimeline)
        @currentDate = new Date()

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
                <div>
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
            </div>
            <div class="span7 outline-block">
                <h2>Action Graph</h2>
                <div id="action-graph" class="graph job-view"></div>
            </div>

            <div class="span12 outline-block">
              <h2>Timeline</h2>
              <div>
                <div id="slider-chart"></div>
                <div id="timeline-graph"></div>
              </div>
            </div>

            <div id="job-runs"></div>
        </div>
        """

    # TODO: move to JobActionGraphView
    renderGraph: =>
        new GraphView(
            model: @model.get('action_graph')
            buildContent: (d) -> """<code class="command">#{d.command}</code>"""
            height: @$('table.details').height() - 5 # TODO: why -5 to get it flush?
        ).render()

    # TODO: move to JobTimelineView
    renderTimeline: =>
        job_runs = @model.get('runs')[...@sliderView.displayCount]
        new modules.timeline.TimelineView(
            model: job_runs
            nameField: 'run_num'
            width: @$('#timeline-graph').innerWidth()
            height: job_runs.length * 30 + 60
            maxDate: @currentDate
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

        @$('#job-runs').html(@jobRunListView.render().el)
        @$('#refresh').html(@refreshView.render().el)
        @renderGraph()
        @renderTimeline()
        @$('#slider-chart').html @sliderView.render().el
        makeTooltips(@$el)
        modules.views.makeHeaderToggle(@$el)
        @


class JobRunListSliderModel

    constructor: (@model) ->

    length: =>
        @model.get('runs').length


class module.JobRunListView extends Backbone.View

    initialize: (options) =>
        sliderModel = new JobRunListSliderModel(@model)
        @sliderView = new modules.views.SliderView(model: sliderModel)
        @listenTo(@sliderView, "slider:change", @renderList)

    tagName: "div"

    className: "span12 outline-block"

    template: _.template """
        <h2>Job Runs</h2>
        <div>
        <div id="slider-table"></div>
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
        """

    renderList: =>
        entry = (jobrun) ->
            new JobRunListEntryView(model:new JobRun(jobrun)).render().el
        models = @model.get('runs')[...@sliderView.displayCount]
        @$('tbody').html(entry(model) for model in models)

    render: =>
        @$el.html @template(@model.attributes)
        @$('#slider-table').html @sliderView.render().el
        @renderList()
        @

module.formatManualRun = (manual) ->
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
            <% print(modules.job.formatManualRun(manual)) %>
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
                <div>
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
            </div>
            <div class="span7 outline-block">
                <h2>Action Graph</h2>
                <div id="action-graph" class="graph job-view"></div>
            </div>

            <div class="span12 outline-block">
              <h2>Timeline</h2>
              <div>
                <div id="slider-chart"></div>
                <div id="timeline-graph"></div>
              </div>
            </div>

            <div class="span12 outline-block">
                <h2>Action Runs</h2>
                <div>
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
        </div>
        """

    renderList: =>
        entry = (run) =>
            run['job_name'] = @model.get('job_name')
            run['run_num'] =  @model.get('run_num')
            model = new modules.actionrun.ActionRun(run)
            new modules.actionrun.ActionRunListEntryView(model: model).render().el
        @$('tbody.actionruns').html(entry(model) for model in @model.get('runs'))

    getMaxDate: =>
        actionRuns = @model.get('runs')
        dates = (r.end_time || r.start_time for r in actionRuns)
        dates = (new Date(date) for date in dates when date?)
        dates.push(new Date(@model.get('run_time')))
        _.max(dates)

    renderTimeline: =>
        actionRuns = @model.get('runs')
        maxDate = @getMaxDate()

        startTime = (item) ->
            if item.start_time then new Date(item.start_time) else maxDate

        endTime = (item) ->
            if item.end_time then new Date(item.end_time) else maxDate

        new modules.timeline.TimelineView(
            model: actionRuns
            nameField: 'action_name'
            width: @$('#timeline-graph').innerWidth()
            height: actionRuns.length * 30 + 60
            maxDate: @currentDate
            startTime: startTime
            endTime: endTime
            margins:
                left: 150
        ).render()

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
        @renderTimeline()
        makeTooltips(@$el)
        modules.views.makeHeaderToggle(@$el)
        @
