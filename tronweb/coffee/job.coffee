
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

    filterTypes: ['name', 'node_pool', 'status']


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
        <table class="table table-hover table-outline">
            <thead>
                <tr>
                    <th class="span4">Name</td>
                    <th>Schedule</td>
                    <th>Node Pool</td>
                    <th>Last Success</td>
                    <th>Next Run</td>
                </tr>
            </thead>
            <tbody>
            </tbody>
        </table>
        """

    # TODO: sort by name/state/node
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

    className: =>
        stateName = switch @model.attributes.status
            when "disabled" then 'warning'
            when "enabled"  then 'enabled'
            when "running"  then 'info'
        "#{ stateName } clickable"

    template: _.template """
        <td><a href="#job/<%= name %>"><% print(formatName(name)) %></a></td>
        <td><%= scheduler %></td>
        <td><%= node_pool %></td>
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
                <table class="table table-condensed details">
                    <tbody>
                    <tr><td>Status</td>         <td><%= status %></td></tr>
                    <tr><td>Node pool</td>      <td><%= node_pool %></td></tr>
                    <tr><td>Schedule</td>
                        <td><code><%= scheduler %></code></td></tr>
                    <tr><td>Allow overlap</td>
                        <td><%= allow_overlap %></td></tr>
                    <tr><td>Queueing</td>       <td><%= queueing %></td></tr>
                    <tr><td>All nodes</td>      <td><%= all_nodes %></td></tr>
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
                <table class="table table-hover table-outline">
                    <thead>
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
        ).render()

    render: ->
        @$el.html @template(@model.attributes)
        entry = (jobrun) -> new JobRunListEntryView(model:new JobRun(jobrun)).render().el
        @$('tbody.jobruns').append(entry(model) for model in @model.get('runs'))
        @$('#refresh').html(@refreshView.render().el)
        @renderGraph()
        makeTooltips(@$el)
        @

class JobRunListEntryView extends ClickableListEntry

    initialize: (options) =>
        @listenTo(@model, "change", @render)

    tagName: "tr"

    className: ->
        stateName = switch @model.get('state')
            when "running"      then 'info'
            when "failed"       then 'error'
            when "succeeded"    then 'success'
        "#{ stateName } clickable"

    # TODO: add icon for manual run flag
    template: _.template """
        <td><a href="#job/<%= job_name %>/<%= run_num %>"><%= run_num %></a></td>
        <td><%= state %></td>
        <td><%= node %></td>
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
            <div class="span5">
                <h2>Details</h2>
                <table class="table table-condensed details">
                    <tr><td class="span2">State</td>
                        <td><%= state %></td></tr>
                    <tr><td>Node</td>           <td><%= node %></td></tr>
                    <tr><td>Manual</td>         <td><%= manual %></td></tr>
                    <tr><td>Scheduled</td>      <td><%= run_time %></td></tr>
                    <tr><td>Start</td>
                        <td><% print(dateFromNow(start_time, 'None')) %></td>
                    </tr>
                    <tr><td>End</td>
                        <td><% print(dateFromNow(end_time, 'None')) %></td>
                    </tr>
                </table>
            </div>
            <div class="span7">
                <h2>Action Graph</h2>
                <div id="action-graph" class="graph job-view"></div>
            </div>

            <div class="span12">
                <h2>Action Runs</h2>
                <table class="table table-hover table-outline">
                    <thead>
                        <tr>
                            <th>Name</th>
                            <th>State</th>
                            <th class="span3">Command</th>
                            <th>Exit</th>
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
            <li><span class="label"><%= state %></span></li>
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
