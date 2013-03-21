

# Jobs

# TODO: move to utils
window.dateFromNow = (string, defaultString='Never') ->
    if string then moment(string).fromNow() else defaultString


class window.Job extends Backbone.Model

    idAttribute: "name"

    urlRoot: "/jobs"


class window.JobCollection extends Backbone.Collection

    model: Job

    url: "/jobs"

    parse: (resp, options) =>
        resp['jobs']


# TODO: unused ?
class JobRun extends Backbone.Model

    initialize: (name) ->
        @name = name

    idAttribute: "run_num"


class JobRunCollection extends Backbone.Collection

    initialize: (name) ->
        @name = name

    url: ->
        "/jobs/" + @name

    parse: (resp, options) =>
        resp['runs']


class window.JobListView extends Backbone.View

    initialize: (options) =>
        @listenTo(@model, "sync", @render)
        @listenTo(@model, "sort", @render)

    tagName: "div"

    className: "span12"

    # TODO: filter by name
    template: _.template '
        <h1>Jobs</h1>
        <table class="table table-hover">
            <thead>
                <tr>
                    <th>Name</td>
                    <th>Schedule</td>
                    <th>Node Pool</td>
                    <th>Last Success</td>
                    <th>Next Run</td>
                </tr>
            </thead>
            <tbody>
            </tbody>
        <table>'

    # TODO: sort by name/state/node
    render: ->
        @$el.html @template()
        entry = (model) -> new JobListEntryView(model: model).render().el
        @$('tbody').append(entry(model) for model in @model.models)
        @


class JobListEntryView extends Backbone.View

    initialize: (options) =>
        @listenTo(@model, "change", @render)

    tagName: "tr"

    className: =>
        switch @model.attributes.status
            when "DISABLED" then 'warning'
            when "ENABLED"  then 'enabled'
            when "RUNNING"  then 'info'

    template: _.template """
        <td><a href="#job/<%= name %>"><%= name %></a></td>
        <td><%= scheduler %></td>
        <td><%= node_pool %></td>
        <td><% print(dateFromNow(last_success, 'Never')) %></td>
        <td><% print(dateFromNow(next_run, 'None')) %></td>
        """

    render: ->
        @$el.html @template(@model.attributes)
        @


class window.JobView extends Backbone.View

    initialize: (options) =>
        @listenTo(@model, "change", @render)

    tagName: "div"

    className: "span12"

    template: _.template """
        <div class="row">
            <div class="span12">
                <h1>Job <%= name %></h1>
            </div>
            <div class="span8">
                <h2>Details</h2>
                <table class="table">
                    <tr><td>status</td>         <td><%= status %></td></tr>
                    <tr><td>Node pool</td>      <td><%= node_pool %></td></tr>
                    <tr><td>Schedule</td>       <td><code><%= scheduler %></code></td></tr>
                    <tr><td>Allow overlap</td>  <td><%= allow_overlap %></td></tr>
                    <tr><td>Queueing</td>       <td><%= queueing %></td></tr>
                    <tr><td>All nodes</td>      <td><%= all_nodes %></td></tr>
                    <tr><td>Last success</td>   <td><%= last_success %></td></tr>
                    <tr><td>Next run</td>       <td><%= next_run %></td></tr>
                </table>
            </div>

            <% if (runs.length > 0) { %>
            <div class="span12">
                <h2>Job Runs</h2>
                <table class="table">
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
            <% } %>

        </div>
        """

    render: ->
        @$el.html @template(@model.attributes)
        entry = (jobrun) -> new JobRunListEntryView(model:new JobRun(jobrun)).render().el
        @$('tbody.jobruns').append(entry(model) for model in @model.get('runs'))
        @


class JobRunListEntryView extends Backbone.View

    initialize: (options) =>
        @name = options.name
        @listenTo(@model, "change", @render)

    tagName: "tr"

    className: ->
        switch @model.get('state')
            when "RUNN"     then 'info'
            when "FAIL"     then 'error'
            when "SUCC"     then 'success'

    # TODO: add manual run flag
    template: _.template """
        <td><a href="#job/<%= job_name %>/<%= run_num %>"><%= id %></a></td>
        <td><%= state %></td>
        <td><%= node %></td>
        <td><% print(dateFromNow(start_time, "Unknown")) %></td>
        <td><% print(dateFromNow(end_time, "")) %></td>
        """

    render: ->
        @$el.html @template(@model.attributes)
        @
