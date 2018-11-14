

window.modules = window.modules || {}
module = window.modules.actionrun = {}


class module.ActionRun extends Backbone.Model

    initialize: (options) =>
        super options
        options = options || {}
        @refreshModel = options.refreshModel

    idAttribute: "action_name"

    urlRoot: ->
        "/jobs/#{ @get('job_name') }/#{ @get('run_num') }/"

    urlArgs: "?include_stdout=1&include_stderr=1&num_lines=0"

    url: =>
        super() + @urlArgs

    parse: (resp, options) =>
        resp['job_url'] = "#job/#{ @get('job_name') }"
        resp['job_run_url'] = "#{ resp['job_url'] }/#{ @get('run_num') }"
        resp['url'] = "#{ resp['job_run_url'] }/#{ @get('action_name') }"
        resp


class module.ActionRunHistoryEntry extends module.ActionRun

    idAttribute: "id"

    parse: (resp, options) =>
        resp


class module.ActionRunHistory extends Backbone.Collection

    initialize: (models, options) =>
        options = options || {}
        @job_name = options.job_name
        @action_name = options.action_name

    model: module.ActionRunHistoryEntry

    url: =>
        "/jobs/#{ @job_name }/#{ @action_name }/"

    parse: (resp, options) =>
        resp

    reset: (models, options) =>
        super models, options

    add: (models, options) =>
        super models, options


class module.ActionRunHistoryListEntryView extends ClickableListEntry

    tagName: "tr"

    template: _.template """
        <td>
            <a href="#job/<%= job_name %>/<%= run_num %>/<%= action_name %>">
            <%= run_num %></a></td>
        <td><%= formatState(state) %></td>
        <td><%= displayNode(node) %></td>
        <td><%= modules.actionrun.formatExit(exit_status) %></td>
        <td><%= dateFromNow(start_time, "None") %></td>
        <td><%= dateFromNow(end_time, "") %></td>
    """

    render: ->
        @$el.html @template(@model.attributes)
        makeTooltips(@$el)
        @


class module.ActionRunTimelineEntry

    constructor: (@actionRun, @maxDate) ->

    toString: =>
        @actionRun.action_name

    getYAxisLink: =>
        "#job/#{@actionRun.job_name}/#{@actionRun.run_num}/#{@actionRun.action_name}"

    getYAxisText: =>
        @actionRun.action_name

    getBarClass: =>
        @actionRun.state

    getStart: =>
        @getDate(@actionRun.start_time)

    getEnd: =>
        @getDate(@actionRun.end_time)

    getDate: (date) ->
        if date then new Date(date) else @maxDate


class module.ActionRunListEntryView extends ClickableListEntry

    initialize: (options) =>
        @listenTo(@model, "change", @render)

    tagName: "tr"

    template: _.template """
        <td>
            <a href="#job/<%= job_name %>/<%= run_num %>/<%= action_name %>">
            <%= formatName(action_name) %></a></td>
        <td><%= formatState(state) %></td>
        <td><code class="command"><%= command || raw_command %></code></td>
        <td><%= displayNode(node) %></td>
        <td><%= dateFromNow(start_time, "None") %></td>
        <td><%= dateFromNow(end_time, "") %></td>
        """

    render: ->
        @$el.html @template(@model.attributes)
        makeTooltips(@$el)
        @


module.formatExit = (exit) ->
    return '' if not exit? or exit == ''
    template = _.template """
        <span class="badge badge-<%= type %>"><%= exit %></span>
    """
    template(exit: exit, type: if not exit then "success" else "important")


class module.ActionRunView extends Backbone.View

    initialize: (options) =>
        @listenTo(@model, "change", @render)
        @refreshView = new RefreshToggleView(model: @model.refreshModel)
        historyCollection = options.history
        @historyView = new module.ActionRunHistoryView(model: historyCollection)
        @listenTo(@refreshView, 'refreshView', => @model.fetch())
        @listenTo(@refreshView, 'refreshView', => historyCollection.fetch())

    tagName: "div"

    template: _.template """
            <div class="span12">
                <h1>
                    <small>Action Run</small>
                    <a href="<%= job_url %>"><%= formatName(job_name) %></a>.<a href="<%= job_run_url %>"><%= run_num %></a>.<%= formatName(action_name) %>
                    <span id="refresh"></span>
                </h1>
            </div>
            <div class="span12 outline-block">
                <h2>Details</h2>
                <div>
                <table class="table details">
                    <tbody>
                    <tr><td class="span2">State</td>
                        <td><%= formatState(state) %><%= formatDelay(in_delay) %></td></tr>
                    <tr><td>Node</td>
                        <td><%= displayNode(node) %></td></tr>
                    <tr><td>Raw command</td>
                        <td><code class="command"><%= raw_command %></code></td></tr>
                    <% if (command) { %>
                    <tr><td>Command</td>
                        <td><code class="command"><%= command %></code></td></tr>
                    <% } %>
                    <tr><td>Exit codes</td>
                        <td>
                            <%= modules.actionrun.formatExit(exit_status) %>
                            <% if (exit_statuses) { %>
                                <small>
                                    (exits of failed attempts:
                                    <%= _.map(
                                            _.sortBy(
                                                exit_statuses,
                                                function(val, key) {
                                                    return -key;
                                                }
                                            ),
                                            modules.actionrun.formatExit
                                        ).join(", ") %>)
                                </small>
                            <% } %>
                        </td>
                    </tr>
                    <tr><td>Start time</td>
                        <td><% print(dateFromNow(start_time, ''))  %></td></tr>
                    <tr><td>End time</td>
                        <td><%= dateFromNow(end_time, 'Unknown') %></td></tr>
                    <tr><td>Duration</td>
                        <td><%= formatDuration(duration) %></td></tr>
                    <tr><td>Waits for triggers</td>
                        <td><%= triggered_by %></td></tr>
                    <tr><td>Publishes triggers</td>
                        <td><%= trigger_downstreams %></td></tr>
                    </tbody>
                </table>
                </div>
            </div>
            <div class="span12 outline-block">
                <h2>stdout</h2>
                <pre class="stdout"><%= stdout.join('\\n') %></pre>
            </div>
            <div class="span12 outline-block">
                <h2>stderr</h2>
                <pre class="stderr"><%= stderr.join('\\n') %></pre>
            </div>

            <div id="action-run-history">
            </div>
        """

    render: ->
        @$el.html @template(@model.attributes)
        @$('#refresh').html(@refreshView.render().el)
        @$('#action-run-history').html(@historyView.render().el)
        makeTooltips(@$el)
        modules.views.makeHeaderToggle(@$el)
        @

class ActionRunHistorySliderModel

    constructor: (@model) ->

    length: =>
        @model.models.length


class module.ActionRunHistoryView extends Backbone.View

    initialize: (options) =>
        @listenTo(@model, "sync", @render)
        sliderModel = new ActionRunHistorySliderModel(@model)
        @sliderView = new modules.views.SliderView(model: sliderModel)
        @listenTo(@sliderView, "slider:change", @renderList)

    tagName: "div"

    className: "span12 outline-block"

    template: _.template """
          <h2>History</h2>
          <div>
          <div id="slider"></div>
          <table class="table table-hover table-outline table-striped">
            <thead class="sub-header">
              <tr>
                <th class="span1">Run</th>
                <th>State</th>
                <th>Node</th>
                <th>Exit</th>
                <th>Start</th>
                <th>End</th>
              </tr>
            </thead>
            <tbody>
            </tbody>
          </table>
          </div>
       """

    renderList: =>
        view = (model) ->
            new module.ActionRunHistoryListEntryView(model: model).render().el
        models = @model.models[...@sliderView.displayCount]
        @$('tbody').html(view(model) for model in models)

    render: =>
        @$el.html @template()
        @renderList()
        @$('#slider').html @sliderView.render().el if @model.models.length
        modules.views.makeHeaderToggle(@$el.parent())
        @
