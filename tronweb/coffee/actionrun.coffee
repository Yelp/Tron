

class window.ActionRun extends Backbone.Model

    initialize: (options) =>
        super options
        options = options || {}
        @refreshModel = options.refreshModel

    idAttribute: "action_name"

    urlRoot: ->
        "/jobs/#{ @get('job_name') }/#{ @get('run_num') }/"

    parse: (resp, options) =>
        resp['job_url'] = "#job/#{ @get('job_name') }"
        resp['job_run_url'] = "#{ resp['job_url'] }/#{ @get('run_num') }"
        resp['url'] = "#{ resp['job_run_url'] }/#{ resp['action_name'] }"
        resp


class window.ActionRunListEntryView extends ClickableListEntry

    initialize: (options) =>
        @listenTo(@model, "change", @render)

    tagName: "tr"

    className: =>
        stateName = switch @model.get('state')
            when "running"      then 'info'
            when "failed"       then 'error'
            when "succeeded"    then 'success'
        "#{ stateName } clickable"

    template: _.template """
        <td>
            <a href="#job/<%= job_name %>/<%= run_num %>/<%= action_name %>">
            <% print(formatName(action_name)) %></a></td>
        <td><%= state %></td>
        <td><code class="command"><% print(command || raw_command) %></code></td>
        <td><%= exit_status %></td>
        <td><%= node %></td>
        <td><% print(dateFromNow(start_time, "None")) %></td>
        <td><% print(dateFromNow(end_time, "")) %></td>
        """

    render: ->
        @$el.html @template(@model.attributes)
        makeTooltips(@$el)
        @


class window.ActionRunView extends Backbone.View

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
                    <small>Action Run</small>
                    <a href="<%= job_url %>"><% print(formatName(job_name)) %></a>.
                    <a href="<%= job_run_url %>"><%= run_num %></a>.
                    <% print(formatName(action_name)) %>
                    <span id="refresh"></span>
                </h1>
            </div>
            <div class="span12">
                <h2>Details</h2>
                <table class="table table-condensed details">
                    <tbody>
                    <tr><td class="span2">State</td>          <td><%= state %></td></tr>
                    <tr><td>Node</td>           <td><%= node %></td></tr>
                    <tr><td>Raw command</td>
                        <td><code class="command"><%= raw_command %></code></td></tr>
                    <% if (command) { %>
                    <tr><td>Command</td>
                        <td><code class="command"><%= command %></code></td></tr>
                    <% } %>
                    <tr><td>Exit</td>           <td><%= exit_status %></td></tr>
                    <tr><td>Start time</td>
                        <td><% print(dateFromNow(start_time, ''))  %></td></tr>
                    <tr><td>End time</td>
                        <td><% print(dateFromNow(end_time, 'Unknown')) %></td></tr>
                    <tr><td>Duration</td>
                        <td><%= duration %></td></tr>
                    </tbody>
                </table>
            </div>
            <div class="span12">
                <h2>stdout</h2>
                <pre><% print(stdout.join('\\n')) %></pre>

                <h2>stderr</h2>
                <pre><% print(stderr.join('\\n')) %></pre>
            </div>
        </div>
        """

    render: ->
        @$el.html @template(@model.attributes)
        @$('#refresh').html(@refreshView.render().el)
        makeTooltips(@$el)
        @
