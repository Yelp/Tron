
# Services

class window.Service extends Backbone.Model

    idAttribute: "name"

    urlRoot: "/services"

    url: =>
        "#{@urlRoot}/#{@get(@idAttribute)}?include_events=6"

    initialize: (options) =>
        super options
        options = options || {}
        @refreshModel = options.refreshModel


class window.ServiceInstance extends Backbone.Model


class window.ServiceCollection extends Backbone.Collection

    initialize: (options) =>
        super options
        options = options || {}
        @refreshModel = options.refreshModel
        @filterModel = options.filterModel

    model: Service

    url: "/services"

    parse: (resp, options) =>
        resp['services']

    comparator: (service) =>
        service.get('name')

class window.ServiceListView extends Backbone.View

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
            Services
            <span id="refresh"></span>
        </h1>
        <div id="filter-bar"></div>

        <div class="outline-block">
        <table class="table table-hover table-outline table-striped">
            <thead class="header">
                <tr>
                    <th>Name</td>
                    <th>State</th>
                    <th>Count</td>
                    <th>Node Pool</td>
                </tr>
            </thead>
            <tbody>
            </tbody>
        <table>
        </div>
        """

    # TODO: sort by name/state/node
    render: ->
        @$el.html @template()
        @renderFilter()
        @renderList()
        @renderRefresh()
        makeTooltips(@$el)
        @

    renderList:  =>
        models = @model.filter(@model.filterModel.createFilter())
        entry = (model) -> new ServiceListEntryView(model: model).render().el
        @$('tbody').html(entry(model) for model in models)

    renderRefresh: ->
        @$('#refresh').html(@refreshView.render().el)

    renderFilter: ->
        @$('#filter-bar').html(@filterView.render().el)

    filter: (prefix) ->
        @renderList @model.filter((job) -> _.str.startsWith(job.get('name'), prefix))


class ServiceListEntryView extends ClickableListEntry

    initialize: (options) =>
        @listenTo(@model, "change", @render)

    tagName: "tr"

    className: "clickable"

    template: _.template """
        <td><a href="#service/<%= name %>"><% print(formatName(name)) %></a></td>
        <td><% print(formatState(state)) %></td>
        <td>
            <span class="label label-inverse">
                <%= live_count %> / <%= count %></span>
        </td>
        <td><% print(displayNodePool(node_pool)) %></td>
        """

    render: ->
        @$el.html @template(@model.attributes)
        @


class window.ServiceView extends Backbone.View

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
                    <small>Service</small>
                    <%= name %>
                    <span id="refresh"></span>
                </h1>
            </div>
            <div class="span8 outline-block">
                <h2>Details</h2>
                <table class="table details">
                    <tr><td class="span2">Count</td>
                        <td><span class="label label-inverse">
                            <%= live_count %> / <%= count %></span>
                        </td></tr>
                    <tr><td>Node Pool</td>
                        <td><% print(displayNodePool(node_pool)) %></td></tr>
                    <tr><td>State</td>
                        <td><% print(formatState(state)) %></td></tr>
                    <tr><td>Command</td>    <td><code><%= command %></code></td></tr>
                    <tr><td>Restart Delay</td>
                        <td>
                            <% if (restart_delay) { %>
                                <span class=""><%= restart_delay %></span>
                                seconds
                            <% } else { %>
                                <span class="label info">none</span>
                            <% } %>
                        </td></tr>
                    <tr><td>Monitor Interval</td>
                        <td>
                            <span class=""><%= monitor_interval %></span>
                            seconds
                        </td></tr>
                </table>
            </div>
            <div class="span4 outline-block">
               <h2>Events</h2>
                 <table id="event-list" class="table table-hover">
                   <tbody>
                   </tbody>
                 </table>
            </div>

            <% if (instances.length > 0) { %>
            <div class="span12 outline-block">
                <h2>Instances</h2>
                <table class="table table-outline">
                    <thead class="sub-header">
                        <tr>
                            <th>Id</th>
                            <th>State</th>
                            <th>Node</th>
                            <th></th>
                        </tr>
                    </thead>
                    <tbody class="instances">
                    </tbody>
                </table>
            </div>
            <% } %>

        </div>
        """

    renderEvents: (data) =>
        entry = (event) ->
            new MinimalEventListEntryView(model: new TronEvent(event)).render().el
        @$('#event-list tbody').html(entry(model) for model in data)

    renderInstances: (data) =>
        entry = (inst) ->
            new ServiceInstanceView(model: new ServiceInstance(inst)).render().el
        @$('tbody.instances').html(entry(model) for model in data)

    render: ->
        @$el.html @template(@model.attributes)
        @renderInstances(@model.get('instances'))
        @renderEvents(@model.get('events'))
        @$('#refresh').html(@refreshView.render().el)
        makeTooltips(@$el)
        @


class ServiceInstanceView extends Backbone.View

    tagName: "tr"

    template: _.template """
        <td><% print(formatName(id)) %></td>
        <td><% print(formatState(state)) %></td>
        <td><% print(displayNode(node)) %></td>
        <td>
        <% if (failures.length) { %>
          <pre><%= failures %></pre>
        <% } %>
        </td>
        """

    render: ->
        @$el.html @template(@model.attributes)
        @
