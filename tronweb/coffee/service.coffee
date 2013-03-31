
# Services

class window.Service extends Backbone.Model

    idAttribute: "name"

    urlRoot: "/services"

    initialize: (options) =>
        super options
        options = options || {}
        @refreshModel = options.refreshModel


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
        <table class="table table-hover">
            <thead>
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
        """

    # TODO: sort by name/state/node
    render: ->
        @$el.html @template()
        @renderFilter()
        @renderList()
        @renderRefresh()
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

    className: =>
        stateName = switch @model.attributes.state
            when "disabled" then 'info'
            when "failed"   then 'error'
            when "degraded" then 'warning'
            when "up"       then 'success'
        "#{ stateName } clickable"

    template: _.template """
        <td><a href="#service/<%= name %>"><%= name %></a></td>
        <td><%= state %>
        <td><%= live_count %> / <%= count %></td>
        <td><%= node_pool %></td>
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
            <div class="span12">
                <h2>Details</h2>
                <table class="table table-condensed details">
                    <tr><td class="span3">Count</td>
                        <td><%= live_count %> / <%= count %></td></tr>
                    <tr><td>Node Pool</td>  <td><%= node_pool %></td></tr>
                    <tr><td>State</td>      <td><%= state %></td></tr>
                    <tr><td>Command</td>    <td><code><%= command %></code></td></tr>
                    <tr><td>Restart Delay</td><td><%= restart_interval %></td></tr>
                    <tr><td>Monitor Interval</td><td><%= monitor_interval %></td></tr>
                </table>
            </div>

            <% if (instances.length > 0) { %>
            <div class="span12">
                <h2>Instances</h2>
                <table class="table">
                    <thead>
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

    render: ->
        @$el.html @template(@model.attributes)
        entry = (inst) -> new ServiceInstanceView(model:inst).render().el
        @$('tbody.instances').append(entry(model) for model in @model.get('instances'))
        @$('#refresh').html(@refreshView.render().el)
        @


class ServiceInstanceView extends Backbone.View

    tagName: "tr"

    className: ->
        switch @model.state
            when "failed"   then 'error'
            when "up"       then 'success'
            when "starting" then 'info'

    template: _.template """
        <td><%= id %></td>
        <td><%= state %></td>
        <td><%= node %></td>
        <td><%= failures %></td>
        """

    render: ->
        @$el.html @template(@model)
        @
