
# Dashboard

class window.Dashboard extends Backbone.Model

    initialize: ->
        @refreshModel = new RefreshModel(interval: 30)
        @serviceList = new ServiceCollection()
        @jobList = new JobCollection()
        @listenTo(@serviceList, "sync", @change)
        @listenTo(@jobList, "sync", @change)

    fetch: =>
        @serviceList.fetch()
        @jobList.fetch()

    change: (args) ->
        @trigger("change", args)


class window.DashboardView extends Backbone.View

    initialize: (options) =>
        @refreshView = new RefreshToggleView(model: @model.refreshModel)
        @listenTo(@model, "change", @render)
        @listenTo(@refreshView, 'refreshView', => @model.fetch())

    tagName: "div"

    className: "span12 dashboard-view"

    # TODO: filters
    template: _.template """
        <h1>
            <small>Tron</small>
            <a href="#dashboard">Dashboard</a>
            <span id="refresh"></span>
        </h1>
        <div id="status-boxes">
        </div>
        """

    renderServices: =>
        entry = (model) -> new ServiceStatusBoxView(model: model).render().el
        @$('#status-boxes').append(entry(model) for model in @model.serviceList.models)

    renderJobs: =>
        entry = (model) -> new JobStatusBoxView(model: model).render().el
        @$('#status-boxes').append(entry(model) for model in @model.jobList.models)

    renderRefresh: ->
        @$('#refresh').html(@refreshView.render().el)

    render: ->
        @$el.html @template()
        @renderServices()
        @renderJobs()
        @renderRefresh()
        @


class window.StatusBoxView extends ClickableListEntry

    initialize: (options) =>
        @listenTo(@model, "change", @render)

    tagName: "div"

    template: _.template """
        <div class="status-header">
            <a href="<%= url %>"><%= name %></a>
        </div>
        """

    render: =>
        context = _.extend {},
            url: @buildUrl()
            name: formatName(@model.attributes.name)
        @$el.html @template(context)
        @

class window.ServiceStatusBoxView extends StatusBoxView

    buildUrl: =>
        "#service/#{@model.get('name')}"

    className: =>
        state = switch @model.get('state')
            when "up"       then "success"
            when "starting" then "info"
            when "disabled" then "warning"
            when "degraded" then "warning"
            when "failed"   then "error"
        "span2 clickable status-box #{state}"


class window.JobStatusBoxView extends StatusBoxView

    buildUrl: =>
        "#job/#{@model.get('name')}"

    className: =>
        state = switch @model.get('status')
            when "enabled"  then "success"
            when "running"  then "info"
            when "disabled" then "warning"
        "span2 clickable status-box #{state}"
