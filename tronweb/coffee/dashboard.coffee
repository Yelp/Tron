
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

    makeServiceViews: =>
        entry = (model) -> new ServiceStatusBoxView(model: model)
        entry(model) for model in @model.serviceList.models

    makeJobViews: =>
        entry = (model) -> new JobStatusBoxView(model: model)
        entry(model) for model in @model.jobList.models

    sortedViews: =>
        allViews = @makeServiceViews().concat @makeJobViews()
        _.sortBy(allViews, (item) -> item.model.get('name'))

    renderRefresh: ->
        @$('#refresh').html(@refreshView.render().el)

    render: ->
        @$el.html @template()
        @$('#status-boxes').append(item.render().el) for item in @sortedViews()
        @renderRefresh()
        @


class window.StatusBoxView extends ClickableListEntry

    initialize: (options) =>
        @listenTo(@model, "change", @render)

    tagName: "div"

    className: =>
        "span2 clickable status-box #{@getState()}"

    template: _.template """
        <div class="status-header">
            <a href="<%= url %>">
            <%= name %></a>
        </div>
        <span class="count">
          <i class="<%= icon %> icon-white"></i><%= count %>
        </span>
        """

    render: =>
        context = _.extend {},
            url: @buildUrl()
            icon: @icon
            count: @count()
            name: formatName(@model.attributes.name)
        @$el.html @template(context)
        @

class window.ServiceStatusBoxView extends StatusBoxView

    buildUrl: =>
        "#service/#{@model.get('name')}"

    icon: "icon-repeat"

    #  TODO: this is duplicated with ServiceListEntryView
    getState: =>
        switch @model.get('state')
            when "up"       then "success"
            when "starting" then "info"
            when "disabled" then "warning"
            when "degraded" then "warning"
            when "failed"   then "error"

    count: =>
        @model.get('instances').length


class window.JobStatusBoxView extends StatusBoxView

    buildUrl: =>
        "#job/#{@model.get('name')}"

    icon: "icon-time"

    # TODO: get state of last run if enabled
    # TODO: this is duplicated with JobListEntryView
    getState: =>
        switch @model.get('status')
            when "enabled"  then "success"
            when "running"  then "info"
            when "disabled" then "warning"
            when "unknown"  then "error"

    count: =>
        if @model.get('runs') then _.first(@model.get('runs')).run_num else 0
