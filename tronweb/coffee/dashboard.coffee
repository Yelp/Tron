
# Dashboard

class window.Dashboard extends Backbone.Model

    initialize: (options)->
        options = options || {}
        @refreshModel = new RefreshModel(interval: 30)
        @filterModel = options.filterModel
        @serviceList = new ServiceCollection()
        @jobList = new JobCollection()
        @listenTo(@serviceList, "sync", @change)
        @listenTo(@jobList, "sync", @change)

    fetch: =>
        @serviceList.fetch()
        @jobList.fetch()

    change: (args) ->
        @trigger("change", args)

    models: =>
        @serviceList.models.concat @jobList.models

    sorted: =>
        _.sortBy(@models(), (item) -> item.get('name'))

    filter: (filter) =>
        _.filter(@sorted(), filter)


matchType = (item, query) ->
    console.log(item)
    switch query
        when 'service' then true if item instanceof Service
        when 'job' then true if item instanceof Job


class window.DashboardFilterModel extends FilterModel

    filterTypes:
        name:       buildMatcher(fieldGetter('name'), matchAny)
        type:       buildMatcher(_.identity, matchType)


class window.DashboardView extends Backbone.View

    initialize: (options) =>
        @refreshView = new RefreshToggleView(model: @model.refreshModel)
        @filterView = new FilterView(model: @model.filterModel)
        @listenTo(@model, "change", @render)
        @listenTo(@refreshView, 'refreshView', => @model.fetch())
        @listenTo(@filterView, "filter:change", @renderBoxes)

    tagName: "div"

    className: "span12 dashboard-view"

    template: _.template """
        <h1>
            <small>Tron</small>
            <a href="#dashboard">Dashboard</a>
            <span id="refresh"></span>
        </h1>
        <div id="filter-bar"></div>
        <div id="status-boxes">
        </div>
        """

    makeView: (model) =>
        switch model.constructor.name
            when Service.name
                new ServiceStatusBoxView(model: model)
            when Job.name
                new JobStatusBoxView(model: model)

    renderRefresh: ->
        @$('#refresh').html(@refreshView.render().el)

    renderBoxes: =>
        models = @model.filter(@model.filterModel.createFilter())
        views = (@makeView(model) for model in models)
        @$('#status-boxes').html(item.render().el for item in views)

    render: ->
        @$el.html @template()
        @$('#filter-bar').html(@filterView.render().el)
        @renderBoxes()
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

    getState: =>
        @model.get('state')

    count: =>
        @model.get('instances').length


class window.JobStatusBoxView extends StatusBoxView

    buildUrl: =>
        "#job/#{@model.get('name')}"

    icon: "icon-time"

    # TODO: get state of last run if enabled
    getState: =>
        @model.get('status')

    count: =>
        if @model.get('runs') then _.first(@model.get('runs')).run_num else 0
