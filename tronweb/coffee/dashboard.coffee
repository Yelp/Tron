
# Dashboard
#window.modules = window.modules || {}
#window.modules.dashboard = module = {}

class window.Dashboard extends Backbone.Model

    initialize: (options)->
        options = options || {}
        @refreshModel = new RefreshModel(interval: 30)
        @filterModel = options.filterModel
        @jobList = new JobCollection()
        @listenTo(@jobList, "sync", @change)

    fetch: =>
        @jobList.fetch()

    change: (args) ->
        @trigger("change", args)

    models: =>
        @jobList.models

    sorted: =>
        _.sortBy(@models(), (item) -> item.get('name'))

    filter: (filter) =>
        _.filter(@sorted(), filter)


matchType = (item, query) ->
    switch query
        when 'job' then true if item instanceof Job


class window.DashboardFilterModel extends FilterModel

    filterTypes:
        name:       buildMatcher(fieldGetter('name'), matchAny)
        type:       buildMatcher(_.identity, matchType)


class window.DashboardFilterView extends FilterView

    createtype: _.template """
        <div class="input-prepend">
           <i class="icon-markerright icon-grey"></i>
           <div class="filter-select">
             <select id="filter-<%= filterName %>"
                  class="span3"
                  data-filter-name="<%= filterName %>Filter">
              <option value="">All</option>
              <option <%= isSelected(defaultValue, 'job') %>
                  value="job">Scheduled Jobs</option>
            </select>
          </div>
        </div>
    """

class window.DashboardView extends Backbone.View

    initialize: (options) =>
        @refreshView = new RefreshToggleView(model: @model.refreshModel)
        @filterView = new DashboardFilterView(model: @model.filterModel)
        @listenTo(@model, "change", @render)
        @listenTo(@refreshView, 'refreshView', => @model.fetch())
        @listenTo(@filterView, "filter:change", @renderBoxes)

    tagName: "div"

    className: "span12 dashboard-view"

    template: _.template """
        <h1>
            <i class="icon-th icon-white"></i>
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
            when Job.name then new module.JobStatusBoxView(model: model)

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

class module.JobStatusBoxView extends StatusBoxView

    buildUrl: =>
        "#job/#{@model.get('name')}"

    icon: "icon-time"

    # TODO: get state of last run if enabled
    getState: =>
        @model.get('status')

    count: =>
        if _.isEmpty(@model.get('runs')) then 0 else _.first(@model.get('runs')).run_num
