
# Routes
window.modules = window.modules || {}
module = window.modules.routes = {}

class module.TronRoutes extends Backbone.Router

    routes:
        "":                         "index"
        "home(;*params)":           "home"
        "dashboard(;*params)":      "dashboard"
        "jobs(;*params)":           "jobs"
        "job/:name":                "job"
        "job/:job_name/:run_num":   "jobrun"
        "job/:name/:run/:action":   "actionrun"
        "services(;*params)":       "services"
        "service/:name":            "service"
        "configs":                  "configs"
        "config/:name":             "config"

    updateMainView: (model, viewType) ->
        view = new viewType(model: model)
        model.fetch()
        mainView.updateMain(view)

    index: ->
        @navigate('home', trigger: true)

    home: (params) ->
        model = new Dashboard
            filterModel: new DashboardFilterModel(module.getParamsMap(params))
        @updateMainView(model, DashboardView)

    dashboard: (params) ->
        mainView.close()
        model = new Dashboard
            filterModel: new DashboardFilterModel(module.getParamsMap(params))
        dashboard = new DashboardView(model: model)
        model.fetch()
        mainView.updateFullView dashboard.render()

    configs: ->
        @updateMainView(new NamespaceList(), NamespaceListView)

    config: (name) ->
        @updateMainView(new Config(name: name), ConfigView)

    services: (params) ->
        collection = new ServiceCollection([],
            refreshModel: new RefreshModel(),
            filterModel: new FilterModel(module.getParamsMap(params)))
        @updateMainView(collection, ServiceListView)

    service: (name) ->
        refreshModel = new RefreshModel()
        @updateMainView(
            new Service(name: name, refreshModel: refreshModel),
            ServiceView)

    jobs: (params) ->
        collection = new JobCollection([],
            refreshModel: new RefreshModel(),
            filterModel: new JobListFilterModel(module.getParamsMap(params)))
        @updateMainView(collection, JobListView)

    job: (name) ->
        refreshModel = new RefreshModel()
        @updateMainView(new Job(name: name, refreshModel: refreshModel), JobView)

    jobrun: (name, run) ->
        model = new JobRun(
            name: name, run_num: run, refreshModel: new RefreshModel())
        @updateMainView(model, JobRunView)

    actionrun: (name, run, action) ->
        model = new modules.actionrun.ActionRun(
            job_name: name
            run_num: run
            action_name: action
            refreshModel: new RefreshModel())
        historyCollection = new modules.actionrun.ActionRunHistory([],
            job_name: name
            action_name: action)
        view = new modules.actionrun.ActionRunView(
            model: model
            history: historyCollection)
        model.fetch()
        historyCollection.fetch()
        mainView.updateMain(view)


class NavView extends Backbone.View

    initialize: (options) ->
        #@listenTo(@model, "sync", @renderTypeahead)

    tagName: "div"

    className: "navbar navbar-inverse navbar-static-top"

    attributes:
        id: "menu"

    template: """
          <div class="navbar-inner">
            <div class="container">
            <ul class="nav">
              <li class="brand">Tronweb</li>
              <li class="divider-vertical"></li>
              <li><a href="#home">
                <i class="icon-th icon-white"></i>Dashboard</a>
              </li>
              <li><a href="#jobs">
                <i class="icon-time icon-white"></i>Scheduled Jobs</a>
              </li>
              <li><a href="#services">
                <i class="icon-repeat icon-white"></i>Services</a>
              </li>
              <li><a href="#configs">
                <i class="icon-wrench icon-white"></i>Config</a>
              </li>
            </ul>

            <form class="navbar-search pull-right">
            </form>

            </div>
          </div>
    """

    typeaheadTemplate: """
        <input type="text" class="input-medium search-query typeahead"
            placeholder="Jump to view"
            autocomplete="off"
            data-provide="typeahead">
        <div class="icon-search"></div>
    """

    render: =>
        @$el.html @template
        @renderTypeahead()
        @

    updater: (item) =>
        entry = @model.get(item)
        routes.navigate(entry.getUrl(), trigger: true)
        entry.name

    source: (query, process) =>
        (entry.name for _, entry of @model.attributes)

    highlighter: (item) =>
        typeahead = @$('.typeahead').data().typeahead
        name = module.typeahead_hl.call(typeahead, item)
        console.log(item)
        entry = @model.get(item)
        # TODO: truncate sides if name is too long
        "<small>#{entry.type}</small> #{name}"

    # TODO: new sorter which sorts shorter names first
    # TODO: move all typeahead to its own module (maybe nav too)
    renderTypeahead: =>
        @$('.navbar-search').html @typeaheadTemplate
        @$('.typeahead').typeahead
            source: @source,
            updater: @updater
            highlighter: @highlighter
        @

    setActive: =>
        @$('li').removeClass 'active'
        [path, params] = module.getLocationParams()
        path = path.split('/')[0]
        @$("a[href=#{path}]").parent('li').addClass 'active'

Typeahead = $.fn.typeahead.Constructor.prototype

Typeahead.show = ->
    top = @$element.position().top + @$element[0].offsetHeight + 1
    @$menu.insertAfter(@$element).css(top: top).show()
    @shown = true
    @

module.typeahead_hl = $.fn.typeahead.Constructor.prototype.highlighter


class MainView extends Backbone.View

    initialize: (options) ->
       @navView = new NavView(model: @model)

    el: $("#all-view")

    template: """
        <div id="nav"></div>
        <div class="container">
            <div id="main" class="row">
            </div>
        </div>
        """

    updateMain: (view) =>
        @close()
        @renderNav() if @$('#nav').html() == ''
        @navView.setActive()
        @$('#main').html view.el

    updateFullView: (view) =>
        @$('#nav').html ''
        @$('#main').html view.el

    render: =>
        @$el.html @template
        @renderNav()
        @

    renderNav: =>
        console.log('rendering nav')
        @$('#nav').html @navView.render().el

    close: =>
        @trigger('closeView')


module.splitKeyValuePairs = (pairs) ->
    _.mash(param.split('=') for param in pairs)

module.getParamsMap = (paramString) ->
    paramString = paramString || ""
    module.splitKeyValuePairs(paramString.split(';'))

module.getLocationHash = ->
    document.location.hash

module.getLocationParams = ->
    parts = module.getLocationHash().split(';')
    [parts[0], module.splitKeyValuePairs(parts[1..])]


module.buildLocationString = (base, params) ->
    params = (pair.join('=') for pair in _.pairs(params) when pair[1]).join(';')
    "#{ base };#{ params }"


module.updateLocationParam = (name, value) ->
    [base, params] = module.getLocationParams()
    params[name] = value
    routes.navigate(module.buildLocationString(base, params))


window.attachRouter = () ->
    $(document).ready ->

        window.routes = new modules.routes.TronRoutes()
        model = modules.models = new modules.models.QuickFindModel()
        window.mainView = new MainView(model: model).render()
        model.fetch()
        Backbone.history.start(root: "/web/")
