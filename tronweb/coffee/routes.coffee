
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
        @listenTo(@model, "sync", @setTypeahead)

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
              <input type="text" class="input-medium search-query typeahead"
                placeholder="search"
                autocomplete="off"
                data-provide="typeahead">
              <div class="icon-search"></div>
            </form>

            </div>
          </div>
    """

    render: =>
        @$el.html @template
        @

    updater: (item) ->
        item = _.find(@source, (e) -> e.name == item)
        routes.navigate(item.getUrl(), trigger: true)
        item.name

    # TODO: this breaks if search is used before it has data
    setTypeahead: =>
        @$('.typeahead').typeahead(
            source: @model.get('index'),
            updater: @updater)
        @

    setActive: =>
        @$('li').removeClass 'active'
        [path, params] = module.getLocationParams()
        path = path.split('/')[0]
        @$("a[href=#{path}]").parent('li').addClass 'active'


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
