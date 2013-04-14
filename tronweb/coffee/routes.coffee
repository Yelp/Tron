
# Routes

class TronRoutes extends Backbone.Router

    routes:
        "":                         "index"
        "home":                     "home"
        "dashboard":                "dashboard"
        "jobs(;*params)":           "jobs"
        "job/:name":                "job"
        "job/:name/:run":           "jobrun"
        "job/:name/:run/:action":   "actionrun"
        "services(;*params)":       "services"
        "service/:name":            "service"
        "configs":                  "configs"
        "config/:name":             "config"

    updateMainView: (model, viewType) ->
        view = new viewType(model: model)
        model.fetch()
        mainView.render(view)

    index: ->
        @navigate('home', trigger: true)

    home: ->
        @updateMainView(new Dashboard(), DashboardView)

    dashboard: ->
        mainView.close()
        model = new Dashboard()
        dashboard = new DashboardView(model: model)
        model.fetch()
        $('#all-view').html dashboard.render().el

    configs: ->
        @updateMainView(new NamespaceList(), NamespaceListView)

    config: (name) ->
        @updateMainView(new Config(name: name), ConfigView)

    services: (params) ->
        collection = new ServiceCollection(
            refreshModel: new RefreshModel(),
            filterModel: new FilterModel(getParamsMap(params)))
        @updateMainView(collection, ServiceListView)

    service: (name) ->
        refreshModel = new RefreshModel()
        @updateMainView(
            new Service(name: name, refreshModel: refreshModel),
            ServiceView)

    jobs: (params) ->
        collection = new JobCollection(
            refreshModel: new RefreshModel(),
            filterModel: new JobListFilterModel(getParamsMap(params)))
        @updateMainView(collection, JobListView)

    job: (name) ->
        refreshModel = new RefreshModel()
        @updateMainView(new Job(name: name, refreshModel: refreshModel), JobView)

    jobrun: (name, run) ->
        model = new JobRun(
            name: name, run_num: run, refreshModel: new RefreshModel())
        @updateMainView(model, JobRunView)

    actionrun: (name, run, action) ->
        model = new ActionRun(
            job_name: name
            run_num: run
            action_name: action
            refreshModel: new RefreshModel())
        @updateMainView(model, ActionRunView)


class MainView extends Backbone.View

    el: $("body")

    template: _.template """
        <div id="menu" class="navbar navbar-inverse navbar-static-top">
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
            </div>
          </div>
        </div>

        <div id="main" class="container">
        </div>
    """

    setActive: =>
        [path, params] = getLocationParams()
        path = path.split('/')[0]
        @$("a[href=#{path}]").parent('li').addClass 'active'

    render: (item) =>
        @close()
        @$('#all-view').html @template()
        @setActive()
        @$('#main').html item.el

    close: =>
        @trigger('closeView')


splitKeyValuePairs = (pairs) ->
    _.mash(param.split('=') for param in pairs)

getParamsMap = (paramString) ->
    paramString = paramString || ""
    splitKeyValuePairs(paramString.split(';'))


getLocationParams = ->
    parts = document.location.hash.split(';')
    [parts[0], splitKeyValuePairs(parts[1..])]


buildLocationString = (base, params) ->
    params = (pair.join('=') for pair in _.pairs(params) when pair[1]).join(';')
    "#{ base };#{ params }"


window.updateLocationParam = (name, value) ->
    [base, params] = getLocationParams()
    params[name] = value
    routes.navigate(buildLocationString(base, params))


$(document).ready ->

    window.routes = new TronRoutes()
    window.mainView = new MainView()
    Backbone.history.start(root: "/web/")
