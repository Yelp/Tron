
# Routes

class TronRoutes extends Backbone.Router

    routes:
        "":                         "index"
        "home":                     "home"
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

    el: $("#main")

    render: (item) =>
        @trigger('closeView')
        @$el.html(item.el)


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
