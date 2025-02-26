# Routes
# This file defines the URL structure and navigation flow for Tronweb.
#
# Flow:
# 1. When a URL changes (e.g., #job/name), Backbone matches it to a route handler
# 2. The route handler creates appropriate models (i.e. from job.coffee, etc.) and views
# 3. The route handler fetches data from the API
# 4. Returned data is rendered in the view
window.modules = window.modules || {}
module = window.modules.routes = {}

# Backbone router that handles URL navigation and view management
# Routes follow the pattern: "url/:parameter": "handlerMethod".
# E.g. navigating to .../#job/my-job-name will call the job method
# with name = "my-job-name", and this feeds into updateMainView.
class module.TronRoutes extends Backbone.Router

    routes:
        "":                         "index"
        "home(;*params)":           "home"
        "dashboard(;*params)":      "dashboard"
        "jobs(;*params)":           "jobs"
        "job/:name":                "job"
        "job/:job_name/:run_num":   "jobrun"
        "job/:name/:run/:action":   "actionrun"
        "configs":                  "configs"
        "config/:name":             "config"

    # Create a view based on the provided model and viewType. Then, trigger
    # the model's API call and update the main view with returned content.
    updateMainView: (model, viewType) ->
        view = new viewType(model: model)
        model.fetch()
        mainView.updateMain(view)

    index: ->
        @navigate('home', trigger: true)

    home: (params) ->
        model = new Dashboard
            filterModel: new DashboardFilterModel(module.getParamsMap(params))
        document.title = "home"
        @updateMainView(model, DashboardView)

    dashboard: (params) ->
        mainView.close()
        model = new Dashboard
            filterModel: new DashboardFilterModel(module.getParamsMap(params))
        dashboard = new DashboardView(model: model)
        model.fetch()
        document.title = "dashboard"
        mainView.updateFullView dashboard.render()

    configs: ->
        document.title = "configs"
        @updateMainView(new NamespaceList(), NamespaceListView)

    config: (name) ->
        document.title = "config #{name}"
        @updateMainView(new Config(name: name), ConfigView)

    jobs: (params) ->
        collection = new JobCollection([],
            refreshModel: new RefreshModel(),
            filterModel: new JobListFilterModel(module.getParamsMap(params)))
        document.title = "jobs"
        @updateMainView(collection, JobListView)

    job: (name) ->
        refreshModel = new RefreshModel()
        document.title = "#{name}"
        @updateMainView(new Job(name: name, refreshModel: refreshModel), JobView)

    jobrun: (name, run) ->
        model = new JobRun(
            name: name, run_num: run, refreshModel: new RefreshModel())
        document.title = "#{name}.#{run}"
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
        document.title = "#{name}.#{run}.#{action}"
        mainView.updateMain(view)

# Main view that manages our overall page structure
class MainView extends Backbone.View

    initialize: (options) ->
       @navView = new modules.navbar.NavView(model: @model)

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
        @$('#nav').html @navView.render().el

    close: =>
        @trigger('closeView')


# URL parameter helpers
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
