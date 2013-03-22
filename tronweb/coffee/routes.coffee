
# Routes

class TronRoutes extends Backbone.Router

    routes:
        "home":             "home"
        "jobs":             "jobs"
        "job/:name":        "job"
        "job/:name/:run":   "jobrun"
        "services":         "services"
        "service/:name":    "service"
        "configs":          "configs"
        "config/:name":     "config"

    updateMainView: (model, view_type) ->
        view = new view_type(model: model)
        model.fetch()
        mainView.render(view)

    home: ->
        @updateMainView(new Dashboard(), DashboardView)

    configs: ->
        @updateMainView(new NamespaceList(), NamespaceListView)

    config: (name) ->
        @updateMainView(new Config(name: name), ConfigView)

    services: ->
        refreshModel = new RefreshModel()
        @updateMainView(new ServiceCollection(refresh: refreshModel), ServiceListView)

    service: (name) ->
        @updateMainView(new Service(name: name), ServiceView)

    jobs: ->
        refreshModel = new RefreshModel()
        @updateMainView(new JobCollection(refresh: refreshModel), JobListView)

    job: (name) ->
        refreshModel = new RefreshModel()
        @updateMainView(new Job(name: name, refresh: refreshModel), JobView)

    jobrun: (name, run) ->
        refreshModel = new RefreshModel()
        @updateMainView(new JobRun(name: name, run_num: run, refresh: refreshModel), JobRunView)


class MainView extends Backbone.View

    el: $("#main")

    render: (item) =>
        @trigger('closeView')
        breadcrumbView.clear()
        @$el.html(item.el)

    clear: =>
        breadcrumbView.clear()
        @$el.empty()


$(document).ready ->

    window.routes = new TronRoutes()
    window.mainView = new MainView()
    window.breadcrumbView = new BreadcrumbView()
    Backbone.history.start(root: "/web/")
