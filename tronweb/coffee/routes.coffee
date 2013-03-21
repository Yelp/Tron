
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

    home: ->
        window.mainView.clear()

    updateMainView: (model, view_type) ->
        view = new view_type(model: model)
        model.fetch()
        window.mainView.render(view)

    configs: ->
        @updateMainView(new NamespaceList(), NamespaceListView)

    config: (name) ->
        @updateMainView(new Config(name: name), ConfigView)

    services: ->
        @updateMainView(new ServiceCollection(), ServiceListView)

    service: (name) ->
        @updateMainView(new Service(name: name), ServiceView)

    jobs: ->
        @updateMainView(new JobCollection(), JobListView)

    job: (name) ->
        @updateMainView(new Job(name: name), JobView)

    jobrun: (name, run) ->
        @updateMainView(new JobRun(name: name, run_num: run), JobRunView)


class MainView extends Backbone.View

    el: $("#main")

    render: (item) =>
        @$el.html(item.el)

    clear: =>
        @$el.html('')


$(document).ready ->

    window.routes = new TronRoutes()
    window.mainView = new MainView()
    Backbone.history.start(root: "/web/")
