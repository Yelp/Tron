
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

    configs: ->
        namespaces = new NamespaceList()
        view = new NamespaceListView(model: namespaces)
        namespaces.fetch()
        window.mainView.render(view)

    config: (name) ->
        config = new Config(name: name)
        view = new ConfigView(model: config)
        config.fetch()
        window.mainView.render(view)


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
