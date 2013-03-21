
# Services

class Service extends Backbone.Model

    idAttribute: "name"


class window.ServiceCollection extends Backbone.Collection

    model: Service

    url: "/services"

    parse: (resp, options) =>
        resp['services']


class window.ServiceListView extends Backbone.View

    initialize: (options) =>
        @listenTo(@model, "sync", @render)
        @listenTo(@model, "sort", @render)

    tagName: "div"

    className: "span12"

    # TODO: filter by name
    template: _.template '
        <h1>Services</h1>
        <table class="table table-hover">
            <thead>
                <tr>
                    <th>Name</td>
                    <th>Count</td>
                    <th>Node Pool</td>
                </tr>
            </thead>
            <tbody>
            </tbody>
        <table>'

    # TODO: sort by name/state/node
    render: ->
        @$el.html @template()
        entry = (model) -> new ServiceView(model: model).render().el
        @$('tbody').append(entry(model) for model in @model.models)
        @


class window.ServiceView extends Backbone.View

    initialize: (options) =>
        @listenTo(@model, "change", @render)

    tagName: "tr"

    className: =>
        switch @model.attributes.state
            when "DISABLED" then 'info'
            when "FAILED"   then 'error'
            when "DEGRADED" then 'warning'
            when "UP"       then 'success'

    template: _.template '
        <td><a href="#service/<%= name %>"><%= name %></a></td>
        <td><%= live_count %> / <%= count %></td>
        <td><%= node_pool %></td>'

    render: ->
        @$el.html @template(@model.attributes)
        @
