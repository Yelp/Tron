
# Dashboard

class window.Dashboard extends Backbone.Model

    initialize: ->
        @eventList = new EventList(refresh: new RefreshModel(interval: 10))
        @listenTo(@eventList, "sync", @change)

    fetch: ->
        @eventList.fetch()

    change: (args) ->
        @trigger("change", args)


class window.DashboardView extends Backbone.View

    initialize: (options) =>
        @listenTo(@model, "change", @render)

    tagName: "div"

    className: "row"

    template: _.template """
        <div class="span12"><h1>Dashboard</h1></div>
        <div class="span6"></div>
        <div class="span6" id="events"></div>
        """

    renderEvents: ->
        view = new EventListView(model: @model.eventList)
        @$('#events').html(view.render().el)

    render: ->
        @$el.html @template()
        @renderEvents()
        @
