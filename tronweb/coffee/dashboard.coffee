
# Dashboard

class window.Dashboard extends Backbone.Model

    initialize: ->
        @eventList = new EventList(refresh: new RefreshModel(interval: 10))
        @listenTo(@eventList, "sync", @change)

    fetch: ->
        @eventList.fetch()

    change: (args) ->
        @trigger("change", args)


class Event extends Backbone.Model


class window.EventList extends Backbone.Collection

    initialize: (options) =>
        super options
        @refresh = options.refresh

    model: Event

    url: "/events"

    parse: (resp, options) ->
        resp['data']

    comparator: (event) ->
        event.get('time')

    recent: (seconds) ->
        _.last(@models, 15).reverse()
#        seconds = seconds || 5
#        func = (event) ->
#            moment().diff(moment(event.get('time')), 'seconds') < seconds
#        _.takeWhile(@models.reverse(), func)


class window.EventListView extends Backbone.View

    initialize: (options) =>
        super options
        @refreshView = new RefreshToggleView(model: @model.refresh)
        @listenTo(@model, "sync", @render)
        @listenTo(@refreshView.model, 'refresh', @update)

    tagName: "div"

    className: ""

    template: _.template """
        <h2>Recent Events</h2>
        <table class="table table-hover">
          <thead>
            <tr>
              <th>Time</th>
              <th>Entity</th>
              <th>Event</th>
            </tr>
          </thead>
          <tbody>
          </tbody>
        </table>
        """

    # TODO: why doesn't this work directly ?
    update: (name) =>
        @model.fetch()

    render_list: (models) =>
        entry = (event) -> new EventListEntryView(model: event).render().el
        @$('tbody').html(entry(model) for model in models)

    # TODO: filter by time
    render: =>
        @$el.html @template()
        @render_list @model.recent()
        @$('h2').append(@refreshView.render().el)
        @


class EventListEntryView extends Backbone.View

    tagName: "tr"

    className: ->
        switch @model.get('level')
            when "OK"       then "success"
            when "INFO"     then "info"
            when "NOTICE"   then "warning"
            when "CRITICAL" then "error"

    template: _.template """
        <td><% print(dateFromNow(time)) %></td>
        <td><%= entity %></td>
        <td><%= name %></td>
        """

    render: ->
        @$el.html @template(@model.attributes)
        makeTooltips(@$el)
        @


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
