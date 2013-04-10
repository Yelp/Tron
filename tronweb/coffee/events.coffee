
# Events

class window.TronEvent extends Backbone.Model


class window.EventListView extends Backbone.View

    initialize: (options) =>
        super options
        @refreshView = new RefreshToggleView(model: @model.refresh)
        @listenTo(@model, "sync", @render)
        @listenTo(@refreshView.model, 'refresh', => @model.fetch())

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

    render_list: (models) =>
        entry = (event) -> new EventListEntryView(model: event).render().el
        @$('tbody').html(entry(model) for model in models)

    # TODO: filter by time
    render: =>
        @$el.html @template()
        @render_list @model.recent()
        @$('h2').append(@refreshView.render().el)
        @


class window.EventListEntryView extends Backbone.View

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


class window.MinimalEventListEntryView extends Backbone.View

    tagName: "tr"

    className: ->
        switch @model.get('level')
            when "OK"       then "success"
            when "INFO"     then "info"
            when "NOTICE"   then "warning"
            when "CRITICAL" then "error"

    template: _.template """
        <td><% print(dateFromNow(time)) %></td>
        <td><%= name %></td>
        """

    render: ->
        @$el.html @template(@model.attributes)
        makeTooltips(@$el)
        @
