
# Events

class window.TronEvent extends Backbone.Model



class window.MinimalEventListEntryView extends Backbone.View

    tagName: "tr"

    template: _.template """
        <td><%= dateFromNow(time) %></td>
        <td>
          <span class="label <%= level %>">
            <%= name %>
          </span>
        </td>
        """

    render: ->
        @$el.html @template(@model.attributes)
        makeTooltips(@$el)
        @
