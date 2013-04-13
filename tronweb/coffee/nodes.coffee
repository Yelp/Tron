

class NodeModel extends Backbone.Model



class NodePoolModel extends Backbone.Model



class NodeInlineView extends Backbone.View

    tagName: "span"

    template: _.template """
        <span class="tt-enable" title="<%= username %>@<%= hostname %>">
            <%= name %>
        </span>
    """

    render: =>
        @$el.html @template(@model.attributes)
        # TODO: this isn't working
        makeTooltips(@$el)
        @


window.displayNode = (node) ->
    new NodeInlineView(model: new NodeModel(node)).render().$el.html()
