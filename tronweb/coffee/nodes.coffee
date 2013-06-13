

class NodeModel extends Backbone.Model



class NodePoolModel extends Backbone.Model



class NodeInlineView extends Backbone.View

    tagName: "span"

    template: _.template """
        <span class="tt-enable" title="<%= username %>@<%= hostname %>:<%= port %>">
            <%= name %>
        </span>
    """

    render: =>
        @$el.html @template(@model.attributes)
        @


class NodePoolInlineView extends Backbone.View

    tagName: "span"

    template: _.template """
        <span class="tt-enable" title="<%= nodes.length %> node(s)">
            <%= name %>
        </span>
    """

    render: =>
        @$el.html @template(@model.attributes)
        @


window.displayNode = (node) ->
    new NodeInlineView(model: new NodeModel(node)).render().$el.html()


window.displayNodePool = (pool) ->
    new NodePoolInlineView(model: new NodePoolModel(pool)).render().$el.html()
