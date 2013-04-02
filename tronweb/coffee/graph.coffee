
# Action graph using d3

class window.GraphView extends Backbone.View

    el: "#action-graph"

    initialize: (options) =>
        options = options || {}
        @height = options.height || 250
        @width = options.width || @$el.width()
        @showZoom = if options.showZoom? then options.showZoom else true
        @buildContent = options.buildContent
        @nodeClass = options.nodeClass || "node"

    buildNodeMap: (data) =>
        nodes = {}
        for node in data
            nodes[node.name] = node
        nodes

    getLinks: (data) =>
        nodes = @buildNodeMap(data)
        nested = for node in data
            ({source: node, target: nodes[target]} for target in node.dependent)
        _.flatten(nested)

    buildSvgLinks: (links) =>
        @svg.append("svg:defs")
            .append("svg:marker")
            .attr("id", "arrow")
            .attr("viewBox", "0 0 10 10")
            .attr("refX", 16)
            .attr("refY", 5)
            .attr("markerUnits", "strokeWidth")
            .attr("markerWidth", 7)
            .attr("markerHeight", 7)
            .attr("orient", "auto")
            .append("svg:path")
            .attr("d", "M 0 0 L 10 5 L 0 10 z")

        @link = @svg.selectAll(".link")
            .data(links)
            .enter().append("line")
            .attr("class", "link")
            .attr("marker-end", "url(#arrow)")

    buildSvgNodes: (data)=>
        @node = @svg.selectAll(".node")
            .data(data)
            .enter().append("svg:g")
            .call(@force.drag)
            .attr
                class: @nodeClass
                'data-title': (d) -> d.name
                'data-html': true
                'data-content': @buildContent

        @node.append("svg:circle")
            .attr("r", "0.5em")

        @node.append("svg:text")
            .attr(dx: 12, dy: "0.25em")
            .text((d) -> d.name)

    attachEvents: =>
        $('.node').popover
            container: @$el
            placement: 'top'
            trigger: 'hover'

        @force.on "tick", =>
            @link.attr("x1", (d) -> d.source.x)
                .attr("y1", (d) -> d.source.y)
                .attr("x2", (d) -> d.target.x)
                .attr("y2", (d) -> d.target.y)

            @node.attr("transform", (d) -> "translate(#{d.x}, #{d.y})")

    addNodes: (data) ->
        @force.nodes data

    addLinks: (links) =>
        @force.links links

    buildForce: (height, width) ->
        # TODO: randomly move nodes when links cross
        @force = d3.layout.force()
            .charge(-500)
            .theta(1)
            .linkDistance(100)
            .size([width, height])

    buildSvg: (height, width) ->
        @svg = d3.select(@el)
            .append("svg")
            .attr
                height: height
                width: width

    render: =>
        @buildForce(@height, @width)
        @buildSvg(@height, @width)
        @addNodes(@model)
        links = @getLinks(@model)
        @addLinks(links)
        @force.start()
        @buildSvgLinks(links)
        @buildSvgNodes(@model)
        if @showZoom
            new GraphModalView(el: @el, model: @model, graphOptions: this).render()
        @attachEvents()
        @


class GraphModalView extends Backbone.View

    initialize: (options) =>
        options = options || {}
        @graphOptions = options.graphOptions

    attachEvents: =>
        @$('#view-full').click(@showModal)

    template: """
        <div class="top-right-corner">
        <button class="btn btn-default tt-enable"
                title="Full view"
                data-placement="top"
                id="view-full"
            >
            <i class="icon-resize-full"></i>
        </button>
        </div>
        <div class="modal hide fade">
            <div class="modal-header">
                <button type="button"
                    class="close"
                    data-dismiss="modal"
                    aria-hidden="true">
                    <i class="icon-remove-circle"></i>
                </button>
                <h3>Action Graph</h3>
            </div>
            <div class="modal-body graph job-view">
            </div>
        </div>
        """

    showModal: =>
        options = _.extend {},
            @graphOptions,
            model:      @model
            el:         @$('.modal-body.graph').html('').get()
            height:     $(document).height() - 130
            width:      $(document).width() - 150
            showZoom:   false

        graph = new GraphView(options).render()
        $('.modal').modal()

    render: =>
        @$el.append(@template)
        @attachEvents()
        @
