
# Action graph using d3

class window.GraphView extends Backbone.View

    el: "#action-graph"

    initialize: (options) =>
        options = options || {}
        @height = options.height || 250
        @width = options.width || @$el.width()
        @linkDistance = options.linkDistance || 80
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
            .attr("markerWidth",  15)
            .attr("markerHeight", 30)
            .attr("orient", "auto")
            .append("svg:path")
            .attr("d", "M 0 2 L 10 5 L 0 8 z")

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
            .attr("r", 6)

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
            .charge(-400)
            .theta(1)
            .linkDistance(@linkDistance)
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

    events:
        'click #view-full-screen':   'toggleModal'

    toggleModal: (event) ->
        $('.modal').modal('toggle')

    attachEvents: =>
        @$('.modal').on('show', @showModal)
        @$('.modal').on('hide', @removeGraph)

    template: """
        <div class="top-right-corner">
        <button class="btn btn-clear tt-enable"
                title="Full view"
                data-placement="top"
                id="view-full-screen"
            >
            <i class="icon-opennewwindow icon-white"></i>
        </button>
        </div>
        <div class="modal hide fade">
            <div class="modal-header">
                <button class="btn btn-clear"
                    data-dismiss="modal"
                    aria-hidden="true">
                    <i class="icon-circledown icon-white"></i>
                </button>
                <h3>
                    <i class="icon-barchart icon-white"></i>
                    Action Graph
                </h3>
            </div>
            <div class="modal-body graph job-view">
            </div>
        </div>
        """

    showModal: (event) =>
        # prevent firing on child events
        return if event.target != $('.modal')[0]
        options = _.extend {},
            @graphOptions,
            model:          @model
            el:             @$('.modal-body.graph').html('').get()
            height:         $(window).height() - 130
            width:          $(document).width() - 150
            linkDistance:   250
            showZoom:       false

        graph = new GraphView(options).render()

    removeGraph: (event) =>
        return if event.target != $('.modal')[0]
        @$('.modal-body.graph').empty()

    render: =>
        @$el.append(@template)
        @attachEvents()
        @delegateEvents()
        @
