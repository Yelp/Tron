# Action Graph
# This file implements the visualization of Job action_graphs as node-edge diagrams.
#
# Flow:
# 1. Views in job.coffee create GraphView instances with action_graph data
# 2. GraphView renders the directed graph showing actions and their dependencies
# 3. GraphModalView adds full screen functionality
window.modules = window.modules || {}
window.modules.graph = module = {}

module.GraphUtils = {
    getDefaultLayout: ->
        {
            # Dagre is a layout algorithm that positions nodes in a directed graph.
            name: 'dagre'
            # LR (left-to-right) is our default direction, but we can change this to TB (top-to-bottom). I found that LR
            # works a bit better since modern screens are wider than they are tall. That said, some graphs may look better
            # in TB. I think it'd be cool to add a toggle that lets users switch between these two options.
            rankDir: 'LR'
            padding: 30
            fit: true
            nodeSep: 50
            rankSep: 80
        }

    getNodeWidth: (node) ->
        # Use canvas to measure text width accurately. We need to do all this because Cytoscape no longer
        # supports setting the node width as 'label'. See https://github.com/cytoscape/cytoscape.js/issues/2713
        ctx = document.createElement('canvas').getContext("2d")
        fStyle = node.pstyle('font-style').strValue
        size = node.pstyle('font-size').pfValue + 'px'
        family = node.pstyle('font-family').strValue
        weight = node.pstyle('font-weight').strValue
        ctx.font = "#{fStyle} #{weight} #{size} #{family}"

        return ctx.measureText(node.data('name')).width + 20

    # Search based on node name. Highlights matching nodes and their connected edges, dims the rest.
    applySearch: (cy, searchText) ->
        if searchText
            cy.nodes().forEach (node) ->
                nodeName = node.data('name').toLowerCase()
                if nodeName.includes(searchText)
                    node.style('opacity', 1)
                else
                    node.style('opacity', 0.2)

            cy.edges().style('opacity', 0.1)

            matchingNodes = cy.nodes().filter (node) ->
                node.data('name').toLowerCase().includes(searchText)

            matchingNodes.connectedEdges().style('opacity', 0.8)
        else
            # Reset all styles when search is cleared
            cy.nodes().style('opacity', 1)
            cy.edges().style('opacity', 1)

    # Reset the graph to its initial state, including layout and node positions
    resetGraph: (cy) ->
        cy.stop()
        cy.nodes().style('opacity', 1)
        cy.edges().style('opacity', 1)

        # Reset node positions before re-layout
        cy.nodes().positions (node) ->
            return { x: 0, y: 0 }

        layout = cy.layout(module.GraphUtils.getDefaultLayout())
        layout.run()

        # Fit all elements (if no elements are specified it fits all, so we pass undefined) with 25px padding
        cy.fit(undefined, 25)
        cy.center()

        cy.nodes().forEach (node) ->
            node.data 'manuallyPositioned', false

    defaultStylesheet: [
        {
            selector: 'node',
            style: {
                'label': 'data(name)'
                'text-valign': 'center'
                'text-halign': 'center'
                'background-color': '#f8f8f8'
                'color': '#000'
                'font-size': '16px'
                'font-weight': 'bold'
                'shape': 'roundrectangle'
                'width': (node) -> module.GraphUtils.getNodeWidth(node)
                'height': 25
                'padding-top': '5px'
                'padding-bottom': '5px'
                'padding-left': '10px'
                'padding-right': '10px'
                'text-wrap': 'none'
                'border-width': 3
                'border-color': '#999'
            }
        },
        {
            selector: 'edge',
            style: {
                'curve-style': 'bezier'
                'target-arrow-shape': 'triangle'
                'target-arrow-color': '#999'
                'line-color': '#999'
                'width': 2
            }
        },
        # This is unfortunately necessary. For consistency, this duplicates some of the colours and styles defined in our LESS.
        # The LESS classes (.succeeded, .running, etc.) apply to HTML elements, while the styles here apply to the graph's
        # Cytoscape nodes (SVG canvas elements). Those LESS variables only exist during LESS compilation, so we can't use them here.
        #
        # If you change a colour here, you should also change it in the LESS, unless you're trying to be more specific about states
        # in the graph than the state mixins allow.
        #
        # Success states (.success mixin in LESS)
        {
            selector: '.succeeded',
            style: {
                'border-color': '#218E0B'      # @green from LESS
                'background-color': '#f0ffe0'  # Light green
            }
        },
        # Info states (.info mixin in LESS)
        {
            selector: '.running, .starting',
            style: {
                'border-color': '#2F47B8'      # @blue
                'background-color': '#f0f5ff'  # Light blue
            }
        },
        # Warning states (.warning mixin in LESS)
        {
            selector: '.cancelled, .skipped',
            style: {
                'border-color': '#A6790D'      # @yellow
                'background-color': '#fffbf0'  # Light yellow
            }
        },
        # Error states (.error mixin in LESS)
        {
            selector: '.failed, .unknown',
            style: {
                'border-color': '#BA434F'      # @red
                'background-color': '#fff0f0'  # Light red
            }
        },
        # Pending states (.pending mixin in LESS)
        {
            selector: '.scheduled, .waiting, .queued',
            style: {
                'border-color': '#999999'      # @medium-grey
                'background-color': '#f9f9f9'  # Light grey
            }
        }
    ]
}

module.tooltips = {
    # Shared tooltip template for graph nodes
    nodeTooltipTemplate: _.template """
        <div class="tooltip-header">
            <h4><%= name || id %></h4>
            <% if (typeof state !== 'undefined') { %>
                <span class="state-badge"><%= formatState(state) %></span>
            <% } %>
        </div>
        <div class="tooltip-content">
            <code class="command tooltip-command"><%= command || raw_command || "No command available" %></code>
        </div>
    """

    buildTooltipContent: (data, options = {}) ->
        resultData = { formatState: window.formatState }

        # If we have lookup data, use that as the primary data source
        if options.actionLookup && data.name
            fullData = options.actionLookup[data.name] || options.actionLookup[data.id]
            if fullData
                resultData = _.extend(resultData, fullData, {
                    name: data.name || fullData.name || data.id,
                    id: data.id || fullData.id
                })
                return @nodeTooltipTemplate(resultData)

        # If no lookup data or match is found we should fall back to the provided data
        return @nodeTooltipTemplate(_.extend(resultData, data))
}

class window.GraphView extends Backbone.View
    el: "#action-graph"
    initialize: (options) =>
        options = options || {}
        @height = options.height || 250
        @width = options.width || @$el.width()
        @showZoom = if options.showZoom? then options.showZoom else true
        @buildContent = options.buildContent
        @nodeClass = options.nodeClass || "node"

    # Create Cytoscape elements from graph data
    formatGraphData: (data) =>
        nodes = data.map (node) =>
            {
                data: {
                    id: node.name
                    name: node.name
                    command: node.command
                    nodeClass: if typeof @nodeClass is 'function' then @nodeClass(node) else @nodeClass
                }
            }

        edges = []
        for node in data
            for dep in node.dependencies
                edges.push {
                    data: {
                        id: "#{dep}-#{node.name}"
                        source: dep
                        target: node.name
                    }
                }

        { nodes, edges }

    buildCytoscape: (elements) =>
        # We set an explicit container height so that the graph actually shows up
        @$el.css('height', @height)

        @cy = cytoscape({
            container: @el
            elements: {
                nodes: elements.nodes,
                edges: elements.edges
            }
            style: module.GraphUtils.defaultStylesheet
            layout: module.GraphUtils.getDefaultLayout()
            # We cap the zoom in and out. Turns out if you don't do this it is extremely easy to lose the
            # graph. This + the reset button should make this a non-issue.
            minZoom: 0.2
            maxZoom: 3
        })

        @cy.ready =>
            @cy.nodes().forEach (node) =>
                nodeClass = node.data('nodeClass')
                if nodeClass
                    node.addClass(nodeClass)

            # 50 ms is enough time for the graph to render before we resize and fit it. Is there a better way? Yeah, probably.
            setTimeout(() =>
                @cy.resize()
                # Fit all elements (if no elements are specified it fits all, so we pass undefined) with 25px padding
                @cy.fit(undefined, 25)
            , 50)

    # Set up the popovers (tooltip that displays the action command) on nodes
    setupPopovers: =>
        @$el.append('<div class="cy-tooltip" style="display:none; position:absolute; z-index:999; background:white; padding:10px; border:1px solid #ccc; border-radius:4px; min-width:200px; max-width:400px; word-wrap:break-word; white-space:normal;"></div>')
        tooltip = @$('.cy-tooltip')

        @cy.on 'mouseover', 'node', (e) =>
            node = e.target
            content = @buildContent(node.data())
            tooltip.html(content)
            tooltip.show()

            @positionTooltip(e.originalEvent, tooltip)

        @cy.on 'mouseout', 'node', =>
            tooltip.hide()

        @cy.on 'mousemove', 'node', (e) =>
            @positionTooltip(e.originalEvent, tooltip)

    # Relative positioning of the tooltip and some shenanigans to ensure it's actually visible on screen
    positionTooltip: (event, tooltip) =>
        containerOffset = @$el.offset()
        containerWidth = @$el.width()
        containerHeight = @$el.height()

        # Relative position to the cursor
        left = event.pageX - containerOffset.left - (tooltip.outerWidth() / 2)
        top = event.pageY - containerOffset.top - tooltip.outerHeight() - 10

        # Ensure tooltip is fully visible
        if left < 0
            left = 5
        else if left + tooltip.outerWidth() > containerWidth
            left = containerWidth - tooltip.outerWidth() - 5

        # Show below cursor instead if it's too close to the top
        if top < 0
            top = event.pageY - containerOffset.top + 20

        tooltip.css({
            top: top + 'px'
            left: left + 'px'
        })

    render: =>
        @$el.html('')

        elements = @formatGraphData(@model)
        @buildCytoscape(elements)
        @setupPopovers()

        if @showZoom
            new GraphModalView(el: @el, model: @model, graphOptions: this).render()

        @

class GraphModalView extends Backbone.View
    initialize: (options) =>
        options = options || {}
        @graphOptions = options.graphOptions

    events:
        'click #view-full-screen': 'toggleModal'

    toggleModal: (event) ->
        $('.modal').modal('toggle')

    attachEvents: =>
        @$('.modal').on('show', @showModal)
        @$('.modal').on('hide', @removeGraph)

        # Add controls to parent title block
        title = @$el.closest('.outline-block').find('h2')
        if title.length
            # Only add if not already present
            if title.find('.graph-search').length == 0
                title.css({
                    'display': 'flex',
                    'justify-content': 'space-between',
                    'align-items': 'center'
                })

                # Create container for controls and add search and reset fields
                controlsHtml = '<div class="graph-controls" style="display: inline-flex; align-items: center; margin-right: 45px;">'
                controlsHtml += '<span class="graph-search" style="display: inline-flex; align-items: center;"><input type="text" placeholder="Search nodes..." style="width:150px; padding:3px; margin: 0; height: 24px; font-weight: normal; vertical-align: middle;"></span>'
                controlsHtml += '<button class="reset-graph-btn btn btn-clear tt-enable" title="Reset Graph" data-placement="top" style="margin-left: 10px;"><i class="icon-refresh icon-white"></i></button>'
                controlsHtml += '</div>'
                title.append(controlsHtml)

                # Prevent control clicks from triggering section collapse (why do we have collapsing sections anyway?)
                title.find('.graph-controls').on 'click', (e) ->
                    e.stopPropagation()

                # Set up reset button functionality
                title.find('.reset-graph-btn').on 'click', (e) =>
                    e.stopPropagation()
                    return unless @graphOptions.cy
                    title.find('.graph-search input').val('')
                    module.GraphUtils.resetGraph(@graphOptions.cy)

                # Set up search functionality
                title.find('.graph-search input').on 'input', (e) =>
                    e.stopPropagation() # Prevent event bubbling to title
                    return unless @graphOptions.cy

                    searchText = $(e.target).val().toLowerCase()
                    module.GraphUtils.applySearch(@graphOptions.cy, searchText)

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
            <div class="modal-header" style="display: flex; align-items: center;">
                <button class="btn btn-clear"
                    data-dismiss="modal"
                    style="margin-right: 10px;">
                    <i class="icon-circledown icon-white"></i>
                </button>
                <h3 style="flex: 1; display: flex; align-items: center; justify-content: space-between; margin: 0;">
                    <div style="display: flex; align-items: center;">
                        <i class="icon-barchart icon-white" style="margin-right: 5px;"></i>
                        <span>Action Graph</span>
                    </div>
                    <div class="graph-controls" style="display: inline-flex; align-items: center;">
                        <span class="graph-search" style="display: inline-flex; align-items: center;">
                            <input type="text" placeholder="Search nodes..." style="width:200px; padding:3px; margin: 0; height: 24px; font-weight: normal; vertical-align: middle;">
                        </span>
                        <button class="reset-graph-btn btn btn-clear tt-enable"
                                title="Reset Graph"
                                data-placement="top"
                                style="margin-left: 10px;">
                            <i class="icon-refresh icon-white"></i>
                        </button>
                    </div>
                </h3>
            </div>
            <div class="modal-body graph job-view">
            </div>
        </div>
        """

    showModal: (event) =>
        # If event.target isn't the modal, we want to return early without doing anything. This guards against
        # responding to events from children of the modal.
        return if event.target != $('.modal')[0]

        # Get window dimensions and explicitly set the width on the modal body.
        modalHeight = $(window).height() - 130
        modalWidth = $(window).width() - 150
        @$('.modal-body').css({
            width: modalWidth + 'px',
            height: modalHeight + 'px'
        })

        # Disable interactions on the main graph while the modal is open.
        #
        # This is necessary because Cytoscape graphs use canvas elements for rendering, and canvas
        # events don't respect modal z-index in the same way as normal DOM elements. Without this,
        # interactions in the full-screen modal would also affect the main graph when the mouse is
        # over an area where they overlap.
        if @graphOptions.cy
            # Save current state to restore from when the modal is closed
            @savedInteractionState = {
                userPanningEnabled: @graphOptions.cy.userPanningEnabled(),
                userZoomingEnabled: @graphOptions.cy.userZoomingEnabled(),
                boxSelectionEnabled: @graphOptions.cy.boxSelectionEnabled(),
                autoungrabify: @graphOptions.cy.autoungrabify(),
                autounselectify: @graphOptions.cy.autounselectify()
            }

            # Disable all interactions
            @graphOptions.cy.userPanningEnabled(false)
            @graphOptions.cy.userZoomingEnabled(false)
            @graphOptions.cy.boxSelectionEnabled(false)
            @graphOptions.cy.autoungrabify(true)
            @graphOptions.cy.autounselectify(true)

        options = _.extend {},
            @graphOptions,
            model: @model
            el: @$('.modal-body.graph').html('').get()
            height: modalHeight
            width: modalWidth
            showZoom: false

        modalGraph = new GraphView(options).render()

        # Set up reset
        @$('.modal-header .reset-graph-btn').off('click').on 'click', (e) =>
            return unless modalGraph.cy
            @$('.modal-header .graph-search input').val('')
            module.GraphUtils.resetGraph(modalGraph.cy)

        # Set up search
        @$('.modal-header .graph-search input').off('input').on 'input', (e) =>
            return unless modalGraph.cy
            searchText = $(e.target).val().toLowerCase()
            module.GraphUtils.applySearch(modalGraph.cy, searchText)

        # Resize the graph to fit the modal. We do a longer timeout here because the modal takes a bit longer to render.
        setTimeout(() =>
            if modalGraph.cy
                modalGraph.cy.resize()
                # Fit all elements (if no elements are specified it fits all, so we pass undefined) with 25px padding
                modalGraph.cy.fit(undefined, 25)
        , 200)

    removeGraph: (event) =>
        return if event.target != $('.modal')[0]
        @$('.modal-body.graph').empty()

        # Restore all interaction capabilities on the main graph
        if @graphOptions.cy && @savedInteractionState
            @graphOptions.cy.userPanningEnabled(@savedInteractionState.userPanningEnabled)
            @graphOptions.cy.userZoomingEnabled(@savedInteractionState.userZoomingEnabled)
            @graphOptions.cy.boxSelectionEnabled(@savedInteractionState.boxSelectionEnabled)
            @graphOptions.cy.autoungrabify(@savedInteractionState.autoungrabify)
            @graphOptions.cy.autounselectify(@savedInteractionState.autounselectify)

    render: =>
        @$el.append(@template)
        @attachEvents()
        @delegateEvents()
        @
