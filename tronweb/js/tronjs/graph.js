/*
 * decaffeinate suggestions:
 * DS101: Remove unnecessary use of Array.from
 * DS102: Remove unnecessary code created because of implicit returns
 * DS206: Consider reworking classes to avoid initClass
 * DS207: Consider shorter variations of null checks
 * Full docs: https://github.com/decaffeinate/decaffeinate/blob/master/docs/suggestions.md
 */

// Action graph using d3

Cls = (window.GraphView = class GraphView extends Backbone.View {
    constructor(...args) {
        this.initialize = this.initialize.bind(this);
        this.buildNodeMap = this.buildNodeMap.bind(this);
        this.getLinks = this.getLinks.bind(this);
        this.buildSvgLinks = this.buildSvgLinks.bind(this);
        this.buildSvgNodes = this.buildSvgNodes.bind(this);
        this.attachEvents = this.attachEvents.bind(this);
        this.addLinks = this.addLinks.bind(this);
        this.render = this.render.bind(this);
        super(...args);
    }

    static initClass() {

        this.prototype.el = "#action-graph";
    }

    initialize(options) {
        options = options || {};
        this.height = options.height || 250;
        this.width = options.width || this.$el.width();
        this.linkDistance = options.linkDistance || 80;
        this.showZoom = (options.showZoom != null) ? options.showZoom : true;
        this.buildContent = options.buildContent;
        return this.nodeClass = options.nodeClass || "node";
    }

    buildNodeMap(data) {
        const nodes = {};
        for (let node of Array.from(data)) {
            nodes[node.name] = node;
        }
        return nodes;
    }

    getLinks(data) {
        const nodes = this.buildNodeMap(data);
        const nested = Array.from(data).map((node) =>
            (Array.from(node.dependent).map((target) => ({source: node, target: nodes[target]}))));
        return _.flatten(nested);
    }

    buildSvgLinks(links) {
        this.svg.append("svg:defs")
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
            .attr("d", "M 0 2 L 10 5 L 0 8 z");

        return this.link = this.svg.selectAll(".link")
            .data(links)
            .enter().append("line")
            .attr("class", "link")
            .attr("marker-end", "url(#arrow)");
    }

    buildSvgNodes(data){
        this.node = this.svg.selectAll(".node")
            .data(data)
            .enter().append("svg:g")
            .call(this.force.drag)
            .attr({
                class: this.nodeClass,
                'data-title'(d) { return d.name; },
                'data-html': true,
                'data-content': this.buildContent
        });

        this.node.append("svg:circle")
            .attr("r", 6);

        return this.node.append("svg:text")
            .attr({dx: 12, dy: "0.25em"})
            .text(d => d.name);
    }

    attachEvents() {
        $('.node').popover({
            container: this.$el,
            placement: 'top',
            trigger: 'hover'
        });

        return this.force.on("tick", () => {
            this.link.attr("x1", d => d.source.x)
                .attr("y1", d => d.source.y)
                .attr("x2", d => d.target.x)
                .attr("y2", d => d.target.y);

            return this.node.attr("transform", d => `translate(${d.x}, ${d.y})`);
        });
    }

    addNodes(data) {
        return this.force.nodes(data);
    }

    addLinks(links) {
        return this.force.links(links);
    }

    buildForce(height, width) {
        // TODO: randomly move nodes when links cross
        return this.force = d3.layout.force()
            .charge(-400)
            .theta(1)
            .linkDistance(this.linkDistance)
            .size([width, height]);
    }

    buildSvg(height, width) {
        return this.svg = d3.select(this.el)
            .append("svg")
            .attr({
                height,
                width
        });
    }

    render() {
        this.buildForce(this.height, this.width);
        this.buildSvg(this.height, this.width);
        this.addNodes(this.model);
        const links = this.getLinks(this.model);
        this.addLinks(links);
        this.force.start();
        this.buildSvgLinks(links);
        this.buildSvgNodes(this.model);
        if (this.showZoom) {
            new GraphModalView({el: this.el, model: this.model, graphOptions: this}).render();
        }
        this.attachEvents();
        return this;
    }
});
Cls.initClass();


class GraphModalView extends Backbone.View {
    constructor(...args) {
        this.initialize = this.initialize.bind(this);
        this.attachEvents = this.attachEvents.bind(this);
        this.showModal = this.showModal.bind(this);
        this.removeGraph = this.removeGraph.bind(this);
        this.render = this.render.bind(this);
        super(...args);
    }

    static initClass() {

        this.prototype.events =
            {'click #view-full-screen':   'toggleModal'};

        this.prototype.template = `\
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
</div>\
`;
    }

    initialize(options) {
        options = options || {};
        return this.graphOptions = options.graphOptions;
    }

    toggleModal(event) {
        return $('.modal').modal('toggle');
    }

    attachEvents() {
        this.$('.modal').on('show', this.showModal);
        return this.$('.modal').on('hide', this.removeGraph);
    }

    showModal(event) {
        // prevent firing on child events
        let graph;
        if (event.target !== $('.modal')[0]) { return; }
        const options = _.extend({},
            this.graphOptions, {
            model:          this.model,
            el:             this.$('.modal-body.graph').html('').get(),
            height:         $(window).height() - 130,
            width:          $(document).width() - 150,
            linkDistance:   250,
            showZoom:       false
        }
        );

        return graph = new GraphView(options).render();
    }

    removeGraph(event) {
        if (event.target !== $('.modal')[0]) { return; }
        return this.$('.modal-body.graph').empty();
    }

    render() {
        this.$el.append(this.template);
        this.attachEvents();
        this.delegateEvents();
        return this;
    }
}
GraphModalView.initClass();
