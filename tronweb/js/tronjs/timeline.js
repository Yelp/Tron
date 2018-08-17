/*
 * decaffeinate suggestions:
 * DS101: Remove unnecessary use of Array.from
 * DS102: Remove unnecessary code created because of implicit returns
 * DS206: Consider reworking classes to avoid initClass
 * Full docs: https://github.com/decaffeinate/decaffeinate/blob/master/docs/suggestions.md
 */

//window.modules = window.modules || {}
//module = window.modules.timeline = {}


module.padMaxDate = function(dateRange, padding) {
    const [minDate, maxDate] = (Array.from(dateRange).map((date) => moment(date)));
    const delta = maxDate.diff(minDate);
    maxDate.add('ms', delta * padding);
    return [minDate.toDate(), maxDate.toDate()];
};


const call = field => item => item[field]();


Cls = (module.TimelineView = class TimelineView extends Backbone.View {
    constructor(...args) {
        this.initialize = this.initialize.bind(this);
        this.innerHeight = this.innerHeight.bind(this);
        this.innerWidth = this.innerWidth.bind(this);
        this.buildX = this.buildX.bind(this);
        this.buildY = this.buildY.bind(this);
        this.buildAxis = this.buildAxis.bind(this);
        this.buildSvg = this.buildSvg.bind(this);
        this.buildSvgAxis = this.buildSvgAxis.bind(this);
        this.buildSvgBars = this.buildSvgBars.bind(this);
        this.render = this.render.bind(this);
        super(...args);
    }

    static initClass() {

        this.prototype.el = "#timeline-graph";
    }

    initialize(options) {
        this.margins = _.extend(
            {top: 30, right: 40, bottom: 20, left: 60},
            options.margins);
        const verticalMargins = this.margins.top + this.margins.bottom;
        this.height = options.height || ((this.model.length * 30) + verticalMargins);
        this.width = options.width || this.$el.innerWidth();
        return this.minBarWidth = options.minBarWidth || 5;
    }

    innerHeight() {
        return this.height - this.margins.bottom - this.margins.top;
    }

    innerWidth() {
        return this.width - this.margins.left - this.margins.right;
    }

    buildX(data) {
        let domain = [ d3.min(data, call('getStart')), d3.max(data, call('getEnd'))];
        domain = module.padMaxDate(domain, 0.02);

        return d3.time.scale().domain(domain)
            .rangeRound([0, this.innerWidth()]);
    }

    buildY(data) {
        return d3.scale.ordinal()
            .domain(data)
            .rangeBands([0, this.innerHeight()], 0.1);
    }

    buildAxis(x, y) {
        const xAxis = d3.svg.axis().scale(x).orient("top")
            .ticks([10])
            .tickSize(-this.innerHeight(), 0, 0)
            .tickPadding(5);
        const yAxis = d3.svg.axis().scale(y).orient("left")
            .tickSize(0)
            .tickPadding(5);
        return [xAxis, yAxis];
    }

    buildSvg() {
        return d3.select(this.el).append("svg").attr({
                height: this.height,
                width: this.width,
                class: "timeline-chart"}).append("g").attr({
                transform: `translate(${this.margins.left}, ${this.margins.top})`});
    }

    buildSvgAxis(svg, xAxis, yAxis) {
        const self = this;
        svg.append("g").attr({class: "x axis"}).call(xAxis);
        return svg.append("g").attr({class: "y axis"}).call(yAxis)
            .selectAll('g').each(function(d) {
                const ele = d3.select(this);
                ele.selectAll('text').remove();
                ele.selectAll('line').remove();
                return ele.append('a')
                    .attr({'xlink:href': d.getYAxisLink()})
                    .append('text').attr({ x: -5, y: 0, dy: ".32em"})
                    .text(d.getYAxisText());
        });
    }


    buildSvgBars(svg, data, x, y) {
        const getWidth = d => {
            return _.max([this.minBarWidth, x(d.getEnd()) - x(d.getStart())]);
        };

        return svg.selectAll('.timeline-chart').data(data).enter()
            .append('rect')
            .attr({
                class:  d => `bar ${d.getBarClass()}`,
                x:      d => x(d.getStart()),
                width:  getWidth,
                y:      d => y(d),
                height: d => y.rangeBand()
        });
    }

    render() {
        this.$el.html('');
        const data = this.model;
        const [x, y] = [this.buildX(data), this.buildY(data)];
        const [xAxis, yAxis] = this.buildAxis(x, y);
        const svg = this.buildSvg();
        this.buildSvgAxis(svg, xAxis, yAxis);
        this.buildSvgBars(svg, data, x, y);
        return this;
    }
});
Cls.initClass();
