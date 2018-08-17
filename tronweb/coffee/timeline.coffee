
#window.modules = window.modules || {}
#module = window.modules.timeline = {}


module.padMaxDate = (dateRange, padding) ->
    [minDate, maxDate] = (moment(date) for date in dateRange)
    delta = maxDate.diff(minDate)
    maxDate.add('ms', delta * padding)
    [minDate.toDate(), maxDate.toDate()]


call = (field) -> (item) -> item[field]()


class module.TimelineView extends Backbone.View

    el: "#timeline-graph"

    initialize: (options) =>
        @margins = _.extend(
            {top: 30, right: 40, bottom: 20, left: 60},
            options.margins)
        verticalMargins = @margins.top + @margins.bottom
        @height = options.height || @model.length * 30 + verticalMargins
        @width = options.width || @$el.innerWidth()
        @minBarWidth = options.minBarWidth || 5

    innerHeight: =>
        @height - @margins.bottom - @margins.top

    innerWidth: =>
        @width - @margins.left - @margins.right

    buildX: (data) =>
        domain = [ d3.min(data, call('getStart')), d3.max(data, call('getEnd'))]
        domain = module.padMaxDate(domain, 0.02)

        d3.time.scale().domain(domain)
            .rangeRound([0, @innerWidth()])

    buildY: (data) =>
        d3.scale.ordinal()
            .domain(data)
            .rangeBands([0, @innerHeight()], 0.1)

    buildAxis: (x, y) =>
        xAxis = d3.svg.axis().scale(x).orient("top")
            .ticks([10])
            .tickSize(-@innerHeight(), 0, 0)
            .tickPadding(5)
        yAxis = d3.svg.axis().scale(y).orient("left")
            .tickSize(0)
            .tickPadding(5)
        [xAxis, yAxis]

    buildSvg: =>
        d3.select(@el).append("svg").attr
                height: @height
                width: @width
                class: "timeline-chart"
            .append("g").attr
                transform: "translate(#{@margins.left}, #{@margins.top})"

    buildSvgAxis: (svg, xAxis, yAxis) =>
        self = @
        svg.append("g").attr(class: "x axis").call(xAxis)
        svg.append("g").attr(class: "y axis").call(yAxis)
            .selectAll('g').each (d) ->
                ele = d3.select(this)
                ele.selectAll('text').remove()
                ele.selectAll('line').remove()
                ele.append('a')
                    .attr('xlink:href': d.getYAxisLink())
                    .append('text').attr( x: -5, y: 0, dy: ".32em")
                    .text(d.getYAxisText())


    buildSvgBars: (svg, data, x, y) =>
        getWidth = (d) =>
            _.max([@minBarWidth, x(d.getEnd()) - x(d.getStart())])

        svg.selectAll('.timeline-chart').data(data).enter()
            .append('rect')
            .attr
                class:  (d) => "bar #{d.getBarClass()}"
                x:      (d) => x(d.getStart())
                width:  getWidth
                y:      (d) => y(d)
                height: (d) => y.rangeBand()

    render: =>
        @$el.html('')
        data = @model
        [x, y] = [@buildX(data), @buildY(data)]
        [xAxis, yAxis] = @buildAxis(x, y)
        svg = @buildSvg()
        @buildSvgAxis(svg, xAxis, yAxis)
        @buildSvgBars(svg, data, x, y)
        @
