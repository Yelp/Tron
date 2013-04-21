
window.modules = window.modules || {}
module = window.modules.timeline = {}



start_time = (item) ->
    new Date(item.start_time || item.run_time)

end_time = (item) ->
    return new Date() if item.state == 'running'
    new Date(item.end_time || item.start_time || item.run_time)


class module.TimelineView extends Backbone.View

    el: "#timeline-graph"

    initialize: (options) =>
        @margins = _.extend(
            {top: 40, right: 20, bottom: 20, left: 80},
            options.margins)
        @height = options.height || 500
        @width = options.width || 1000
        @nameField = options.nameField
        @minBarWidth = 5

    innerHeight: =>
        @height - @margins.bottom - @margins.top

    innerWidth: =>
        @width - @margins.left - @margins.right

    # TODO: parameterise these lookups
    buildX: (data) =>
        domain = [ d3.min(data, start_time),
                   d3.max(data, end_time)]
        d3.time.scale().domain(domain)
            .rangeRound([0, @innerWidth()])

    buildY: (data) =>
        d3.scale.ordinal()
            .domain(_.map(data, (item) => item[@nameField]))
            .rangeBands([0, @innerHeight()], 0.1)

    buildAxis: (x, y) =>
        xAxis = d3.svg.axis().scale(x).orient("top")
        yAxis = d3.svg.axis().scale(y).orient("left")
        [xAxis, yAxis]

    # TODO: add some guidelines (x, or y?)
    # TODO: add mouseover for bars
    buildSvg: =>
        d3.select(@el).append("svg").attr
                height: @height
                width: @width
                class: "timeline-chart"
            .append("g").attr
                transform: "translate(#{@margins.left}, #{@margins.top})"

    buildSvgAxis: (svg, xAxis, yAxis) =>
        svg.append("g").attr(class: "x axis").call(xAxis)
        svg.append("g").attr(class: "y axis").call(yAxis)

    buildSvgBars: (svg, data, x, y) =>
        width = (d) =>
            _.max([@minBarWidth, x(end_time(d)) - x(start_time(d))])

        svg.selectAll('.timeline-chart').data(data).enter()
            .append('rect')
            .attr
                class:  (d) -> "bar #{d.state}"
                x:      (d) -> x(start_time(d))
                width:  width
                y:      (d) => y(d[@nameField])
                height: (d) -> y.rangeBand()

    render: =>
        data = @model
        [x, y] = [@buildX(data), @buildY(data)]
        [xAxis, yAxis] = @buildAxis(x, y)
        svg = @buildSvg()
        @buildSvgAxis(svg, xAxis, yAxis)
        @buildSvgBars(svg, data, x, y)
        @
