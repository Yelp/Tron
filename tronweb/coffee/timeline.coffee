
window.modules = window.modules || {}
module = window.modules.timeline = {}


buildStartTime = (maxDate) ->
    (item) -> new Date(item.start_time || item.run_time)


buildEndTime = (maxDate, useMaxDate) ->
    (item) ->
        return maxDate if useMaxDate(item)
        new Date(item.end_time || item.start_time || item.run_time)


isRunningState = (item) ->
    item.state == 'running'


getState = (item) ->
    return item.state


module.padMaxDate = (dateRange, padding) ->
    [minDate, maxDate] = (moment(date) for date in dateRange)
    delta = maxDate.diff(minDate)
    maxDate.add('ms', delta * padding)
    [minDate.toDate(), maxDate.toDate()]


class module.TimelineView extends Backbone.View

    el: "#timeline-graph"

    initialize: (options) =>
        @margins = _.extend(
            {top: 30, right: 40, bottom: 20, left: 60},
            options.margins)
        @height = options.height || 500
        @width = options.width || 1000
        @nameField = options.nameField
        @maxDate = options.maxDate || new Date()
        @startTime = options.startTime || buildStartTime(@maxDate)
        @endTime = options.endTime || buildEndTime(@maxDate, isRunningState)
        @getClass = options.getClass || getState
        @minBarWidth = options.minBarWidth || 5

    innerHeight: =>
        @height - @margins.bottom - @margins.top

    innerWidth: =>
        @width - @margins.left - @margins.right

    buildX: (data) =>
        domain = [d3.min(data, @startTime), d3.max(data, @endTime)]
        domain = module.padMaxDate(domain, 0.02)

        d3.time.scale().domain(domain)
            .rangeRound([0, @innerWidth()])

    buildY: (data) =>
        d3.scale.ordinal()
            .domain(_.map(data, (item) => item[@nameField]))
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

    # TODO: add mouseover for bars
    buildSvg: =>
        d3.select(@el).append("svg").attr
                height: @height
                width: @width
                class: "timeline-chart"
            .append("g").attr
                transform: "translate(#{@margins.left}, #{@margins.top})"

    # TODO: make links
    buildSvgAxis: (svg, xAxis, yAxis) =>
        svg.append("g").attr(class: "x axis").call(xAxis)
        svg.append("g").attr(class: "y axis").call(yAxis)

    buildSvgBars: (svg, data, x, y) =>
        getWidth = (d) =>
            _.max([@minBarWidth, x(@endTime(d)) - x(@startTime(d))])

        svg.selectAll('.timeline-chart').data(data).enter()
            .append('rect')
            .attr
                class:  (d) => "bar #{@getClass(d)}"
                x:      (d) => x(@startTime(d))
                width:  getWidth
                y:      (d) => y(d[@nameField])
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
