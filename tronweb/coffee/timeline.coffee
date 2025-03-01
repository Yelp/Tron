# Timeline
# This file creates the D3-based timeline visualization for JobRuns.
window.modules = window.modules || {}
window.modules.timeline = module = {}


# We pad the right bound of the timeline by {padding}% to ensure the latest bar is within the bounds of the chart.
# E.g. If we have a 4 hour timeline => 14,400,000 ms * 1.02 = 14,688,000 ms => ~4 hours and 5 minutes are shown.
module.padMaxDate = (dateRange, padding) ->
    [minDate, maxDate] = (moment.tz(date, 'America/Los_Angeles') for date in dateRange)
    delta = maxDate.diff(minDate)
    maxDate.add('ms', delta * padding)
    [minDate.toDate(), maxDate.toDate()]

# Higher-order function that creates accessors for object methods.
# We use this throughout the timeline to extract data from timeline entries (e.g. start, end, etc.).
call = (field) -> (item) -> item[field]()


# The actual rendering of the timeline visualization. There is a decent amount of styling happening here, but I
# think that's okay because it's all quite specific to the timeline and we don't really reuse this code elsewhere.
class module.TimelineView extends Backbone.View

    el: "#timeline-graph"

    initialize: (options) =>
        @margins = _.extend({ top: 30, right: 40, bottom: 20, left: 60 }, options.margins)
        verticalMargins = @margins.top + @margins.bottom
        dataHeight = @model?.length * 30 || 0
        @height = options.height || Math.max(dataHeight + verticalMargins, 100)
        @width = options.width || @$el.innerWidth()
        @minBarWidth = options.minBarWidth || 5

    innerHeight: =>
        @height - @margins.bottom - @margins.top

    innerWidth: =>
        @width - @margins.left - @margins.right

    # Build the x-axis scale based on the start and end times of the data.
    buildX: (data) =>
        minDate = d3.min(data, call('getStart'))
        maxDate = d3.max(data, call('getEnd'))

        if minDate and maxDate
            if maxDate - minDate < 300000  # When the time range is less than 5 minutes (300000 ms) we expand it. This just looks better.
                maxDate = new Date(minDate.getTime() + 300000)
        else
            # This is a fallback in case we don't have any data. We show a 5 minute window regardless.
            now = new Date()
            minDate = new Date(now.getTime() - 300000)
            maxDate = now

        domain = [minDate, maxDate]
        domain = module.padMaxDate(domain, 0.02)  # Hardcoded padding of 2%

        d3.scaleTime()
            .domain(domain)
            .range([0, @innerWidth()])

    # Build the y-axis scale based on the number of runs.
    buildY: (data) =>
        d3.scaleBand()
            .domain(data)
            .range([0, @innerHeight()])
            .padding(0.1)

    # Building the horizontal grid lines so that we can actually know which bar belongs to which run when looking at more than a few runs.
    buildGrid: (svg, x, y) =>
        svg.append("g")
            .attr("class", "grid-lines")
            .selectAll('.horizontal-grid')
            .data(y.domain())
            .join('line')
            .filter((d, i) -> i % 2 is 0)  # Only show every other line to avoid visual clutter.
            .attr('class', 'horizontal-grid')
            .attr('x1', 0)
            .attr('x2', @innerWidth())
            .attr('y1', (d) -> y(d) + y.bandwidth() / 2)
            .attr('y2', (d) -> y(d) + y.bandwidth() / 2)
            .style('stroke', '#e0e0e0')
            .style('stroke-dasharray', '3,3')


    buildAxis: (x, y) =>
        # TODO: Test this with a larger run history window. It looks better with fewer ticks, but I've been testing with frequent runs.
        # In the original version of this function we could get into a situation where the tick labels would overlap and become illegible.
        # One solution could be to set custom intervals based on the span of the data:
        #   d3.timeFormat("%H:%M") Hours + Minutes for short spans
        #   d3.timeFormat("%a %d") Day + Date for short-medium spans
        #   d3.timeFormat("%b %d") Month + Date for medium-long spans
        #   d3.timeFormat("%b %Y") Month + Year for looong spans
        xAxis = d3.axisTop(x)
            .ticks(5)
            .tickSize(-@innerHeight())
            .tickPadding(5)

        yAxis = d3.axisLeft(y)
            .tickSize(0)
            .tickPadding(5)

        { xAxis, yAxis }


    buildSvg: =>
        d3.select(@el).append("svg")
            .attr("height", @height)
            .attr("width", @width)
            .attr("class", "timeline-chart")
            .append("g")
            .attr("transform", "translate(#{@margins.left}, #{@margins.top})")


    buildSvgAxis: (svg, xAxis, yAxis) =>
        svg.append("g")
            .attr("class", "x axis")
            .call(xAxis)
            .selectAll('line')
            .style('stroke', '#e0e0e0')
            .style('stroke-opacity', 0.7)

        yAxisGroup = svg.append("g")
            .attr("class", "y axis")
            .call(yAxis)

        yAxisGroup.selectAll('.tick')
            .each (d) ->
                tick = d3.select(this)
                tick.select('text').remove()
                tick.append('a')
                    .attr('href', d.getYAxisLink())
                    .append('text')
                    .attr("x", -5)
                    .attr("y", 0)
                    .attr("dy", ".32em")
                    .text(d.getYAxisText())
                    .style("fill", "#333")
                    .style("font-size", "12px")
                    .style("text-anchor", "end")


    buildSvgBars: (svg, data, x, y) =>
        # Calculate the width of the bar, ensuring that even short runs have a visible bar.
        getWidth = (d) =>
            _.max([@minBarWidth, x(d.getEnd()) - x(d.getStart())])

        svg.selectAll('.bar')
            .data(data)
            .join('rect')
            .attr("class", (d) -> "bar #{d.getBarClass()}")
            .attr("x", (d) -> x(d.getStart()))
            .attr("width", getWidth)
            .attr("y", (d) -> y(d))
            .attr("height", (d) -> y.bandwidth())


    render: =>
        @$el.html('')
        data = @model

        x = @buildX(data)
        y = @buildY(data)

        {xAxis, yAxis} = @buildAxis(x, y)

        svg = @buildSvg()

        @buildGrid(svg, x, y)

        @buildSvgAxis(svg, xAxis, yAxis)
        @buildSvgBars(svg, data, x, y)
        @
