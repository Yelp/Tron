
# Common view elements
window.modules = window.modules || {}
window.modules.views = module = {}


# Note about subview
# Subviews need to re-delegate events, because they are lost
# when superviews re-render

# Print the date as a string describing the elapsed time
window.dateFromNow = (string, defaultString='never') ->
    template = _.template """
        <span title="<%= formatted %>" class="tt-enable" data-placement="top">
            <%= delta %>
        </span>
        """

    label_template = _.template """
        <span class="label label-<%= type %>"><%= delta %></span>
        """

    if string
        formatted = moment(string).format('MMM, Do YYYY, h:mm:ss a')
        delta = label_template
            delta: moment(string).fromNow()
            type: "clear"
    else
        formatted = defaultString
        delta = label_template
            delta: defaultString
            type: "important"
    template(formatted: formatted, delta: delta)


window.getDuration = (time) ->
    days = '0'
    if time.indexOf("day") != -1 or time.indexOf("days") != -1
        [dayStr, time] = time.split(',')
        [days, day]= dayStr.split(' ')
    [time, ms] = time.split('.')
    [hours, minutes, seconds] = time.split(':')
    moment.duration
        days: parseInt(days)
        hours: parseInt(hours)
        minutes: parseInt(minutes)
        seconds: parseInt(seconds)


window.formatDuration = (duration) ->
    template = _.template """
        <span class="label label-clear tt-enable" title="<%= duration %>">
          <%= humanized %>
        </span>
    """
    humanize = getDuration(duration).humanize()
    template(duration: duration, humanized: humanize)


# If params match, return "selected". Used for select boxes
window.isSelected = (current, value) ->
    if current == value then "selected" else ""

window.makeTooltips = (root) ->
    root.find('.tt-enable').tooltip()


window.formatName = (name) =>
    name.replace(/\./g, '.<wbr/>').replace(/_/g, '_<wbr/>')


window.formatState = (state) =>
    """<span class="label #{state}">#{state}</span>"""


window.formatDelay = (delay) ->
    if delay
        """<small> (retry delayed for #{Math.round(delay)}s)</small>"""
    else
        ""

module.makeHeaderToggle = (root) ->
    headers = root.find('.outline-block h2')
    headers.click (event) -> $(event.target).nextAll().slideToggle()
    headers.addClass('clickable')


class window.FilterView extends Backbone.View

    tagName: "div"

    className: ""

    defaultIcon: "icon-filter"

    filterIcons:
        name:       "icon-filter"
        node_pool:  "icon-connected"
        state:      "icon-switchon"
        status:     "icon-switchon"

    filterTemplate: _.template """
        <div class="input-prepend">
          <input type="text" id="filter-<%= filterName %>"
                 value="<%= defaultValue %>"
                 class="input-medium"
                 autocomplete="off"
                 placeholder="<%= _.str.humanize(filterName) %>"
                 data-filter-name="<%= filterName %>Filter">
          <i class="<%= icon %> icon-grey"></i>
        </div>
    """

    template: _.template """
        <form class="filter-form">
          <div class="control-group outline-block">
            <div class="controls">
            <div class="span1 toggle-header"
                title="Toggle Filters">Filters</div>
                <%= filters.join('') %>
            </div>
          </div>
        </form>
        """

    getFilterTemplate: (filterName) =>
        createName = "create#{filterName}"
        if @[createName] then @[createName] else @filterTemplate

    renderFilters: =>
        createFilter = (filterName) =>
            template = @getFilterTemplate(filterName)
            template
                defaultValue: @model.get("#{filterName}Filter")
                filterName: filterName
                icon: @filterIcons[filterName] || @defaultIcon

        filters = _.map((k for k of @model.filterTypes), createFilter)
        @$el.html @template(filters: filters)

    render: =>
        @renderFilters()
        @delegateEvents()
        makeTooltips(@$el)
        @

    events:
        "keyup input":   "filterChange"
        "submit":        "submit"
        "change input":  "filterDone"
        "change select": "selectFilterChange"

    getFilterFromEvent: (event) =>
        filterEle = $(event.target)
        [filterEle.data('filterName'), filterEle.val()]

    filterChange: (event) =>
        [filterName, filterValue] = @getFilterFromEvent(event)
        @model.set(filterName, filterValue)
        @trigger('filter:change', filterName, filterValue)

    filterDone: (event) ->
        [filterName, filterValue] = @getFilterFromEvent(event)
        @trigger('filter:done', filterName, filterValue)
        window.modules.routes.updateLocationParam(filterName, filterValue)

    selectFilterChange: (event) =>
        @filterChange(event)
        @filterDone(event)

    submit: (event) ->
        event.preventDefault()


class window.RefreshToggleView extends Backbone.View

    initialize: ->
        @listenTo(mainView, 'closeView', @model.disableRefresh)
        @listenTo(@model, 'refresh', @triggerRefresh)

    tagName: "div"

    className: "refresh-view pull-right"

    attributes:
        "type":             "button"
        "data-toggle":      "button"

    template: _.template """
        <span class="muted"><%= text %></span>
        <button class="btn btn-clear tt-enable <%= active %>"
            title="Toggle Refresh"
            data-placement="top">
            <i class="icon-refresh icon-white"></i>
        </button>
        """

    render: =>
        if @model.enabled
            text = "Refresh #{ @model.interval / 1000 }s"
            active = "active"
        else
            text = active = ""
        @$el.html @template(text: text, active: active)
        # See note about subview
        @delegateEvents()
        makeTooltips(@$el)
        @

    events:
        "click button":        "toggle"

    toggle: (event) =>
        @model.toggle(event)
        @render()

    triggerRefresh: =>
        @trigger('refreshView')


class window.ClickableListEntry extends Backbone.View
    # A ciew for an element in a list that is clickable

    className: ->
        "clickable"

    events:
        "click":    "propogateClick"

    propogateClick: (event) =>
        if event.button == 0
            document.location = @$('a').first().attr('href')


module.makeSlider = (root, options) ->
    root.find('.slider-bar').slider(options)


class module.SliderView extends Backbone.View

    initialize: (options) ->
        options = options || {}
        @displayCount = options.displayCount || 10

    tagName: "div"

    className: "list-controls controls-row"

    template: """
            <div class="span1">
              <span id="display-count" class="label label-inverse"></span>
            </div>
            <div class="slider-bar span10"></div>
        """

    handleSliderMove: (event, ui) =>
        @updateDisplayCount(ui.value)
        @trigger('slider:change', ui.value)

    updateDisplayCount: (count) =>
        @displayCount = count
        content = """#{count} / #{@model.length()}"""
        @$('#display-count').html(content)

    render: ->
        @$el.html @template
        @updateDisplayCount(_.min([@model.length(), @displayCount]))
        module.makeSlider @$el,
            max: @model.length()
            min: 0
            range: 'min'
            value: @displayCount
            slide: @handleSliderMove
        @
