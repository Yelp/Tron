
# Common view elements

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
            type: "inverse"
    else
        formatted = defaultString
        delta = label_template
            delta: defaultString
            type: "important"
    template(formatted: formatted, delta: delta)


window.makeTooltips = (root) ->
    root.find('.tt-enable').tooltip()


window.formatName = (name) =>
       name.replace(/\./g, '.<wbr/>').replace(/_/g, '_<wbr/>')


class window.FilterView extends Backbone.View

    tagName: "div"

    className: ""

    filterTemplate: _.template """
        <div class="input-prepend">
          <span class="add-on">
            <i class="icon-filter icon-white"></i>
            <% print(_.str.humanize(filterName)) %>
          </span>
          <input type="text" id="filter-<%= filterName %>"
                 value="<%= defaultValue %>"
                 class="span2"
                 data-filter-name="<%= filterName %>Filter">
        </div>
    """

    template: _.template """
        <form class="filter-form">
        <div class="control-group">
          <fieldset>
            <legend class="tt-enable clickable"
                title="Toggle Filters">Filters</legend>
            <div class="controls">
                <% print(filters.join('')) %>
            </div>
          </fieldset>
        </div>
        </form>
        """

    render: =>
        createFilter = (typeName) =>
            @filterTemplate(
                defaultValue: @model.get("#{typeName}Filter")
                filterName: typeName
            )

        filters = _.map(@model.filterTypes, createFilter)
        @$el.html @template(filters: filters)
        @delegateEvents()
        makeTooltips(@$el)
        @

    events:
        "keyup input":  "filterChange"
        "submit":       "submit"
        "change input": "filterDone"
        "click legend": "toggleVisible"

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
        updateLocationParam(filterName, filterValue)

    submit: (event) ->
        event.preventDefault()

    # TODO: update model to store state
    toggleVisible: (event) =>
        @$('.filter-form .controls').toggle()
        @$('.filter-form fieldset').toggleClass('plain')


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
        <button class="btn btn-inverse tt-enable <%= active %>"
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
