
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
    if string
        formatted = moment(string).format('MMM, Do YYYY, h:mm:ss a')
        delta = moment(string).fromNow()
    else
        formatted = delta = defaultString
    template(formatted: formatted, delta: delta)


window.makeTooltips = (root) ->
    root.find('.tt-enable').tooltip()


class window.FilterView extends Backbone.View

    tagName: "div"

    className: "outline"

    filterTemplate: _.template """
        <div class="input-prepend">
          <span class="add-on">
            <i class="icon-filter"></i>
            <% print(_.str.humanize(filterName)) %>
          </span>
          <input type="text" id="filter-<%= filterName %>"
                 placeholder="filter"
                 value="<%= defaultValue %>"
                 class="span2"
                 data-filter-name="<%= filterName %>Filter">
        </div>
    """

    template: _.template """
        <form class="filter-form">
        <div class="control-group">
          <div class="controls">
            <% print(filters.join('')) %>
          </div>
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
        @

    events:
        "keyup input":  "filterChange"
        "submit":       "submit"
        "change input": "filterDone"

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
        <button class="btn btn-default <%= active %>">
            <i class="icon-refresh"></i>
        </button>
        """

    render: =>
        if @model.enabled
            text = "Auto-refresh #{ @model.interval / 1000 }s"
            active = "active"
        else
            text = active = ""
        @$el.html @template(text: text, active: active)
        # See note about subview
        @delegateEvents()
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
