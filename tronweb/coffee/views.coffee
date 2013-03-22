
# Common view elements

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


class window.BreadcrumbView extends Backbone.View

    el: $('#breadcrum-nav').get(0)

    template: _.template """<ul class="breadcrumb"><%= content %></ul>"""

    template_item: _.template """ <li> <a href="<%= url %>"><%= name %></a> </li> """

    template_active: _.template """ <li class="active"> <%= name %> </li> """

    render: (model) ->
        divider = """ <span class="divider">&gt;</span> """
        items = ( @template_item(item) for item in _.initial(model) ).join(divider)
        @$el.html @template(content: items + divider + @template_active( _.last(model) ))
        @

    clear: ->
        @$el.html('')


class window.FilterView extends Backbone.View

    # TODO: replace default with a model
    initialize: (options) ->
        options = options || {}
        @default = options.default

    tagName: "form"

    className: "pull-right"

    template: _.template """
            <div class="control-group">
                <div class="controls">
                    <div class="input-prepend">
                        <span class="add-on"><i class="icon-filter"></i></span>
                        <input type="text" placeholder="Filter by name" value="<%= defaultValue %>">
                    </div>
                </div>
            </div>
        """

    render: =>
        @$el.html @template(defaultValue: @default)
        @

    events:
        "keyup input":  "filterChange"
        "submit":       "submit"
        "change input": "filterDone"

    # TODO: fix event name
    filterChange: ->
        @trigger('filter_change', @$('input').val())

    filterDone: ->
        value = @$('input').val()
        @trigger('filter:done', value)
        updateLocationParam('nameFilter', value)

    submit: (event) ->
        event.preventDefault()


class window.RefreshToggleView extends Backbone.View

    initialize: ->
        @listenTo(mainView, 'closeView', @model.disable_refresh)

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

    # TODO: why does text get stuck after a couple refresh?
    render: =>
        if @model.enabled
            seconds = @model.interval / 1000
            text = _.template("Auto-refresh <%= seconds %>s ")(seconds:seconds)
            active = "active"
        else
            text = active = ""
        @$el.html @template(text: text, active: active)
        @

    events:
        "click button":        "toggle"

    toggle: (event) =>
        @model.toggle(event)
        @render()
