
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

    tagName: "form"

    className: "pull-right"

    render: ->
        @$el.html """ <input type="text" placeholder="Filter by name"> """
        @

    events:
        "keyup input":  "filter_change"
        "submit":       "submit"

    filter_change: ->
        @trigger('filter_change', @$('input').val())

    submit: (event) ->
        event.preventDefault()
