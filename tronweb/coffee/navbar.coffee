window.modules = window.modules || {}
module = window.modules.navbar = {}

class module.NavView extends Backbone.View

    initialize: (options) ->

    tagName: "div"

    className: "navbar navbar-static-top"

    attributes:
        id: "menu"

    events:
        ".search-query click": "handleClick"

    handleClick: (event) ->
       console.log(event)

    template: """
          <div class="navbar-inner">
            <div class="container">
            <ul class="nav">
              <li class="brand">tron<span>web</span></li>
              <li><a href="#home">
                <i class="icon-th"></i>Dashboard</a>
              </li>
              <li><a href="#jobs">
                <i class="icon-time"></i>Scheduled Jobs</a>
              </li>
              <li><a href="#configs">
                <i class="icon-wrench"></i>Config</a>
              </li>
            </ul>

            <form class="navbar-search pull-right">
            </form>

            </div>
          </div>
    """

    typeaheadTemplate: """
        <input type="text" class="input-medium search-query typeahead"
            placeholder="Search"
            autocomplete="off"
            data-provide="typeahead">
        <div class="icon-search"></div>
    """

    render: =>
        @$el.html @template
        @renderTypeahead()
        @

    updater: (item) =>
        entry = @model.get(item)
        routes.navigate(entry.getUrl(), trigger: true)
        entry.name

    source: (query, process) =>
        (entry.name for _, entry of @model.attributes)

    highlighter: (item) =>
        # Also formats the item for display
        typeahead = @$('.typeahead').data().typeahead
        name = module.typeahead_hl.call(typeahead, item)
        entry = @model.get(item)
        "<small>#{entry.type}</small> #{name}"

    sorter: (items) ->
        [startsWithQuery, containsQuery] = [[], []]
        query = @query.toLowerCase()
        for item in items
            uncasedItem = item.toLowerCase()
            if _.str.startsWith(uncasedItem, query) then startsWithQuery.push item
            else if _.str.include(uncasedItem, query) then containsQuery.push item

        lengthSort = (item) -> item.length
        _.sortBy(startsWithQuery, lengthSort)
            .concat(_.sortBy(containsQuery, lengthSort))

    renderTypeahead: =>
        @$('.navbar-search').html @typeaheadTemplate
        @$('.typeahead').typeahead
            source: @source,
            updater: @updater
            highlighter: @highlighter,
            sorter: @sorter
        @

    setActive: =>
        @$('li').removeClass 'active'
        [path, params] = modules.routes.getLocationParams()
        path = path.split('/')[0]
        @$("a[href=#{path}]").parent('li').addClass 'active'

Typeahead = $.fn.typeahead.Constructor.prototype

Typeahead.show = ->
    top = @$element.position().top + @$element[0].offsetHeight + 1
    @$menu.insertAfter(@$element).css(top: top).show()
    @shown = true
    @

module.typeahead_hl = $.fn.typeahead.Constructor.prototype.highlighter
