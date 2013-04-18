

# Generic models

class window.RefreshModel extends Backbone.Model

    initialize: (options) =>
        options = options || {}
        @interval = (options.interval || 5) * 1000
        @enabled = false
        @timeout = null

    toggle: (event) =>
        if not @enabled
            @enableRefresh()
            @trigger('toggle:on')
        else
            @disableRefresh()
            @trigger('toggle:off')

    enableRefresh: =>
        if not @enabled
            console.log("Enabling refresh")
            @enabled = true
            @scheduleRefresh()

    disableRefresh: =>
        console.log("Disableing refresh ")
        @enabled = false
        @clear()

    clear: =>
        clearTimeout(@timeout)
        @timeout = null

    doRefresh: =>
        @clear()
        if @enabled
            console.log("trigger refresh event")
            @trigger('refresh')
            @scheduleRefresh()

    scheduleRefresh: =>
        if not @timeout
            console.log("scheduled with " + @interval)
            @timeout = setTimeout(@doRefresh, @interval)


window.matchAny = (item, query) ->
    ~item.toLowerCase().indexOf(query.toLowerCase())

window.buildMatcher = (getter, matcher) ->
    (item, query) -> matcher(getter(item), query)

window.fieldGetter = (name) ->
    (item) -> item.get(name)

window.nestedName = (field) ->
    (item) -> item.get(field)['name']


class window.FilterModel extends Backbone.Model

    filterTypes:
        name:       buildMatcher(fieldGetter('name'), matchAny)
        node_pool:  buildMatcher(nestedName('node_pool'), _.str.startsWith)
        state:      buildMatcher(fieldGetter('state'), _.str.startsWith)

    createFilter: =>
        filterFuncs = for type, func of @filterTypes
            do (type, func) =>
                query = @get("#{type}Filter")
                if query
                    (item) -> func(item, query)
                else
                    (item) -> true

        (item) -> _.every(filterFuncs, (func) -> func(item))
