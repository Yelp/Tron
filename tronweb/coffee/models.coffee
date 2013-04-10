

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


class window.FilterModel extends Backbone.Model

    filterTypes: ['name', 'node_pool', 'state']

    createFilter: =>
        filterFuncs = for type in @filterTypes
            do (type) =>
                filterValue = @get("#{type}Filter")
                if filterValue
                    (item) -> _.str.startsWith(item.get(type), filterValue)
                else
                    (item) -> true

        (item) -> _.every(filterFuncs, (func) -> func(item))
