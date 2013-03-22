

# Generic models

class window.RefreshModel extends Backbone.Model

    initialize: (options) =>
        options = options || {}
        @interval = (options.interval || 5) * 1000
        @enabled = false
        @timeout = null

    toggle: (event) =>
        if not @enabled
            @enable_refresh()
            @trigger('toggle:on')
        else
            @disable_refresh()
            @trigger('toggle:off')

    enable_refresh: =>
        if not @enabled
            console.log("Enabling refresh")
            @enabled = true
            @schedule_refresh()

    disable_refresh: =>
        console.log("Disableing refresh ")
        @enabled = false
        @clear()

    clear: =>
        clearTimeout(@timeout)
        @timeout = null

    do_refresh: =>
        @clear()
        if @enabled
            console.log("trigger refresh event")
            @trigger('refresh')
            @schedule_refresh()

    schedule_refresh: =>
        if not @timeout
            console.log("scheduled with " + @interval)
            @timeout = setTimeout(@do_refresh, @interval)
