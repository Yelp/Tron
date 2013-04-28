
# Generic models
window.modules = window.modules || {}
module = window.modules.models = {}


backboneSync = Backbone.sync

Backbone.sync = (method, model, options) ->
    options.url = '/api' + _.result(model, 'url')
    backboneSync(method, model, options)


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
        state:      buildMatcher(fieldGetter('state'), _.str.startsWith)
        node_pool:  buildMatcher(nestedName('node_pool'), _.str.startsWith)

    createFilter: =>
        filterFuncs = for type, func of @filterTypes
            do (type, func) =>
                query = @get("#{type}Filter")
                if query
                    (item) -> func(item, query)
                else
                    (item) -> true

        (item) -> _.every(filterFuncs, (func) -> func(item))


class IndexEntry

    constructor: (@name) ->

    toLowerCase: =>
        @name.toLowerCase()

    replace: (args...) =>
        @name.replace(args...)

    indexOf: (args...) =>
        @name.indexOf(args...)

    toString: =>
       "#{@type} #{@name}"


class JobIndexEntry extends IndexEntry

    type: "Job"

    getUrl: =>
        "#job/#{@name}"

class ServiceIndexEntry extends IndexEntry

    type: "Service"

    getUrl: =>
        "#service/#{@name}"

class ConfigIndexEntry extends IndexEntry

    type: "Config"

    getUrl: =>
        "#config/#{@name}"

class CommandIndexEntry extends IndexEntry

    constructor: (@name, @job_name, @action_name) ->

    type: "command"

    getUrl: =>
        "#job/#{@job_name}/-1/#{@action_name}"


class module.QuickFindModel extends Backbone.Model

    url: "/"

    getJobEntries: (jobs) =>
        buildActions = (actions) ->
            for action in actions
                new CommandIndexEntry(action.command, name, action.name)

        nested = for name, actions of jobs
            [new JobIndexEntry(name), buildActions(actions)]
        console.log nested
        flat = _.flatten(nested)
        console.log flat
        flat

    parse: (resp, options) =>
        index = [].concat(
            @getJobEntries(resp['jobs']),
            new ServiceIndexEntry name for name in resp['services'],
            new ConfigIndexEntry name for name in resp['namespaces'])

        _.mash([entry.name, entry] for entry in index)
