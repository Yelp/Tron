
# Models
# This file provides core model functionality and sets up global Backbone behaviour
window.modules = window.modules || {}
module = window.modules.models = {}

# Override Backbone's sync to prepend '/api' to all API URLs
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
            @enabled = true
            @scheduleRefresh()

    disableRefresh: =>
        @enabled = false
        @clear()

    clear: =>
        clearTimeout(@timeout)
        @timeout = null

    doRefresh: =>
        @clear()
        if @enabled
            @trigger('refresh')
            @scheduleRefresh()

    scheduleRefresh: =>
        if not @timeout
            @timeout = setTimeout(@doRefresh, @interval)

class window.StatusModel extends Backbone.Model

    urlRoot: ->
        "/status/"

    parse: (resp) =>
        booted = moment.unix(resp['boot_time']).format('YYYY-MM-DD HH:mm ZZ')
        uptime = moment.duration(moment() - booted).minutes()
        $('#version').html('<b>Tron:</b> v' + resp['version'] + ' <b>Boot:</b> ' + booted + '</b>')
        resp

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

class ConfigIndexEntry extends IndexEntry

    type: "Config"

    getUrl: =>
        "#config/#{@name}"

class CommandIndexEntry extends IndexEntry

    constructor: (@name, @job_name, @action_name) ->
        @name = name

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
        _.flatten(nested)

    parse: (resp, options) =>
        index = [].concat(
            @getJobEntries(resp['jobs']),
            new ConfigIndexEntry name for name in resp['namespaces'])

        _.mash([entry.name, entry] for entry in index)
