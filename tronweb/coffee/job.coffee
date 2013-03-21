

# Jobs

class window.Job extends Backbone.Model

    idAttribute: "name"

    urlRoot: "/jobs"

    parse: (resp, options) =>
        resp['last_success'] = if resp['last_success'] then moment(resp['last_success']).fromNow() else 'Never'
        resp


class window.JobCollection extends Backbone.Collection

    model: Job

    url: "/jobs"

    parse: (resp, options) =>
        resp['jobs']


class window.JobListView extends Backbone.View

    initialize: (options) =>
        @listenTo(@model, "sync", @render)
        @listenTo(@model, "sort", @render)

    tagName: "div"

    className: "span12"

    # TODO: filter by name
    template: _.template '
        <h1>Jobs</h1>
        <table class="table table-hover">
            <thead>
                <tr>
                    <th>Name</td>
                    <th>Schedule</td>
                    <th>Node Pool</td>
                    <th>Last Success</td>
                    <th>Next Run</td>
                </tr>
            </thead>
            <tbody>
            </tbody>
        <table>'

    # TODO: sort by name/state/node
    render: ->
        @$el.html @template()
        entry = (model) -> new JobListEntryView(model: model).render().el
        @$('tbody').append(entry(model) for model in @model.models)
        @


class JobListEntryView extends Backbone.View

    initialize: (options) =>
        @listenTo(@model, "change", @render)

    tagName: "tr"

    className: =>
        switch @model.attributes.status
            when "DISABLED" then 'warning'
            when "ENABLED"  then 'enabled'
            when "RUNNING"  then 'info'

    template: _.template '
        <td><a href="#service/<%= name %>"><%= name %></a></td>
        <td><%= scheduler %></td>
        <td><%= node_pool %></td>
        <td><%= last_success %></td>
        <td></td>'

    render: ->
        @$el.html @template(@model.attributes)
        @
