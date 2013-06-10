
# Configs


class window.NamespaceList extends Backbone.Model

    url: "/"


class window.Config extends Backbone.Model

    url: =>
        "/config?name=" + @get('name')


class NamespaceListEntryView extends ClickableListEntry

    tagName: "tr"

    template: _.template """
        <td>
            <a href="#config/<%= name %>">
                <span class="label label-inverse"><%= name %></span>
            </a>
        </td>
        """

    render: ->
        @$el.html @template
            name: @model
        @


class window.NamespaceListView extends Backbone.View

    initialize: (options) =>
        @listenTo(@model, "sync", @render)

    tagName: "div"

    className: "span8"

    template: _.template """
        <h1>
            <i class="icon-wrench icon-white"></i>
            Configuration Namespaces
        </h1>
        <div class="outline-block">
        <table class="table table-hover table-outline">
          <thead class="header">
            <tr>
              <th>Name</th>
            </tr>
          </thead>
          <tbody>
          </tbody>
        </table>
        </div>
        """


    render: =>
        @$el.html @template()
        entry = (name) -> new NamespaceListEntryView(model: name).render().el
        @$('tbody').append(entry(name) for name in @model.get('namespaces'))
        @


class window.ConfigView extends Backbone.View

    initialize: (options) =>
        @listenTo(@model, "change", @render)

    tagName: "div"

    className: "span12"

    template: _.template """
        <h1><small>Config</small> <%= name %></h1>
        <div class="outline-block"><div class="border-top">
            <textarea class="config-block"><%= config %></textarea>
        </div></div>
        """

    render: =>
        @$el.html @template(@model.attributes)
        CodeMirror.fromTextArea(@$('textarea').get(0), readOnly: true)
        @
