/*
 * decaffeinate suggestions:
 * DS101: Remove unnecessary use of Array.from
 * DS102: Remove unnecessary code created because of implicit returns
 * DS205: Consider reworking code to avoid use of IIFEs
 * DS206: Consider reworking classes to avoid initClass
 * Full docs: https://github.com/decaffeinate/decaffeinate/blob/master/docs/suggestions.md
 */

// Configs


Cls = (window.NamespaceList = class NamespaceList extends Backbone.Model {
    static initClass() {

        this.prototype.url = "/";
    }
});
Cls.initClass();


window.Config = class Config extends Backbone.Model {

    constructor(...args) {
        this.url = this.url.bind(this);
        super(...args);
    }

    url() {
        return `/config?name=${this.get('name')}`;
    }
};


class NamespaceListEntryView extends ClickableListEntry {
    static initClass() {

        this.prototype.tagName = "tr";

        this.prototype.template = _.template(`\
<td>
    <a href="#config/<%= name %>">
        <span class="label label-inverse"><%= name %></span>
    </a>
</td>\
`
        );
    }

    render() {
        this.$el.html(this.template({
            name: this.model})
        );
        return this;
    }
}
NamespaceListEntryView.initClass();


Cls = (window.NamespaceListView = class NamespaceListView extends Backbone.View {
    constructor(...args) {
        this.initialize = this.initialize.bind(this);
        this.render = this.render.bind(this);
        super(...args);
    }

    static initClass() {

        this.prototype.tagName = "div";

        this.prototype.className = "span8";

        this.prototype.template = _.template(`\
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
</div>\
`
        );
    }

    initialize(options) {
        return this.listenTo(this.model, "sync", this.render);
    }


    render() {
        let name;
        this.$el.html(this.template());
        const entry = name => new NamespaceListEntryView({model: name}).render().el;
        this.$('tbody').append((() => {
            const result = [];
            for (name of Array.from(this.model.get('namespaces'))) {                 result.push(entry(name));
            }
            return result;
        })());
        return this;
    }
});
Cls.initClass();


Cls = (window.ConfigView = class ConfigView extends Backbone.View {
    constructor(...args) {
        this.initialize = this.initialize.bind(this);
        this.render = this.render.bind(this);
        super(...args);
    }

    static initClass() {

        this.prototype.tagName = "div";

        this.prototype.className = "span12";

        this.prototype.template = _.template(`\
<h1><small>Config</small> <%= name %></h1>
<div class="outline-block"><div class="border-top">
    <textarea class="config-block"><%= config %></textarea>
</div></div>\
`
        );
    }

    initialize(options) {
        return this.listenTo(this.model, "change", this.render);
    }

    render() {
        this.$el.html(this.template(this.model.attributes));
        CodeMirror.fromTextArea(this.$('textarea').get(0), {readOnly: true});
        return this;
    }
});
Cls.initClass();
