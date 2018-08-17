/*
 * decaffeinate suggestions:
 * DS001: Remove Babel/TypeScript constructor workaround
 * DS101: Remove unnecessary use of Array.from
 * DS102: Remove unnecessary code created because of implicit returns
 * DS205: Consider reworking code to avoid use of IIFEs
 * DS206: Consider reworking classes to avoid initClass
 * DS207: Consider shorter variations of null checks
 * Full docs: https://github.com/decaffeinate/decaffeinate/blob/master/docs/suggestions.md
 */


window.modules = window.modules || {}
module = window.modules.actionrun = {}


Cls = (module.ActionRun = class ActionRun extends Backbone.Model {
    constructor(...args) {
        {
          // Hack: trick Babel/TypeScript into allowing this before super.
          if (false) { super(); }
          let thisFn = (() => { return this; }).toString();
          let thisName = thisFn.slice(thisFn.indexOf('return') + 6 + 1, thisFn.indexOf(';')).trim();
          eval(`${thisName} = this;`);
        }
        this.initialize = this.initialize.bind(this);
        this.url = this.url.bind(this);
        this.parse = this.parse.bind(this);
        super(...args);
    }

    static initClass() {

        this.prototype.idAttribute = "action_name";

        this.prototype.urlArgs = "?include_stdout=1&include_stderr=1&num_lines=0";
    }

    initialize(options) {
        super.initialize(options);
        options = options || {};
        return this.refreshModel = options.refreshModel;
    }

    urlRoot() {
        return `/jobs/${ this.get('job_name') }/${ this.get('run_num') }/`;
    }

    url() {
        return super.url() + this.urlArgs;
    }

    parse(resp, options) {
        resp['job_url'] = `#job/${ this.get('job_name') }`;
        resp['job_run_url'] = `${ resp['job_url'] }/${ this.get('run_num') }`;
        resp['url'] = `${ resp['job_run_url'] }/${ this.get('action_name') }`;
        return resp;
    }
});
Cls.initClass();


Cls = (module.ActionRunHistoryEntry = class ActionRunHistoryEntry extends module.ActionRun {
    constructor(...args) {
        {
          // Hack: trick Babel/TypeScript into allowing this before super.
          if (false) { super(); }
          let thisFn = (() => { return this; }).toString();
          let thisName = thisFn.slice(thisFn.indexOf('return') + 6 + 1, thisFn.indexOf(';')).trim();
          eval(`${thisName} = this;`);
        }
        this.parse = this.parse.bind(this);
        super(...args);
    }

    static initClass() {

        this.prototype.idAttribute = "id";
    }

    parse(resp, options) {
        return resp;
    }
});
Cls.initClass();


Cls = (module.ActionRunHistory = class ActionRunHistory extends Backbone.Collection {
    constructor(...args) {
        {
          // Hack: trick Babel/TypeScript into allowing this before super.
          if (false) { super(); }
          let thisFn = (() => { return this; }).toString();
          let thisName = thisFn.slice(thisFn.indexOf('return') + 6 + 1, thisFn.indexOf(';')).trim();
          eval(`${thisName} = this;`);
        }
        this.initialize = this.initialize.bind(this);
        this.url = this.url.bind(this);
        this.parse = this.parse.bind(this);
        this.reset = this.reset.bind(this);
        this.add = this.add.bind(this);
        super(...args);
    }

    static initClass() {

        this.prototype.model = module.ActionRunHistoryEntry;
    }

    initialize(models, options) {
        options = options || {};
        this.job_name = options.job_name;
        return this.action_name = options.action_name;
    }

    url() {
        return `/jobs/${ this.job_name }/${ this.action_name }/`;
    }

    parse(resp, options) {
        return resp;
    }

    reset(models, options) {
        return super.reset(models, options);
    }

    add(models, options) {
        return super.add(models, options);
    }
});
Cls.initClass();


Cls = (module.ActionRunHistoryListEntryView = class ActionRunHistoryListEntryView extends ClickableListEntry {
    static initClass() {

        this.prototype.tagName = "tr";

        this.prototype.template = _.template(`\
<td>
    <a href="#job/<%= job_name %>/<%= run_num %>/<%= action_name %>">
    <%= run_num %></a></td>
<td><%= formatState(state) %></td>
<td><%= displayNode(node) %></td>
<td><%= modules.actionrun.formatExit(exit_status) %></td>
<td><%= dateFromNow(start_time, "None") %></td>
<td><%= dateFromNow(end_time, "") %></td>\
`
        );
    }

    render() {
        this.$el.html(this.template(this.model.attributes));
        makeTooltips(this.$el);
        return this;
    }
});
Cls.initClass();


module.ActionRunTimelineEntry = class ActionRunTimelineEntry {

    constructor(actionRun, maxDate) {
        this.toString = this.toString.bind(this);
        this.getYAxisLink = this.getYAxisLink.bind(this);
        this.getYAxisText = this.getYAxisText.bind(this);
        this.getBarClass = this.getBarClass.bind(this);
        this.getStart = this.getStart.bind(this);
        this.getEnd = this.getEnd.bind(this);
        this.actionRun = actionRun;
        this.maxDate = maxDate;
    }

    toString() {
        return this.actionRun.action_name;
    }

    getYAxisLink() {
        return `#job/${this.actionRun.job_name}/${this.actionRun.run_num}/${this.actionRun.action_name}`;
    }

    getYAxisText() {
        return this.actionRun.action_name;
    }

    getBarClass() {
        return this.actionRun.state;
    }

    getStart() {
        return this.getDate(this.actionRun.start_time);
    }

    getEnd() {
        return this.getDate(this.actionRun.end_time);
    }

    getDate(date) {
        if (date) { return new Date(date); } else { return this.maxDate; }
    }
};


Cls = (module.ActionRunListEntryView = class ActionRunListEntryView extends ClickableListEntry {
    constructor(...args) {
        {
          // Hack: trick Babel/TypeScript into allowing this before super.
          if (false) { super(); }
          let thisFn = (() => { return this; }).toString();
          let thisName = thisFn.slice(thisFn.indexOf('return') + 6 + 1, thisFn.indexOf(';')).trim();
          eval(`${thisName} = this;`);
        }
        this.initialize = this.initialize.bind(this);
        super(...args);
    }

    static initClass() {

        this.prototype.tagName = "tr";

        this.prototype.template = _.template(`\
<td>
    <a href="#job/<%= job_name %>/<%= run_num %>/<%= action_name %>">
    <%= formatName(action_name) %></a></td>
<td><%= formatState(state) %></td>
<td><code class="command"><%= command || raw_command %></code></td>
<td><%= displayNode(node) %></td>
<td><%= dateFromNow(start_time, "None") %></td>
<td><%= dateFromNow(end_time, "") %></td>\
`
        );
    }

    initialize(options) {
        return this.listenTo(this.model, "change", this.render);
    }

    render() {
        this.$el.html(this.template(this.model.attributes));
        makeTooltips(this.$el);
        return this;
    }
});
Cls.initClass();


module.formatExit = function(exit) {
    if ((exit == null) || (exit === '')) { return ''; }
    const template = _.template(`\
<span class="badge badge-<%= type %>"><%= exit %></span>\
`
    );
    return template({exit, type: !exit ? "success" : "important"});
};


Cls = (module.ActionRunView = class ActionRunView extends Backbone.View {
    constructor(...args) {
        {
          // Hack: trick Babel/TypeScript into allowing this before super.
          if (false) { super(); }
          let thisFn = (() => { return this; }).toString();
          let thisName = thisFn.slice(thisFn.indexOf('return') + 6 + 1, thisFn.indexOf(';')).trim();
          eval(`${thisName} = this;`);
        }
        this.initialize = this.initialize.bind(this);
        super(...args);
    }

    static initClass() {

        this.prototype.tagName = "div";

        this.prototype.template = _.template(`\
<div class="span12">
    <h1>
        <small>Action Run</small>
        <a href="<%= job_url %>"><%= formatName(job_name) %></a>.
        <a href="<%= job_run_url %>"><%= run_num %></a>.
        <%= formatName(action_name) %>
        <span id="refresh"></span>
    </h1>
</div>
<div class="span12 outline-block">
    <h2>Details</h2>
    <div>
    <table class="table details">
        <tbody>
        <tr><td class="span2">State</td>
            <td><%= formatState(state) %><%= formatDelay(in_delay) %></td></tr>
        <tr><td>Node</td>
            <td><%= displayNode(node) %></td></tr>
        <tr><td>Raw command</td>
            <td><code class="command"><%= raw_command %></code></td></tr>
        <% if (command) { %>
        <tr><td>Command</td>
            <td><code class="command"><%= command %></code></td></tr>
        <% } %>
        <tr><td>Exit codes</td>
            <td>
                <%= modules.actionrun.formatExit(exit_status) %>
                <% if (exit_statuses) { %>
                    <small>
                        (exits of failed attempts:
                        <%= _.map(
                                _.sortBy(
                                    exit_statuses,
                                    function(val, key) {
                                        return -key;
                                    }
                                ),
                                modules.actionrun.formatExit
                            ).join(", ") %>)
                    </small>
                <% } %>
            </td>
        </tr>
        <tr><td>Start time</td>
            <td><% print(dateFromNow(start_time, ''))  %></td></tr>
        <tr><td>End time</td>
            <td><%= dateFromNow(end_time, 'Unknown') %></td></tr>
        <tr><td>Duration</td>
            <td><%= formatDuration(duration) %>
            </td></tr>
        </tbody>
    </table>
    </div>
</div>
<div class="span12 outline-block">
    <h2>stdout</h2>
    <pre class="stdout"><%= stdout.join('\\n') %></pre>
</div>
<div class="span12 outline-block">
    <h2>stderr</h2>
    <pre class="stderr"><%= stderr.join('\\n') %></pre>
</div>

<div id="action-run-history">
</div>\
`
        );
    }

    initialize(options) {
        this.listenTo(this.model, "change", this.render);
        this.refreshView = new RefreshToggleView({model: this.model.refreshModel});
        const historyCollection = options.history;
        this.historyView = new module.ActionRunHistoryView({model: historyCollection});
        this.listenTo(this.refreshView, 'refreshView', () => this.model.fetch());
        return this.listenTo(this.refreshView, 'refreshView', () => historyCollection.fetch());
    }

    render() {
        this.$el.html(this.template(this.model.attributes));
        this.$('#refresh').html(this.refreshView.render().el);
        this.$('#action-run-history').html(this.historyView.render().el);
        makeTooltips(this.$el);
        modules.views.makeHeaderToggle(this.$el);
        return this;
    }
});
Cls.initClass();

class ActionRunHistorySliderModel {

    constructor(model) {
        this.length = this.length.bind(this);
        this.model = model;
    }

    length() {
        return this.model.models.length;
    }
}


Cls = (module.ActionRunHistoryView = class ActionRunHistoryView extends Backbone.View {
    constructor(...args) {
        {
          // Hack: trick Babel/TypeScript into allowing this before super.
          if (false) { super(); }
          let thisFn = (() => { return this; }).toString();
          let thisName = thisFn.slice(thisFn.indexOf('return') + 6 + 1, thisFn.indexOf(';')).trim();
          eval(`${thisName} = this;`);
        }
        this.initialize = this.initialize.bind(this);
        this.renderList = this.renderList.bind(this);
        this.render = this.render.bind(this);
        super(...args);
    }

    static initClass() {

        this.prototype.tagName = "div";

        this.prototype.className = "span12 outline-block";

        this.prototype.template = _.template(`\
<h2>History</h2>
<div>
<div id="slider"></div>
<table class="table table-hover table-outline table-striped">
  <thead class="sub-header">
    <tr>
      <th class="span1">Run</th>
      <th>State</th>
      <th>Node</th>
      <th>Exit</th>
      <th>Start</th>
      <th>End</th>
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
        this.listenTo(this.model, "sync", this.render);
        const sliderModel = new ActionRunHistorySliderModel(this.model);
        this.sliderView = new modules.views.SliderView({model: sliderModel});
        return this.listenTo(this.sliderView, "slider:change", this.renderList);
    }

    renderList() {
        let model;
        const view = model => new module.ActionRunHistoryListEntryView({model}).render().el;
        const models = this.model.models.slice(0, this.sliderView.displayCount);
        return this.$('tbody').html((() => {
            const result = [];
            for (model of Array.from(models)) {                 result.push(view(model));
            }
            return result;
        })());
    }

    render() {
        this.$el.html(this.template());
        this.renderList();
        if (this.model.models.length) { this.$('#slider').html(this.sliderView.render().el); }
        modules.views.makeHeaderToggle(this.$el.parent());
        return this;
    }
});
Cls.initClass();
