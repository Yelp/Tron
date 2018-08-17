/*
 * decaffeinate suggestions:
 * DS001: Remove Babel/TypeScript constructor workaround
 * DS101: Remove unnecessary use of Array.from
 * DS102: Remove unnecessary code created because of implicit returns
 * DS205: Consider reworking code to avoid use of IIFEs
 * DS206: Consider reworking classes to avoid initClass
 * Full docs: https://github.com/decaffeinate/decaffeinate/blob/master/docs/suggestions.md
 */

// Dashboard
window.modules = window.modules || {}
window.modules.dashboard = module = {}

window.Dashboard = class Dashboard extends Backbone.Model {

    constructor(...args) {
        {
          // Hack: trick Babel/TypeScript into allowing this before super.
          if (false) { super(); }
          let thisFn = (() => { return this; }).toString();
          let thisName = thisFn.slice(thisFn.indexOf('return') + 6 + 1, thisFn.indexOf(';')).trim();
          eval(`${thisName} = this;`);
        }
        this.fetch = this.fetch.bind(this);
        this.models = this.models.bind(this);
        this.sorted = this.sorted.bind(this);
        this.filter = this.filter.bind(this);
        super(...args);
    }

    initialize(options){
        options = options || {};
        this.refreshModel = new RefreshModel({interval: 30});
        this.filterModel = options.filterModel;
        this.jobList = new JobCollection();
        return this.listenTo(this.jobList, "sync", this.change);
    }

    fetch() {
        return this.jobList.fetch();
    }

    change(args) {
        return this.trigger("change", args);
    }

    models() {
        return this.jobList.models;
    }

    sorted() {
        return _.sortBy(this.models(), item => item.get('name'));
    }

    filter(filter) {
        return _.filter(this.sorted(), filter);
    }
};


const matchType = function(item, query) {
    switch (query) {
        case 'job': if (item instanceof Job) { return true; } break;
    }
};


Cls = (window.DashboardFilterModel = class DashboardFilterModel extends FilterModel {
    static initClass() {

        this.prototype.filterTypes = {
            name:       buildMatcher(fieldGetter('name'), matchAny),
            type:       buildMatcher(_.identity, matchType)
        };
    }
});
Cls.initClass();


Cls = (window.DashboardFilterView = class DashboardFilterView extends FilterView {
    static initClass() {

        this.prototype.createtype = _.template(`\
<div class="input-prepend">
   <i class="icon-markerright icon-grey"></i>
   <div class="filter-select">
     <select id="filter-<%= filterName %>"
          class="span3"
          data-filter-name="<%= filterName %>Filter">
      <option value="">All</option>
      <option <%= isSelected(defaultValue, 'job') %>
          value="job">Scheduled Jobs</option>
    </select>
  </div>
</div>\
`
        );
    }
});
Cls.initClass();

Cls = (window.DashboardView = class DashboardView extends Backbone.View {
    constructor(...args) {
        {
          // Hack: trick Babel/TypeScript into allowing this before super.
          if (false) { super(); }
          let thisFn = (() => { return this; }).toString();
          let thisName = thisFn.slice(thisFn.indexOf('return') + 6 + 1, thisFn.indexOf(';')).trim();
          eval(`${thisName} = this;`);
        }
        this.initialize = this.initialize.bind(this);
        this.makeView = this.makeView.bind(this);
        this.renderBoxes = this.renderBoxes.bind(this);
        super(...args);
    }

    static initClass() {

        this.prototype.tagName = "div";

        this.prototype.className = "span12 dashboard-view";

        this.prototype.template = _.template(`\
<h1>
    <i class="icon-th icon-white"></i>
    <small>Tron</small>
    <a href="#dashboard">Dashboard</a>
    <span id="refresh"></span>
</h1>
<div id="filter-bar"></div>
<div id="status-boxes">
</div>\
`
        );
    }

    initialize(options) {
        this.refreshView = new RefreshToggleView({model: this.model.refreshModel});
        this.filterView = new DashboardFilterView({model: this.model.filterModel});
        this.listenTo(this.model, "change", this.render);
        this.listenTo(this.refreshView, 'refreshView', () => this.model.fetch());
        return this.listenTo(this.filterView, "filter:change", this.renderBoxes);
    }

    makeView(model) {
        switch (model.constructor.name) {
            case Job.name: return new module.JobStatusBoxView({model});
        }
    }

    renderRefresh() {
        return this.$('#refresh').html(this.refreshView.render().el);
    }

    renderBoxes() {
        let model;
        const models = this.model.filter(this.model.filterModel.createFilter());
        const views = ((() => {
            const result = [];
            for (model of Array.from(models)) {                 result.push(this.makeView(model));
            }
            return result;
        })());
        return this.$('#status-boxes').html(Array.from(views).map((item) => item.render().el));
    }

    render() {
        this.$el.html(this.template());
        this.$('#filter-bar').html(this.filterView.render().el);
        this.renderBoxes();
        this.renderRefresh();
        return this;
    }
});
Cls.initClass();


Cls = (window.StatusBoxView = class StatusBoxView extends ClickableListEntry {
    constructor(...args) {
        {
          // Hack: trick Babel/TypeScript into allowing this before super.
          if (false) { super(); }
          let thisFn = (() => { return this; }).toString();
          let thisName = thisFn.slice(thisFn.indexOf('return') + 6 + 1, thisFn.indexOf(';')).trim();
          eval(`${thisName} = this;`);
        }
        this.initialize = this.initialize.bind(this);
        this.className = this.className.bind(this);
        this.render = this.render.bind(this);
        super(...args);
    }

    static initClass() {

        this.prototype.tagName = "div";

        this.prototype.template = _.template(`\
<div class="status-header">
    <a href="<%= url %>">
    <%= name %></a>
</div>
<span class="count">
  <i class="<%= icon %> icon-white"></i><%= count %>
</span>\
`
        );
    }

    initialize(options) {
        return this.listenTo(this.model, "change", this.render);
    }

    className() {
        return `span2 clickable status-box ${this.getState()}`;
    }

    render() {
        const context = _.extend({}, {
            url: this.buildUrl(),
            icon: this.icon,
            count: this.count(),
            name: formatName(this.model.attributes.name)
        }
        );
        this.$el.html(this.template(context));
        return this;
    }
});
Cls.initClass();

Cls = (module.JobStatusBoxView = class JobStatusBoxView extends StatusBoxView {
    constructor(...args) {
        {
          // Hack: trick Babel/TypeScript into allowing this before super.
          if (false) { super(); }
          let thisFn = (() => { return this; }).toString();
          let thisName = thisFn.slice(thisFn.indexOf('return') + 6 + 1, thisFn.indexOf(';')).trim();
          eval(`${thisName} = this;`);
        }
        this.buildUrl = this.buildUrl.bind(this);
        this.getState = this.getState.bind(this);
        this.count = this.count.bind(this);
        super(...args);
    }

    static initClass() {

        this.prototype.icon = "icon-time";
    }

    buildUrl() {
        return `#job/${this.model.get('name')}`;
    }

    // TODO: get state of last run if enabled
    getState() {
        return this.model.get('status');
    }

    count() {
        if (_.isEmpty(this.model.get('runs'))) { return 0; } else { return _.first(this.model.get('runs')).run_num; }
    }
});
Cls.initClass();
