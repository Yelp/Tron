/*
 * decaffeinate suggestions:
 * DS101: Remove unnecessary use of Array.from
 * DS102: Remove unnecessary code created because of implicit returns
 * DS205: Consider reworking code to avoid use of IIFEs
 * DS206: Consider reworking classes to avoid initClass
 * DS207: Consider shorter variations of null checks
 * Full docs: https://github.com/decaffeinate/decaffeinate/blob/master/docs/suggestions.md
 */

// Jobs
//window.modules = window.modules || {}
//window.modules.job = module = {}


Cls = (window.Job = class Job extends Backbone.Model {
    constructor(...args) {
        this.initialize = this.initialize.bind(this);
        super(...args);
    }

    static initClass() {

        this.prototype.idAttribute = "name";

        this.prototype.urlRoot = "/jobs";
    }

    initialize(options) {
        super.initialize(options);
        options = options || {};
        return this.refreshModel = options.refreshModel;
    }

    url() {
        return super.url() + "?include_action_graph=1";
    }
});
Cls.initClass();


Cls = (window.JobCollection = class JobCollection extends Backbone.Collection {
    constructor(...args) {
        this.initialize = this.initialize.bind(this);
        this.parse = this.parse.bind(this);
        this.comparator = this.comparator.bind(this);
        super(...args);
    }

    static initClass() {

        this.prototype.model = Job;

        this.prototype.url = "/jobs?include_job_runs=1";
    }

    initialize(models, options) {
        super.initialize(options);
        options = options || {};
        this.refreshModel = options.refreshModel;
        return this.filterModel = options.filterModel;
    }

    parse(resp, options) {
        return resp['jobs'];
    }

    comparator(job) {
        return job.get('name');
    }
});
Cls.initClass();


Cls = (window.JobRun = class JobRun extends Backbone.Model {
    constructor(...args) {
        this.initialize = this.initialize.bind(this);
        this.url = this.url.bind(this);
        this.parse = this.parse.bind(this);
        super(...args);
    }

    static initClass() {

        this.prototype.idAttribute = "run_num";
    }

    initialize(options) {
        super.initialize(options);
        options = options || {};
        return this.refreshModel = options.refreshModel;
    }

    urlRoot() {
        return `/jobs/${this.get('name')}`;
    }

    url() {
        return super.url() + "?include_action_graph=1&include_action_runs=1";
    }

    parse(resp, options) {
        resp['job_url'] = `#job/${resp['job_name']}`;
        return resp;
    }
});
Cls.initClass();


Cls = (window.JobListFilterModel = class JobListFilterModel extends FilterModel {
    static initClass() {

        this.prototype.filterTypes = {
            name:       buildMatcher(fieldGetter('name'), matchAny),
            status:     buildMatcher(fieldGetter('status'), _.str.startsWith),
            node_pool:  buildMatcher(nestedName('node_pool'), _.str.startsWith)
        };
    }
});
Cls.initClass();


Cls = (window.JobListView = class JobListView extends Backbone.View {
    constructor(...args) {
        this.initialize = this.initialize.bind(this);
        this.renderList = this.renderList.bind(this);
        this.renderFilter = this.renderFilter.bind(this);
        super(...args);
    }

    static initClass() {

        this.prototype.tagName = "div";

        this.prototype.className = "span12";

        this.prototype.template = _.template(`\
<h1>
    <i class="icon-time icon-white"></i> Scheduled Jobs
    <span id="refresh"></span>
</h1>
<div id="filter-bar"></div>
<div class="outline-block">
<table class="table table-hover table-outline table-striped">
    <thead class="header">
        <tr>
            <th class="span4">Name</th>
            <th>Status</th>
            <th>Schedule</th>
            <th>Node Pool</th>
            <th>Last Success</th>
            <th>Next Run</th>
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
        this.refreshView = new RefreshToggleView({model: this.model.refreshModel});
        this.filterView = new FilterView({model: this.model.filterModel});
        this.listenTo(this.refreshView, 'refreshView', () => this.model.fetch());
        return this.listenTo(this.filterView, "filter:change", this.renderList);
    }

    render() {
        this.$el.html(this.template());
        this.renderFilter();
        this.$('#refresh').html(this.refreshView.render().el);
        this.renderList();
        return this;
    }

    renderList() {
        let model;
        const models = this.model.filter(this.model.filterModel.createFilter());
        const entry = model => new JobListEntryView({model}).render().el;
        return this.$('tbody').html((() => {
            const result = [];
            for (model of Array.from(models)) {                 result.push(entry(model));
            }
            return result;
        })());
    }

    renderFilter() {
        return this.$('#filter-bar').html(this.filterView.render().el);
    }
});
Cls.initClass();


class JobListEntryView extends ClickableListEntry {
    constructor(...args) {
        this.initialize = this.initialize.bind(this);
        super(...args);
    }

    static initClass() {

        this.prototype.tagName = "tr";

        this.prototype.className = "clickable";

        this.prototype.template = _.template(`\
<td><a href="#job/<%= name %>"><%= formatName(name) %></a></td>
<td><%= formatState(status) %></td>
<td><%= formatScheduler(scheduler) %></td>
<td><%= displayNodePool(node_pool) %></td>
<td><%= dateFromNow(last_success, 'never') %></td>
<td><%= dateFromNow(next_run, 'none') %></td>\
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
}
JobListEntryView.initClass();


class JobRunTimelineEntry {

    constructor(jobRun, maxDate) {
        this.toString = this.toString.bind(this);
        this.getYAxisLink = this.getYAxisLink.bind(this);
        this.getYAxisText = this.getYAxisText.bind(this);
        this.getBarClass = this.getBarClass.bind(this);
        this.getStart = this.getStart.bind(this);
        this.getEnd = this.getEnd.bind(this);
        this.jobRun = jobRun;
        this.maxDate = maxDate;
    }

    toString() {
        return this.jobRun.run_num;
    }

    getYAxisLink() {
        return `#job/${this.jobRun.job_name}/${this.jobRun.run_num}`;
    }

    getYAxisText() {
        return this.jobRun.run_num;
    }

    getBarClass() {
        return this.jobRun.state;
    }

    getStart() {
        return new Date(this.jobRun.start_time || this.jobRun.run_time);
    }

    getEnd() {
        if (this.jobRun.state === 'running') { return this.maxDate; }
        return new Date(this.jobRun.end_time || this.jobRun.start_time || this.jobRun.run_time);
    }
}


Cls = (window.JobView = class JobView extends Backbone.View {
    constructor(...args) {
        this.initialize = this.initialize.bind(this);
        this.renderGraph = this.renderGraph.bind(this);
        this.renderTimeline = this.renderTimeline.bind(this);
        this.formatSettings = this.formatSettings.bind(this);
        super(...args);
    }

    static initClass() {

        this.prototype.tagName = "div";

        this.prototype.className = "span12";

        this.prototype.template = _.template(`\
<div class="row">
    <div class="span12">
        <h1>
            <small>Job</small>
            <%= formatName(name) %>
            <span id="refresh"></span>
        </h1>
    </div>
    <div class="span5 outline-block">
        <h2>Details</h2>
        <div>
        <table class="table details">
            <tbody>
            <tr><td>Status</td>
                <td><%= formatState(status) %></td></tr>
            <tr><td>Node pool</td>
                <td><%= displayNodePool(node_pool) %></td></tr>
            <tr><td>Schedule</td>
                <td><%= formatScheduler(scheduler) %></td></tr>
            <tr><td>Settings</td>
                <td><%= settings %></td></tr>
            <tr><td>Last success</td>
                <td><%= dateFromNow(last_success) %></td></tr>
            <tr><td>Next run</td>
                <td><%= dateFromNow( next_run) %></td></tr>
            </tbody>
        </table>
        </div>
    </div>
    <div class="span7 outline-block">
        <h2>Action Graph</h2>
        <div id="action-graph" class="graph job-view"></div>
    </div>

    <div class="span12 outline-block">
      <h2>Timeline</h2>
      <div>
        <div id="slider-chart"></div>
        <div id="timeline-graph"></div>
      </div>
    </div>

    <div id="job-runs"></div>
</div>\
`
        );
    }

    initialize(options) {
        this.listenTo(this.model, "change", this.render);
        this.refreshView = new RefreshToggleView({model: this.model.refreshModel});
        this.jobRunListView = new module.JobRunListView({model: this.model});
        this.listenTo(this.refreshView, 'refreshView', () => this.model.fetch());
        const sliderModel = new JobRunListSliderModel(this.model);
        this.sliderView = new modules.views.SliderView({model: sliderModel});
        this.listenTo(this.sliderView, "slider:change", this.renderTimeline);
        return this.currentDate = new Date();
    }

    // TODO: move to JobActionGraphView
    renderGraph() {
        return new GraphView({
            model: this.model.get('action_graph'),
            buildContent(d) { return `<code class="command">${d.command}</code>`; },
            height: this.$('table.details').height() - 5 // TODO: why -5 to get it flush?
        }).render();
    }

    // TODO: move to JobTimelineView
    renderTimeline() {
        let jobRuns = this.model.get('runs').slice(0, this.sliderView.displayCount);
        jobRuns = (Array.from(jobRuns).map((run) => new JobRunTimelineEntry(run, this.currentDate)));
        return new modules.timeline.TimelineView({model: jobRuns}).render();
    }

    formatSettings(attrs) {
        const template = _.template(`\
<span class="label-icon tt-enable" title="<%= title %>">
    <i class="icon-<%= icon %>"></i>
</span>\
`
        );

        const [icon, title] = attrs.allow_overlap ?
            ['layers', "Allow overlapping runs"]
        : attrs.queueing ?
            ['circlepauseempty', "Queue overlapping runs"]
        :
            ['remove-circle', "Cancel overlapping runs"];

        const content = attrs.all_nodes ?
            template({icon: 'treediagram', title: "Run on all nodes"})
        :
            "";
        return template({icon, title}) + content;
    }

    render() {
        this.$el.html(this.template(_.extend({},
            this.model.attributes,
            {settings: this.formatSettings(this.model.attributes)})
        )
        );

        this.$('#job-runs').html(this.jobRunListView.render().el);
        this.$('#refresh').html(this.refreshView.render().el);
        this.renderGraph();
        this.renderTimeline();
        this.$('#slider-chart').html(this.sliderView.render().el);
        makeTooltips(this.$el);
        modules.views.makeHeaderToggle(this.$el);
        return this;
    }
});
Cls.initClass();


class JobRunListSliderModel {

    constructor(model) {
        this.length = this.length.bind(this);
        this.model = model;
    }

    length() {
        return this.model.get('runs').length;
    }
}


Cls = (module.JobRunListView = class JobRunListView extends Backbone.View {
    constructor(...args) {
        this.initialize = this.initialize.bind(this);
        this.renderList = this.renderList.bind(this);
        this.render = this.render.bind(this);
        super(...args);
    }

    static initClass() {

        this.prototype.tagName = "div";

        this.prototype.className = "span12 outline-block";

        this.prototype.template = _.template(`\
<h2>Job Runs</h2>
<div>
<div id="slider-table"></div>
<table class="table table-hover table-outline table-striped">
    <thead class="sub-header">
        <tr>
            <th>Id</th>
            <th>State</th>
            <th>Node</th>
            <th>Start</th>
            <th>End</th>
        </tr>
    </thead>
    <tbody class="jobruns">
    </tbody>
</table>
</div>\
`
        );
    }

    initialize(options) {
        const sliderModel = new JobRunListSliderModel(this.model);
        this.sliderView = new modules.views.SliderView({model: sliderModel});
        return this.listenTo(this.sliderView, "slider:change", this.renderList);
    }

    renderList() {
        let model;
        const entry = jobrun => new JobRunListEntryView({model:new JobRun(jobrun)}).render().el;
        const models = this.model.get('runs').slice(0, this.sliderView.displayCount);
        return this.$('tbody').html((() => {
            const result = [];
            for (model of Array.from(models)) {                 result.push(entry(model));
            }
            return result;
        })());
    }

    render() {
        this.$el.html(this.template(this.model.attributes));
        this.$('#slider-table').html(this.sliderView.render().el);
        this.renderList();
        return this;
    }
});
Cls.initClass();

module.formatManualRun = function(manual) {
    if (!manual) { return ""; } else { return `\
<span class="label label-manual">
    <i class="icon-hand-down icon-white tt-enable" title="Manual run"></i>
</span>\
`; }
};

const formatInterval = function(interval) {
    const humanized = getDuration(interval).humanize();
    return `\
<span class="tt-enable" title="${interval}">
 ${humanized}
</span>\
`;
};

window.formatScheduler = function(scheduler) {
    const [icon, value] = (() => { switch (scheduler.type) {
        case 'constant': return ['icon-repeatone', 'constant'];
        case 'interval': return ['icon-time', formatInterval(scheduler.value)];
        case 'groc':     return ['icon-calendarthree', scheduler.value];
        case 'daily':    return ['icon-notestasks', scheduler.value];
        case 'cron':     return ['icon-calendaralt-cronjobs', scheduler.value];
    } })();

    return _.template(`\
    <i class="<%= icon %> tt-enable"
        title="<%= type %> scheduler"></i>
<span class="scheduler label label-clear">
    <%= value %>
</span>
<% if (jitter) { %>
    <i class="icon-random tt-enable" title="Jitter<%= jitter %>"></i>
<% } %>\
`)({
         icon,
         type: scheduler.type,
         value,
         jitter: scheduler.jitter});
};


class JobRunListEntryView extends ClickableListEntry {
    constructor(...args) {
        this.initialize = this.initialize.bind(this);
        super(...args);
    }

    static initClass() {

        this.prototype.tagName = "tr";

        this.prototype.className = "clickable";

        this.prototype.template = _.template(`\
<td>
    <a href="#job/<%= job_name %>/<%= run_num %>"><%= run_num %></a>
    <%= modules.job.formatManualRun(manual) %>
</td>
<td><%= formatState(state) %></td>
<td><%= displayNode(node) %></td>
<td><%= dateFromNow(start_time || run_time, "Unknown") %></td>
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
}
JobRunListEntryView.initClass();


Cls = (window.JobRunView = class JobRunView extends Backbone.View {
    constructor(...args) {
        this.initialize = this.initialize.bind(this);
        this.renderList = this.renderList.bind(this);
        this.getMaxDate = this.getMaxDate.bind(this);
        this.renderTimeline = this.renderTimeline.bind(this);
        this.renderGraph = this.renderGraph.bind(this);
        this.sortActionRuns = this.sortActionRuns.bind(this);
        this.render = this.render.bind(this);
        super(...args);
    }

    static initClass() {

        this.prototype.tagName = "div";

        this.prototype.className = "span12";

        this.prototype.template = _.template(`\
 <div class="row">
    <div class="span12">
        <h1>
            <small>Job Run</small>
            <a href="<%= job_url %>">
                <%= formatName(job_name) %></a>.<%= run_num %>
            <span id="filter"</span>
        </h1>

    </div>
    <div class="span5 outline-block">
        <h2>Details</h2>
        <div>
        <table class="table details">
            <tr><td class="span2">State</td>
                <td><%= formatState(state) %></td></tr>
            <tr><td>Node</td>
                <td><%= displayNode(node) %></td></tr>
            <tr><td>Scheduled</td>
                <td>
                    <%= modules.job.formatManualRun(manual) %>
                    <span class="label label-clear"><%= run_time %></span>
                </td></tr>
            <tr><td>Start</td>
                <td><%= dateFromNow(start_time, '') %></td>
            </tr>
            <tr><td>End</td>
                <td><%= dateFromNow(end_time, '') %></td>
            </tr>
        </table>
        </div>
    </div>
    <div class="span7 outline-block">
        <h2>Action Graph</h2>
        <div id="action-graph" class="graph job-view"></div>
    </div>

    <div class="span12 outline-block">
      <h2>Timeline</h2>
      <div>
        <div id="slider-chart"></div>
        <div id="timeline-graph"></div>
      </div>
    </div>

    <div class="span12 outline-block">
        <h2>Action Runs</h2>
        <div>
        <table class="table table-hover table-outline">
            <thead class="sub-header">
                <tr>
                    <th>Name</th>
                    <th>State</th>
                    <th class="span3">Command</th>
                    <th>Node</th>
                    <th>Start</th>
                    <th>End</th>
                </tr>
            </thead>
            <tbody class="actionruns">
            </tbody>
        </table>
        </div>
    </div>
</div>\
`
        );

        this.prototype.popupTemplate = _.template(`\
<div class="top-right-corner"><%= formatState(state) %></div>
<code class="command"><%= command || raw_command %></code>\
`
        );
    }

    initialize(options) {
        this.listenTo(this.model, "change", this.render);
        this.refreshView = new RefreshToggleView({model: this.model.refreshModel});
        return this.listenTo(this.refreshView, 'refreshView', () => this.model.fetch());
    }

    renderList(actionRuns) {
        let model;
        const entry = run => {
            run['job_name'] = this.model.get('job_name');
            run['run_num'] =  this.model.get('run_num');
            model = new modules.actionrun.ActionRun(run);
            return new modules.actionrun.ActionRunListEntryView({model}).render().el;
        };
        return this.$('tbody.actionruns').html((() => {
            const result = [];
            for (model of Array.from(actionRuns)) {                 result.push(entry(model));
            }
            return result;
        })());
    }

    getMaxDate() {
        const actionRuns = this.model.get('runs');
        let dates = (Array.from(actionRuns).map((r) => r.end_time || r.start_time));
        dates = ((() => {
            const result = [];
            for (let date of Array.from(dates)) {                 if (date != null) {
                    result.push(new Date(date));
                }
            }
            return result;
        })());
        dates.push(new Date(this.model.get('run_time')));
        return _.max(dates);
    }

    renderTimeline(actionRuns) {
        const maxDate = this.getMaxDate();
        actionRuns = Array.from(actionRuns).map((actionRun) =>
            new modules.actionrun.ActionRunTimelineEntry(actionRun, maxDate));

        return new modules.timeline.TimelineView({
            model: actionRuns,
            margins: {
                left: 150
            }
        }).render();
    }

    renderGraph() {
        return new GraphView({
            model: this.model.get('action_graph'),
            buildContent: this.popupTemplate,
            nodeClass(d) { return `node ${d.state}`; },
            height: this.$('table.details').height() - 5 // TODO: why -5 to get it flush?
        }).render();
    }

    sortActionRuns() {
        const maxDate = this.getMaxDate();
        const getStart = function(item) {
            if (item.start_time) { return new Date(item.start_time); } else { return maxDate; }
        };
        return _.sortBy(this.model.get('runs'), getStart);
    }

    render() {
        this.$el.html(this.template(this.model.attributes));
        this.$('#filter').html(this.refreshView.render().el);
        const actionRuns = this.sortActionRuns();
        this.renderList(actionRuns);
        this.renderGraph();
        this.renderTimeline(actionRuns);
        makeTooltips(this.$el);
        modules.views.makeHeaderToggle(this.$el);
        return this;
    }
});
Cls.initClass();
