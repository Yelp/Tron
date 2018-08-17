/*
 * decaffeinate suggestions:
 * DS001: Remove Babel/TypeScript constructor workaround
 * DS101: Remove unnecessary use of Array.from
 * DS102: Remove unnecessary code created because of implicit returns
 * DS206: Consider reworking classes to avoid initClass
 * Full docs: https://github.com/decaffeinate/decaffeinate/blob/master/docs/suggestions.md
 */

// Routes
window.modules = window.modules || {}
module = window.modules.routes = {}

Cls = (module.TronRoutes = class TronRoutes extends Backbone.Router {
    static initClass() {

        this.prototype.routes = {
            "":                         "index",
            "home(;*params)":           "home",
            "dashboard(;*params)":      "dashboard",
            "jobs(;*params)":           "jobs",
            "job/:name":                "job",
            "job/:job_name/:run_num":   "jobrun",
            "job/:name/:run/:action":   "actionrun",
            "configs":                  "configs",
            "config/:name":             "config"
        };
    }

    updateMainView(model, viewType) {
        const view = new viewType({model});
        model.fetch();
        return mainView.updateMain(view);
    }

    index() {
        return this.navigate('home', {trigger: true});
    }

    home(params) {
        const model = new Dashboard({
            filterModel: new DashboardFilterModel(module.getParamsMap(params))});
        return this.updateMainView(model, DashboardView);
    }

    dashboard(params) {
        mainView.close();
        const model = new Dashboard({
            filterModel: new DashboardFilterModel(module.getParamsMap(params))});
        const dashboard = new DashboardView({model});
        model.fetch();
        return mainView.updateFullView(dashboard.render());
    }

    configs() {
        return this.updateMainView(new NamespaceList(), NamespaceListView);
    }

    config(name) {
        return this.updateMainView(new Config({name}), ConfigView);
    }

    jobs(params) {
        const collection = new JobCollection([], {
            refreshModel: new RefreshModel(),
            filterModel: new JobListFilterModel(module.getParamsMap(params))
        });
        return this.updateMainView(collection, JobListView);
    }

    job(name) {
        const refreshModel = new RefreshModel();
        return this.updateMainView(new Job({name, refreshModel}), JobView);
    }

    jobrun(name, run) {
        const model = new JobRun({
            name, run_num: run, refreshModel: new RefreshModel()});
        return this.updateMainView(model, JobRunView);
    }

    actionrun(name, run, action) {
        const model = new modules.actionrun.ActionRun({
            job_name: name,
            run_num: run,
            action_name: action,
            refreshModel: new RefreshModel()});
        const historyCollection = new modules.actionrun.ActionRunHistory([], {
            job_name: name,
            action_name: action
        });
        const view = new modules.actionrun.ActionRunView({
            model,
            history: historyCollection});
        model.fetch();
        historyCollection.fetch();
        return mainView.updateMain(view);
    }
});
Cls.initClass();


class MainView extends Backbone.View {
    constructor(...args) {
        {
          // Hack: trick Babel/TypeScript into allowing this before super.
          if (false) { super(); }
          let thisFn = (() => { return this; }).toString();
          let thisName = thisFn.slice(thisFn.indexOf('return') + 6 + 1, thisFn.indexOf(';')).trim();
          eval(`${thisName} = this;`);
        }
        this.updateMain = this.updateMain.bind(this);
        this.updateFullView = this.updateFullView.bind(this);
        this.render = this.render.bind(this);
        this.renderNav = this.renderNav.bind(this);
        this.close = this.close.bind(this);
        super(...args);
    }

    static initClass() {

        this.prototype.el = $("#all-view");

        this.prototype.template = `\
<div id="nav"></div>
<div class="container">
    <div id="main" class="row">
    </div>
</div>\
`;
    }

    initialize(options) {
       return this.navView = new modules.navbar.NavView({model: this.model});
   }

    updateMain(view) {
        this.close();
        if (this.$('#nav').html() === '') { this.renderNav(); }
        this.navView.setActive();
        return this.$('#main').html(view.el);
    }

    updateFullView(view) {
        this.$('#nav').html('');
        return this.$('#main').html(view.el);
    }

    render() {
        this.$el.html(this.template);
        this.renderNav();
        return this;
    }

    renderNav() {
        console.log('rendering nav');
        return this.$('#nav').html(this.navView.render().el);
    }

    close() {
        return this.trigger('closeView');
    }
}
MainView.initClass();


module.splitKeyValuePairs = pairs => _.mash(Array.from(pairs).map((param) => param.split('=')));

module.getParamsMap = function(paramString) {
    paramString = paramString || "";
    return module.splitKeyValuePairs(paramString.split(';'));
};

module.getLocationHash = () => document.location.hash;

module.getLocationParams = function() {
    const parts = module.getLocationHash().split(';');
    return [parts[0], module.splitKeyValuePairs(parts.slice(1))];
};


module.buildLocationString = function(base, params) {
    params = (Array.from(_.pairs(params)).filter((pair) => pair[1]).map((pair) => pair.join('='))).join(';');
    return `${ base };${ params }`;
};


module.updateLocationParam = function(name, value) {
    const [base, params] = Array.from(module.getLocationParams());
    params[name] = value;
    return routes.navigate(module.buildLocationString(base, params));
};


window.attachRouter = () =>
    $(document).ready(function() {

        window.routes = new modules.routes.TronRoutes();
        const model = (modules.models = new modules.models.QuickFindModel());
        window.mainView = new MainView({model}).render();
        model.fetch();
        return Backbone.history.start({root: "/web/"});
    })
;
