/*
 * decaffeinate suggestions:
 * DS001: Remove Babel/TypeScript constructor workaround
 * DS101: Remove unnecessary use of Array.from
 * DS102: Remove unnecessary code created because of implicit returns
 * DS205: Consider reworking code to avoid use of IIFEs
 * DS206: Consider reworking classes to avoid initClass
 * Full docs: https://github.com/decaffeinate/decaffeinate/blob/master/docs/suggestions.md
 */

// Generic models
window.modules = window.modules || {};
let module = (window.modules.models = {});


const backboneSync = Backbone.sync;

Backbone.sync = function(method, model, options) {
    options.url = `/api${_.result(model, 'url')}`;
    return backboneSync(method, model, options);
};


window.RefreshModel = class RefreshModel extends Backbone.Model {

    constructor(...args) {
        {
          // Hack: trick Babel/TypeScript into allowing this before super.
          if (false) { super(); }
          let thisFn = (() => { return this; }).toString();
          let thisName = thisFn.slice(thisFn.indexOf('return') + 6 + 1, thisFn.indexOf(';')).trim();
          eval(`${thisName} = this;`);
        }
        this.initialize = this.initialize.bind(this);
        this.toggle = this.toggle.bind(this);
        this.enableRefresh = this.enableRefresh.bind(this);
        this.disableRefresh = this.disableRefresh.bind(this);
        this.clear = this.clear.bind(this);
        this.doRefresh = this.doRefresh.bind(this);
        this.scheduleRefresh = this.scheduleRefresh.bind(this);
        super(...args);
    }

    initialize(options) {
        options = options || {};
        this.interval = (options.interval || 5) * 1000;
        this.enabled = false;
        return this.timeout = null;
    }

    toggle(event) {
        if (!this.enabled) {
            this.enableRefresh();
            return this.trigger('toggle:on');
        } else {
            this.disableRefresh();
            return this.trigger('toggle:off');
        }
    }

    enableRefresh() {
        if (!this.enabled) {
            console.log("Enabling refresh");
            this.enabled = true;
            return this.scheduleRefresh();
        }
    }

    disableRefresh() {
        console.log("Disableing refresh ");
        this.enabled = false;
        return this.clear();
    }

    clear() {
        clearTimeout(this.timeout);
        return this.timeout = null;
    }

    doRefresh() {
        this.clear();
        if (this.enabled) {
            console.log("trigger refresh event");
            this.trigger('refresh');
            return this.scheduleRefresh();
        }
    }

    scheduleRefresh() {
        if (!this.timeout) {
            console.log(`scheduled with ${this.interval}`);
            return this.timeout = setTimeout(this.doRefresh, this.interval);
        }
    }
};


window.matchAny = (item, query) => ~item.toLowerCase().indexOf(query.toLowerCase());

window.buildMatcher = (getter, matcher) => (item, query) => matcher(getter(item), query);

window.fieldGetter = name => item => item.get(name);

window.nestedName = field => item => item.get(field)['name'];


let Cls = (window.FilterModel = class FilterModel extends Backbone.Model {
    constructor(...args) {
        {
          // Hack: trick Babel/TypeScript into allowing this before super.
          if (false) { super(); }
          let thisFn = (() => { return this; }).toString();
          let thisName = thisFn.slice(thisFn.indexOf('return') + 6 + 1, thisFn.indexOf(';')).trim();
          eval(`${thisName} = this;`);
        }
        this.createFilter = this.createFilter.bind(this);
        super(...args);
    }

    static initClass() {

        this.prototype.filterTypes = {
            name:       buildMatcher(fieldGetter('name'), matchAny),
            state:      buildMatcher(fieldGetter('state'), _.str.startsWith),
            node_pool:  buildMatcher(nestedName('node_pool'), _.str.startsWith)
        };
    }

    createFilter() {
        let func, item;
        const filterFuncs = (() => {
            const result = [];
            for (let type in this.filterTypes) {
                func = this.filterTypes[type];
                result.push(((type, func) => {
                    const query = this.get(`${type}Filter`);
                    if (query) {
                        return item => func(item, query);
                    } else {
                        return item => true;
                    }
                })(type, func));
            }
            return result;
        })();

        return item => _.every(filterFuncs, func => func(item));
    }
});
Cls.initClass();


class IndexEntry {

    constructor(name) {
        this.toLowerCase = this.toLowerCase.bind(this);
        this.replace = this.replace.bind(this);
        this.indexOf = this.indexOf.bind(this);
        this.toString = this.toString.bind(this);
        this.name = name;
    }

    toLowerCase() {
        return this.name.toLowerCase();
    }

    replace(...args) {
        return this.name.replace(...Array.from(args || []));
    }

    indexOf(...args) {
        return this.name.indexOf(...Array.from(args || []));
    }

    toString() {
       return `${this.type} ${this.name}`;
   }
}


class JobIndexEntry extends IndexEntry {
    constructor(...args) {
        {
          // Hack: trick Babel/TypeScript into allowing this before super.
          if (false) { super(); }
          let thisFn = (() => { return this; }).toString();
          let thisName = thisFn.slice(thisFn.indexOf('return') + 6 + 1, thisFn.indexOf(';')).trim();
          eval(`${thisName} = this;`);
        }
        this.getUrl = this.getUrl.bind(this);
        super(...args);
    }

    static initClass() {

        this.prototype.type = "Job";
    }

    getUrl() {
        return `#job/${this.name}`;
    }
}
JobIndexEntry.initClass();

class ConfigIndexEntry extends IndexEntry {
    constructor(...args) {
        {
          // Hack: trick Babel/TypeScript into allowing this before super.
          if (false) { super(); }
          let thisFn = (() => { return this; }).toString();
          let thisName = thisFn.slice(thisFn.indexOf('return') + 6 + 1, thisFn.indexOf(';')).trim();
          eval(`${thisName} = this;`);
        }
        this.getUrl = this.getUrl.bind(this);
        super(...args);
    }

    static initClass() {

        this.prototype.type = "Config";
    }

    getUrl() {
        return `#config/${this.name}`;
    }
}
ConfigIndexEntry.initClass();

class CommandIndexEntry extends IndexEntry {
    static initClass() {

        this.prototype.type = "command";
    }

    constructor(name, job_name, action_name) {
        {
          // Hack: trick Babel/TypeScript into allowing this before super.
          if (false) { super(); }
          let thisFn = (() => { return this; }).toString();
          let thisName = thisFn.slice(thisFn.indexOf('return') + 6 + 1, thisFn.indexOf(';')).trim();
          eval(`${thisName} = this;`);
        }
        this.getUrl = this.getUrl.bind(this);
        this.name = name;
        this.job_name = job_name;
        this.action_name = action_name;
        this.name = name;
    }

    getUrl() {
        return `#job/${this.job_name}/-1/${this.action_name}`;
    }
}
CommandIndexEntry.initClass();


Cls = (module.QuickFindModel = class QuickFindModel extends Backbone.Model {
    constructor(...args) {
        {
          // Hack: trick Babel/TypeScript into allowing this before super.
          if (false) { super(); }
          let thisFn = (() => { return this; }).toString();
          let thisName = thisFn.slice(thisFn.indexOf('return') + 6 + 1, thisFn.indexOf(';')).trim();
          eval(`${thisName} = this;`);
        }
        this.getJobEntries = this.getJobEntries.bind(this);
        this.parse = this.parse.bind(this);
        super(...args);
    }

    static initClass() {

        this.prototype.url = "/";
    }

    getJobEntries(jobs) {
        let name, actions;
        const buildActions = actions =>
            Array.from(actions).map((action) =>
                new CommandIndexEntry(action.command, name, action.name))
        ;

        const nested = (() => {
            const result = [];
            for (name in jobs) {
                actions = jobs[name];
                result.push([new JobIndexEntry(name), buildActions(actions)]);
            }
            return result;
        })();
        return _.flatten(nested);
    }

    parse(resp, options) {
        let name;
        const index = [].concat(
            this.getJobEntries(resp['jobs']),
            (() => {
            const result = [];
            for (name of Array.from(resp['namespaces'])) {                 result.push(new ConfigIndexEntry(name));
            }
            return result;
        })());

        return _.mash(Array.from(index).map((entry) => [entry.name, entry]));
    }
});
Cls.initClass();
