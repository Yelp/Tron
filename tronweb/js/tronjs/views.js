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

// Common view elements
window.modules = window.modules || {};
window.modules.views = module = {}


// Note about subview
// Subviews need to re-delegate events, because they are lost
// when superviews re-render

// Print the date as a string describing the elapsed time
window.dateFromNow = function(string, defaultString) {
    let delta, formatted;
    if (defaultString == null) { defaultString = 'never'; }
    const template = _.template(`\
<span title="<%= formatted %>" class="tt-enable" data-placement="top">
    <%= delta %>
</span>\
`
    );

    const label_template = _.template(`\
<span class="label label-<%= type %>"><%= delta %></span>\
`
    );

    if (string) {
        formatted = moment(string).format('MMM, Do YYYY, h:mm:ss a');
        delta = label_template({
            delta: moment(string).fromNow(),
            type: "clear"
        });
    } else {
        formatted = defaultString;
        delta = label_template({
            delta: defaultString,
            type: "important"
        });
    }
    return template({formatted, delta});
};


window.getDuration = function(time) {
    let ms;
    [time, ms] = Array.from(time.split('.'));
    const [hours, minutes, seconds] = Array.from(time.split(':'));
    return moment.duration({
        hours: parseInt(hours),
        minutes: parseInt(minutes),
        seconds: parseInt(seconds)
    });
};


window.formatDuration = function(duration) {
    const template = _.template(`\
<span class="label label-clear tt-enable" title="<%= duration %>">
  <%= humanized %>
</span>\
`
    );
    const humanize = getDuration(duration).humanize();
    return template({duration, humanized: humanize});
};


// If params match, return "selected". Used for select boxes
window.isSelected = function(current, value) {
    if (current === value) { return "selected"; } else { return ""; }
};

window.makeTooltips = root => root.find('.tt-enable').tooltip();


window.formatName = name => {
    return name.replace(/\./g, '.<wbr/>').replace(/_/g, '_<wbr/>');
};


window.formatState = state => {
    return `<span class="label ${state}">${state}</span>`;
};


window.formatDelay = function(delay) {
    if (delay) {
        return `<small> (retry delayed for ${Math.round(delay)}s)</small>`;
    } else {
        return "";
    }
};

module.makeHeaderToggle = function(root) {
    const headers = root.find('.outline-block h2');
    headers.click(event => $(event.target).nextAll().slideToggle());
    return headers.addClass('clickable');
};


Cls = (window.FilterView = class FilterView extends Backbone.View {
    constructor(...args) {
        {
          // Hack: trick Babel/TypeScript into allowing this before super.
          if (false) { super(); }
          let thisFn = (() => { return this; }).toString();
          let thisName = thisFn.slice(thisFn.indexOf('return') + 6 + 1, thisFn.indexOf(';')).trim();
          eval(`${thisName} = this;`);
        }
        this.getFilterTemplate = this.getFilterTemplate.bind(this);
        this.renderFilters = this.renderFilters.bind(this);
        this.render = this.render.bind(this);
        this.getFilterFromEvent = this.getFilterFromEvent.bind(this);
        this.filterChange = this.filterChange.bind(this);
        this.selectFilterChange = this.selectFilterChange.bind(this);
        super(...args);
    }

    static initClass() {

        this.prototype.tagName = "div";

        this.prototype.className = "";

        this.prototype.defaultIcon = "icon-filter";

        this.prototype.filterIcons = {
            name:       "icon-filter",
            node_pool:  "icon-connected",
            state:      "icon-switchon",
            status:     "icon-switchon"
        };

        this.prototype.filterTemplate = _.template(`\
<div class="input-prepend">
  <input type="text" id="filter-<%= filterName %>"
         value="<%= defaultValue %>"
         class="input-medium"
         autocomplete="off"
         placeholder="<%= _.str.humanize(filterName) %>"
         data-filter-name="<%= filterName %>Filter">
  <i class="<%= icon %> icon-grey"></i>
</div>\
`
        );

        this.prototype.template = _.template(`\
<form class="filter-form">
  <div class="control-group outline-block">
    <div class="controls">
    <div class="span1 toggle-header"
        title="Toggle Filters">Filters</div>
        <%= filters.join('') %>
    </div>
  </div>
</form>\
`
        );

        this.prototype.events = {
            "keyup input":   "filterChange",
            "submit":        "submit",
            "change input":  "filterDone",
            "change select": "selectFilterChange"
        };
    }

    getFilterTemplate(filterName) {
        const createName = `create${filterName}`;
        if (this[createName]) { return this[createName]; } else { return this.filterTemplate; }
    }

    renderFilters() {
        const createFilter = filterName => {
            const template = this.getFilterTemplate(filterName);
            return template({
                defaultValue: this.model.get(`${filterName}Filter`),
                filterName,
                icon: this.filterIcons[filterName] || this.defaultIcon
            });
        };

        const filters = _.map(((() => {
            const result = [];
            for (let k in this.model.filterTypes) {
                result.push(k);
            }
            return result;
        })()), createFilter);
        return this.$el.html(this.template({filters}));
    }

    render() {
        this.renderFilters();
        this.delegateEvents();
        makeTooltips(this.$el);
        return this;
    }

    getFilterFromEvent(event) {
        const filterEle = $(event.target);
        return [filterEle.data('filterName'), filterEle.val()];
    }

    filterChange(event) {
        const [filterName, filterValue] = Array.from(this.getFilterFromEvent(event));
        this.model.set(filterName, filterValue);
        return this.trigger('filter:change', filterName, filterValue);
    }

    filterDone(event) {
        const [filterName, filterValue] = Array.from(this.getFilterFromEvent(event));
        this.trigger('filter:done', filterName, filterValue);
        return window.modules.routes.updateLocationParam(filterName, filterValue);
    }

    selectFilterChange(event) {
        this.filterChange(event);
        return this.filterDone(event);
    }

    submit(event) {
        return event.preventDefault();
    }
});
Cls.initClass();


Cls = (window.RefreshToggleView = class RefreshToggleView extends Backbone.View {
    constructor(...args) {
        {
          // Hack: trick Babel/TypeScript into allowing this before super.
          if (false) { super(); }
          let thisFn = (() => { return this; }).toString();
          let thisName = thisFn.slice(thisFn.indexOf('return') + 6 + 1, thisFn.indexOf(';')).trim();
          eval(`${thisName} = this;`);
        }
        this.render = this.render.bind(this);
        this.toggle = this.toggle.bind(this);
        this.triggerRefresh = this.triggerRefresh.bind(this);
        super(...args);
    }

    static initClass() {

        this.prototype.tagName = "div";

        this.prototype.className = "refresh-view pull-right";

        this.prototype.attributes = {
            "type":             "button",
            "data-toggle":      "button"
        };

        this.prototype.template = _.template(`\
<span class="muted"><%= text %></span>
<button class="btn btn-clear tt-enable <%= active %>"
    title="Toggle Refresh"
    data-placement="top">
    <i class="icon-refresh icon-white"></i>
</button>\
`
        );

        this.prototype.events =
            {"click button":        "toggle"};
    }

    initialize() {
        this.listenTo(mainView, 'closeView', this.model.disableRefresh);
        return this.listenTo(this.model, 'refresh', this.triggerRefresh);
    }

    render() {
        let active, text;
        if (this.model.enabled) {
            text = `Refresh ${ this.model.interval / 1000 }s`;
            active = "active";
        } else {
            text = (active = "");
        }
        this.$el.html(this.template({text, active}));
        // See note about subview
        this.delegateEvents();
        makeTooltips(this.$el);
        return this;
    }

    toggle(event) {
        this.model.toggle(event);
        return this.render();
    }

    triggerRefresh() {
        return this.trigger('refreshView');
    }
});
Cls.initClass();


Cls = (window.ClickableListEntry = class ClickableListEntry extends Backbone.View {
    constructor(...args) {
        {
          // Hack: trick Babel/TypeScript into allowing this before super.
          if (false) { super(); }
          let thisFn = (() => { return this; }).toString();
          let thisName = thisFn.slice(thisFn.indexOf('return') + 6 + 1, thisFn.indexOf(';')).trim();
          eval(`${thisName} = this;`);
        }
        this.propogateClick = this.propogateClick.bind(this);
        super(...args);
    }

    static initClass() {

        this.prototype.events =
            {"click":    "propogateClick"};
    }
    // A ciew for an element in a list that is clickable

    className() {
        return "clickable";
    }

    propogateClick(event) {
        if (event.button === 0) {
            return document.location = this.$('a').first().attr('href');
        }
    }
});
Cls.initClass();


module.makeSlider = (root, options) => root.find('.slider-bar').slider(options);


Cls = (module.SliderView = class SliderView extends Backbone.View {
    constructor(...args) {
        {
          // Hack: trick Babel/TypeScript into allowing this before super.
          if (false) { super(); }
          let thisFn = (() => { return this; }).toString();
          let thisName = thisFn.slice(thisFn.indexOf('return') + 6 + 1, thisFn.indexOf(';')).trim();
          eval(`${thisName} = this;`);
        }
        this.handleSliderMove = this.handleSliderMove.bind(this);
        this.updateDisplayCount = this.updateDisplayCount.bind(this);
        super(...args);
    }

    static initClass() {

        this.prototype.tagName = "div";

        this.prototype.className = "list-controls controls-row";

        this.prototype.template = `\
<div class="span1">
  <span id="display-count" class="label label-inverse"></span>
</div>
<div class="slider-bar span10"></div>\
`;
    }

    initialize(options) {
        options = options || {};
        return this.displayCount = options.displayCount || 10;
    }

    handleSliderMove(event, ui) {
        this.updateDisplayCount(ui.value);
        return this.trigger('slider:change', ui.value);
    }

    updateDisplayCount(count) {
        this.displayCount = count;
        const content = `${count} / ${this.model.length()}`;
        return this.$('#display-count').html(content);
    }

    render() {
        this.$el.html(this.template);
        this.updateDisplayCount(_.min([this.model.length(), this.displayCount]));
        module.makeSlider(this.$el, {
            max: this.model.length(),
            min: 0,
            range: 'min',
            value: this.displayCount,
            slide: this.handleSliderMove
        }
        );
        return this;
    }
});
Cls.initClass();
