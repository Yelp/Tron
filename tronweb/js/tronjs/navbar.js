/*
 * decaffeinate suggestions:
 * DS101: Remove unnecessary use of Array.from
 * DS102: Remove unnecessary code created because of implicit returns
 * DS205: Consider reworking code to avoid use of IIFEs
 * DS206: Consider reworking classes to avoid initClass
 * Full docs: https://github.com/decaffeinate/decaffeinate/blob/master/docs/suggestions.md
 */


//window.modules = window.modules || {}
//module = window.modules.navbar = {}


Cls = (module.NavView = class NavView extends Backbone.View {
    constructor(...args) {
        this.render = this.render.bind(this);
        this.updater = this.updater.bind(this);
        this.source = this.source.bind(this);
        this.highlighter = this.highlighter.bind(this);
        this.renderTypeahead = this.renderTypeahead.bind(this);
        this.setActive = this.setActive.bind(this);
        super(...args);
    }

    static initClass() {

        this.prototype.tagName = "div";

        this.prototype.className = "navbar navbar-static-top";

        this.prototype.attributes =
            {id: "menu"};

        this.prototype.events =
            {".search-query click":  "handleClick"};

        this.prototype.template = `\
<div class="navbar-inner">
  <div class="container">
  <ul class="nav">
    <li class="brand">tron<span>web</span></li>
    <li><a href="#home">
      <i class="icon-th"></i>Dashboard</a>
    </li>
    <li><a href="#jobs">
      <i class="icon-time"></i>Scheduled Jobs</a>
    </li>
    <li><a href="#configs">
      <i class="icon-wrench"></i>Config</a>
    </li>
  </ul>

  <form class="navbar-search pull-right">
  </form>

  </div>
</div>\
`;

        this.prototype.typeaheadTemplate = `\
<input type="text" class="input-medium search-query typeahead"
    placeholder="Search"
    autocomplete="off"
    data-provide="typeahead">
<div class="icon-search"></div>\
`;
    }

    initialize(options) {}

    handleClick(event) {
       return console.log(event);
   }

    render() {
        this.$el.html(this.template);
        this.renderTypeahead();
        return this;
    }

    updater(item) {
        const entry = this.model.get(item);
        routes.navigate(entry.getUrl(), {trigger: true});
        return entry.name;
    }

    source(query, process) {
        return ((() => {
            const result = [];
            for (let _ in this.model.attributes) {
                const entry = this.model.attributes[_];
                result.push(entry.name);
            }
            return result;
        })());
    }

    highlighter(item) {
        // Also formats the item for display
        const { typeahead } = this.$('.typeahead').data();
        const name = module.typeahead_hl.call(typeahead, item);
        const entry = this.model.get(item);
        return `<small>${entry.type}</small> ${name}`;
    }

    sorter(items) {
        const [startsWithQuery, containsQuery] = [[], []];
        const query = this.query.toLowerCase();
        for (let item of Array.from(items)) {
            const uncasedItem = item.toLowerCase();
            if (_.str.startsWith(uncasedItem, query)) { startsWithQuery.push(item);
            } else if (_.str.include(uncasedItem, query)) { containsQuery.push(item); }
        }

        const lengthSort = item => item.length;
        return _.sortBy(startsWithQuery, lengthSort)
            .concat(_.sortBy(containsQuery, lengthSort));
    }

    renderTypeahead() {
        this.$('.navbar-search').html(this.typeaheadTemplate);
        this.$('.typeahead').typeahead({
            source: this.source,
            updater: this.updater,
            highlighter: this.highlighter,
            sorter: this.sorter
        });
        return this;
    }

    setActive() {
        this.$('li').removeClass('active');
        let [path, params] = modules.routes.getLocationParams();
        path = path.split('/')[0];
        return this.$(`a[href=${path}]`).parent('li').addClass('active');
    }
});
Cls.initClass();

const Typeahead = $.fn.typeahead.Constructor.prototype;

Typeahead.show = function() {
    const top = this.$element.position().top + this.$element[0].offsetHeight + 1;
    this.$menu.insertAfter(this.$element).css({top}).show();
    this.shown = true;
    return this;
};

module.typeahead_hl = $.fn.typeahead.Constructor.prototype.highlighter;
