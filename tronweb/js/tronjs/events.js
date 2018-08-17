/*
 * decaffeinate suggestions:
 * DS102: Remove unnecessary code created because of implicit returns
 * DS206: Consider reworking classes to avoid initClass
 * Full docs: https://github.com/decaffeinate/decaffeinate/blob/master/docs/suggestions.md
 */

// Events

window.TronEvent = class TronEvent extends Backbone.Model {};



Cls = (window.MinimalEventListEntryView = class MinimalEventListEntryView extends Backbone.View {
    static initClass() {

        this.prototype.tagName = "tr";

        this.prototype.template = _.template(`\
<td><%= dateFromNow(time) %></td>
<td>
  <span class="label <%= level %>">
    <%= name %>
  </span>
</td>\
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
