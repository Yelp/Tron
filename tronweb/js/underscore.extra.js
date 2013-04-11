_.mixin({

  /* take elements from list while callback condition is met */
  takeWhile: function(list, callback, context) {
    var xs = [];
    _.any(list, function(item, index, list) {
      var res = callback.call(context, item, index, list);
      if (res) {
        xs.push(item);
        return false;
      } else {
        return true;
      }
    });
    return xs;
  },

  /* Build an object with [key, value] from pair list or callback */
  mash: function(list, callback, context) {
    var pair_callback = callback || _.identity;
    return _.reduce(list, function(obj, value, index, list) {
      var pair = pair_callback.call(context, value, index, list);
      if (typeof pair == "object" && pair.length == 2) {
        obj[pair[0]] = pair[1];
      }
      return obj;
    }, {});
  },

  /* Return pairs [key, value] of object */
  pairs: function(object) {
    return _.map(object, function(value, key) {
      return [key, value];
    });
  },

})
