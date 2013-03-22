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
})
