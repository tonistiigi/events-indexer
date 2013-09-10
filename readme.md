WIP

```
var indexer = require('events-indexer')

var db = indexer()

db.set(key, property, value)
db.merge(key, object)
db.get(key)
db.range(start, end)

// listen for changes
var reader = db.subscribe(start, opt_end)
reader.on('data', function() { })
reader.close()

// reduced values

var avgWind = db.reducer('avgWind', function (values) {
  var sum = 0;
  for (var i = 0; i < values.length; i++) {
    sum += i
  }
  return sum / values.length
})

avgWind.set(key, uid, 10)
````