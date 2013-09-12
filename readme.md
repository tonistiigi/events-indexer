WIP

```
var indexer = require('events-indexer')

var db = indexer({throttle: 100}) // throttle queues up subscribe events(ms)

db.set(key, property, value)
db.set(key, object) // merges values
db.get(key)
db.getRange(start, opt_end) // if no end then use start*

// listen for changes
var reader = db.subscribe(start, opt_end)
reader.on('data', function() { })
reader.close()

// reduced values

var avgWind = db.createReducedField('avgWind', function (values) {
  var sum = 0;
  for (var i = 0; i < values.length; i++) {
    sum += i
  }
  return sum / values.length
})

avgWind.set(key, uid, 10) // uid defines order + overwrite cases
````