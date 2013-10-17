[![Build Status](https://secure.travis-ci.org/tonistiigi/events-indexer.png)](http://travis-ci.org/tonistiigi/events-indexer)

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

var park = db.get(['fp', country, parkId])
park.set({p: 100})
park.set({wind: indexer.reduced(tid, 100) })

db.has(key)
db.del(key)

var reader = db.subscribe(start, opt_end)
reader.on('add', function(key, value) { })
reader.on('update', function(key, value) { })
reader.on('delete', function(key, value) { })
reader.close()

reader.throttle(.2).on('data', function(list) {
})

var park = db.define(['fp', ':country', ':parkId'])
park.on('create', function(p, params) {
})
park.on('change', function(p) {
})

park.reduce('wind', function(values) {
  return sum(values)
})

// static key only
park.map(['fp_allparks', ':parkId'])

// static key + format
park.map(['fp_allparks', ':parkId'], ['name'])

// dynamic key only
park.map(function() {
  if (this.wind > 100) return ['highwind', this.parkId]
}, ['name'])




