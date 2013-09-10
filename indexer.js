var Tree = require('btreejs').create()
var bytewise = require('bytewise')
var assert = require('assert')



function Data(key) {
  this.key_ = key
  this.data = {}
  this.reducers_to_run_ = []
}

Data.prototype.getData = function() {
  if (this.reducers_to_run_.length) {
    this.processReducers()
  }
  return this.data
}

Data.prototype.invalidateReducer = function(r) {
  if (-1 === this.reducers_to_run_.indexOf(r)) {
    this.reducers_to_run_.push(r)
  }
}

Data.prototype.processReducers = function() {
  for (var i = 0; i < this.reducers_to_run_.length; i++) {
    this.reducers_to_run_[i].run(this.key_)
  }
  this.reducers_to_run_ = []
}

function Indexer() {
  this.tree = new Tree()
}

Indexer.prototype.set = function (key, property, value) {
  assert(key, key + ' is not a valid key')
  assert.equal(typeof property, 'string', 'property needs to be a string')

  var enckey = bytewise.encode(key)
  var current = this.tree.get(enckey)

  if (current === undefined) {
    current = new Data(key)
    this.tree.put(enckey, current)
  }
  current.data[property] = value
}

Indexer.prototype.merge = function (key, object) {
  assert(key, key + ' is not a valid key')
  assert.equal(typeof object, 'object')

  var enckey = bytewise.encode(key)
  var current = this.tree.get(enckey)

  if (current === undefined) {
    current = new Data(key)
    this.tree.put(enckey, current)
  }
  for (var i in object) {
    current.data[i] = object[i]
  }
}

Indexer.prototype.get = function (key) {
  assert(key, key + ' is not a valid key')
  var data = this.tree.get(bytewise.encode(key))
  return data && data.getData()
}

Indexer.prototype.range = function (start, end) {
  if (!start) {
    start = ''
  }
  if (end === undefined) {
    if (start === '') {
      end = ['\u9999']
    }
    else {
      end = start
    }
  }
  var result = []
  this.tree.walk(bytewise.encode(start), bytewise.encode(end), function(key, v) {
    result.push({k: bytewise.decode(key), v: v.getData()})
  })
  return result
}

Indexer.prototype.subscribe = function (start, end) {

}

Indexer.prototype.reducer = function (property, func) {
  return new Reducer(this, property, func)
}

function Reducer(indexer, property, func) {
  assert(indexer instanceof Indexer)
  assert.equal(typeof property, 'string')
  assert.equal(typeof func, 'function')

  this.indexer = indexer
  this.property = property
  this.func = func
  this.tree = new Tree
}

Reducer.prototype.set = function (key, id, value) {
  var rkey;
  if (Array.isArray(key)) {
    rkey = key.concat(id)
  }
  else {
    rkey = [key, id]
  }
  this.tree.put(bytewise.encode(rkey), value)
  var keyenc = bytewise.encode(key)
  var current = this.indexer.tree.get(keyenc)
  if (current === undefined) {
    current = new Data(key)
    this.indexer.tree.put(keyenc, current)
  }
  current.invalidateReducer(this)
}

Reducer.prototype.run = function(key) {
  var lastkey, firstkey
  if (Array.isArray(key)) {
    lastkey = key.concat('\u9999')
    firstkey = key
  }
  else {
    firstkey = [key]
    lastkey = [key, '\u9999']
  }

  var values = []
  this.tree.walk(bytewise.encode(firstkey), bytewise.encode(lastkey),
    function(k, v) {
      values.push(v)
    }
  )

  var keyenc = bytewise.encode(key)
  var current = this.indexer.tree.get(keyenc)
  if (current === undefined) {
    current = new Data
    this.indexer.tree.put(keyenc, current)
  }
  current.data[this.property] = this.func(values)
}


module.exports = exports = function(opt) {
  return new Indexer(opt)
}

exports.Indexer = Indexer
exports.Reducer = Reducer