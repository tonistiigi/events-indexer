var Tree = require('btreejs').create()
var bytewise = require('bytewise')
var assert = require('assert')
var Stream = require('stream').Stream
var inherits = require('util').inherits


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

function Subscription(indexer, start, end) {
  Stream.call(this)
  this.start = bytewise.encode(start).toString('binary')
  this.end = bytewise.encode(end).toString('binary')
  this.throttle = indexer.throttle || 0
  this.queue = new Tree
  this.indexer = indexer
  this.tm = 0
  this.processQueue_ = Subscription.prototype.processQueue_.bind(this)

  this.indexer.log({start: this.start, end: this.end}, 'subscribe')
}
inherits(Subscription, Stream)

Subscription.prototype.close = function() {
  this.emit('end')
  this.emit('close')

  this.indexer.log({start: this.start, end: this.end}, 'unsubscribe')
}

Subscription.prototype.processQueue_ = function() {
  var result = []
  this.queue.walk(function(key, v) {
    result.push({k: bytewise.decode(key), v: v.getData()})
  })
  this.emit('data', result)
  this.queue = new Tree
  this.tm = 0
}

Subscription.prototype.invalidate_ = function(k, v) {
  if (!this.throttle) {
    return this.emit('data', [{k: k, v: v.getData()}])
  }

  this.queue.put(bytewise.encode(k), v)

  if (!this.tm) {
    this.tm = setTimeout(this.processQueue_, this.throttle)
  }
}

function Indexer(opt) {
  opt = opt || {}
  this.tree = new Tree()
  this.throttle = opt.throttle || 0
  this.listeners_ = []
  this.log = opt.log  || function() {}
}

Indexer.prototype.set = function (key, property, value) {
  assert(key, key + ' is not a valid key')
  assert.ok(property, 'invalid property')

  var enckey = bytewise.encode(key)
  var current = this.tree.get(enckey)

  if (current === undefined) {
    current = new Data(key)
    this.tree.put(enckey, current)
  }

  if (typeof property === 'string') {
    if (current.data[property] !== value) {
      current.data[property] = value
      this.dispatch_(key, current)
    }
  }
  else {
    var changed = false
    var object = property
    for (var i in object) {
      if (current.data[i] !== object[i]) {
        current.data[i] = object[i]
        changed = true
      }
    }
    if (changed) {
      this.dispatch_(key, current)
    }
  }

}

Indexer.prototype.get = function (key) {
  assert(key, key + ' is not a valid key')

  var data = this.tree.get(bytewise.encode(key))
  return data && data.getData()
}

Indexer.prototype.getRange = function (start, end, options) {
  assert.ok(start || start === undefined)
  assert.ok(end || end === undefined)

  options = options || {}
  var order = options.order || 'asc'
  var limit = options.limit || -1

  if (!start) {
    start = ''
  }
  if (end === undefined) {
    if (start === '') {
      end = ['\u9999']
    }
    else if (Array.isArray(start)) {
      end = start.concat('\u9999')
    }
    else {
      end = start + '\u9999'
    }
  }
  var result = []
  var i = 0
  var walk = order === 'desc' ? 'walkDesc' : 'walkAsc'
  this.tree[walk](bytewise.encode(start), bytewise.encode(end), function(key, v) {
    if (limit !== -1 && ++i > limit) return true
    result.push({k: bytewise.decode(key), v: v.getData()})
  })
  return result
}

Indexer.prototype.subscribe = function (start, end) {
  assert.ok(start || start === undefined)
  assert.ok(end || end === undefined)

  if (!start) {
    start = ''
  }
  if (end === undefined) {
    if (start === '') {
      end = ['\u9999']
    }
    else if (Array.isArray(start)) {
      end = start.concat('\u9999')
    }
    else {
      end = start + '\u9999'
    }
  }
  var s = new Subscription(this, start, end)
  this.listeners_.push(s)
  var self = this
  s.once('close', function() {
    self.listeners_.splice(self.listeners_.indexOf(s), 1)
  })
  return s
}

Indexer.prototype.dispatch_ = function (k, v) {
  var keyenc = bytewise.encode(k).toString('binary')
  // r-tree?
  for (var i = 0; i < this.listeners_.length; i++) {
    var s = this.listeners_[i]
    if (s.start <= keyenc && s.end >= keyenc) {
      s.invalidate_(k, v)
    }
  }
}

Indexer.prototype.createReducedField = function (property, func) {
  assert.ok(typeof property === 'string')
  assert.ok(typeof func === 'function')

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
  var rkey
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
  this.indexer.dispatch_(key, current)
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
  // add this back when with del() support
  // if (current === undefined) {
  //   current = new Data
  //   this.indexer.tree.put(keyenc, current)
  // }
  current.data[this.property] = this.func(values)
}


module.exports = exports = function(opt) {
  return new Indexer(opt)
}

exports.Indexer = Indexer
exports.Reducer = Reducer