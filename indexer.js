var Tree = require('btreejs').create()
var bytewise = require('bytewise')
var assert = require('assert')
var Stream = require('stream').Stream
var inherits = require('util').inherits
var EventEmitter = require('events').EventEmitter
var deepEqual = require('deep-equal')

function MapResult(key, data, link) {
  this.key = key
  this.data = null
  this.status = 0
  this.link = null
}

function MapDefinition(key, format) {
  this.key = key
  this.format = format
}

MapDefinition.prototype.run = function(data) {
  var key = this.key.map(identity), f = this.format, res = null
  if (!key) {
    key = this.format(data.data)
    if (typeof key[key.length - 1] === 'function') {
      f = key.pop()
    }
  }
  if (key) {
    for (var i = 0; i < key.length; i++) {
      if (key[i][0] === ':') {
        key[i] = data.data[key[i].substr(1)]
      }
    }
    res = new MapResult(bytewise.encode(key).toString('binary'),
      f(data.data), data)
  }
  return res
}


function identity(a) {
  return a
}

function Data(def, key, init) {
  this.def_ = def
  this.inTree_ = 0
  this.key_ = key
  this.data = init || {}
  this.reducers_to_run_ = []
  this.map_ = []
  this.def_.emit('create', this)
}

Data.prototype.set = function(data) {
  assert.ok(data, 'invalid object')

  var changed = false
  for (var i in data) {
    if (this.data[i] !== data[i]) {
      this.data[i] = data[i]
      changed = true
    }
  }
  if (changed) {
    this.def_.emit('change', this)
    for (i = 0; i < this.def_.map_.length; i++) {
      var m = this.def_.map_[i].run(this)

      var exists = false
      for (var j = 0; j < this.map_.length; j++) {
        if (this.map_[j].key === m.key) {
          if (!deepEqual(this.map_[j].data, m.data)) {
            this.map_[j] = m
            this.def_.indexer.dispatch_('update', m.key, m)
          }
          this.map_[j].status = 0
          exists = true
          break
        }
      }

      if (!exists) {
        this.map_.push(m)
        this.def_.indexer.dispatch_('add', m.key, m)
      }

    }
    for (var j = 0; j < this.map_.length; j++) {
      if (this.map_[j].status === 1) {
        this.map_.splice(j--, 1)
        this.def_.indexer.dispatch_('delete', this.map_[j].key, m)
      }
    }
  }
  return this
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
Data.prototype.del = function() {
  // todo
  assert.ok(false)
}


function Definition(indexer, key) {
  EventEmitter.call(this)
  this.indexer = indexer
  this.key = key
  this.map_ = []
  this.reduce_ = {}
}
inherits(Definition, EventEmitter)

Definition.prototype.match = function(key) {
  if (key.length === this.key.length) {
    for (var i = 0; i < key.length; i++) {
      if (this.key[i][0] != ':' && this.key[i] !== key[i]) {
        return false
      }
    }
    return true
  }
  return false
}

Definition.prototype.getOrCreate = function(key) {
  // todo: throw if no match
  var params = {}
  for (var i = 0; i < this.key.length; i++) {
    if (this.key[i][0] == ':') {
      params[this.key[i].substr(1)] = key[i]
    }
  }
  var d = this.indexer.tree.get(bytewise.encode(key))
  if (!d) {
    d = new Data(this, key, params)
    this.indexer.tree.put(bytewise.encode(key), d)
  }
  return d
}

Definition.prototype.map = function(key, format) {
  if (typeof key === 'function') {
    format = key
    key = null
  }
  this.map_.push(new MapDefinition(key, format || identity))
}

Definition.prototype.reduce = function(name, func) {
  this.reduce_[name] = func
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

Subscription.prototype.invalidate_ = function(type, k, v) {
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
  this.defs_ = []
  this.default_ = new Definition(this, [])
  this.log = opt.log  || function() {}
}

/*
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

}*/


Indexer.prototype.define = function (key) {
  var def = new Definition(this, key)
  // todo: check collisions
  this.defs_.push(def)
  return def
}

Indexer.prototype.get = function (key) {
  assert(key, key + ' is not a valid key')

  for (var i = 0; i < this.defs_.length; i++) {
    if (this.defs_[i].match(key)) {
      return this.defs_[i].getOrCreate(key)
    }
  }

  return this.default_.getOrCreate(key)
}


Indexer.prototype.has = function(key) {
  assert(key, key + ' is not a valid key')
  return !!this.tree.get(bytewise.encode(key))
}

Indexer.prototype.del = function(key) {
  if (this.has(key)) {
    this.tree.get(bytewise.encode(key)).del()
  }
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

Indexer.prototype.dispatch_ = function (type, k, v) {
  if (type === 'add') {
    this.tree.put(Buffer(k, 'binary'), v)
  }
  else if (type === 'delete') {
    this.tree.del(Buffer(k, 'binary'), v)
  }

  // r-tree?
  for (var i = 0; i < this.listeners_.length; i++) {
    var s = this.listeners_[i]
    if (s.start <= keyenc && s.end >= keyenc) {
      s.invalidate_(type, k, v)
    }
  }
}

/*
Indexer.prototype.createReducedField = function (property, func) {
  assert.ok(typeof property === 'string')
  assert.ok(typeof func === 'function')

  return new Reducer(this, property, func)
}
*/

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