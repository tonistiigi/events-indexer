var Tree = require('btreejs').create()
var bytewise = require('bytewise')
var assert = require('assert')

function Indexer() {
  this.tree = new Tree()
}

Indexer.prototype.set = function (key, property, value) {
  assert(key, key + ' is not a valid key')
  assert.equal(typeof property, 'string', 'property needs to be a string')

  var key = bytewise.encode(key)
  var current = this.tree.get(key)

  if (current === undefined) {
    current = {}
    this.tree.put(key, current)
  }
  current[property] = value
}

Indexer.prototype.merge = function (key, object) {
  assert(key, key + ' is not a valid key')
  assert.equal(typeof object, 'object')

  var key = bytewise.encode(key)
  var current = this.tree.get(key)

  if (current === undefined) {
    current = {}
    this.tree.put(key, current)
  }
  for (var i in object) {
    current[i] = object[i]
  }
}

Indexer.prototype.get = function (key) {
  assert(key, key + ' is not a valid key')
  return this.tree.get(bytewise.encode(key))
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
    result.push({k: bytewise.decode(key), v: v})
  })
  return result
}

Indexer.prototype.subscribe = function (start, end) {

}

Indexer.prototype.reducer = function (property, func) {

}

function Reducer() {

}

Reducer.prototype.set = function (key, id, value) {

}


module.exports = exports = function(opt) {
  return new Indexer(opt)
}

exports.Indexer = Indexer
exports.Reducer = Reducer