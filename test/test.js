var test = require('tape')
var indexer = require('../')


test('get/set strings', function(t) {
  t.plan(7)

  var db = indexer()

  db.set('foo', 'width', 1)
  db.set('bar', 'width', 2)
  db.set('foo', 'height', 3)

  t.deepEqual(db.get('foo'), {width: 1, height: 3})
  t.deepEqual(db.get('bar'), {width: 2})

  var bar = db.get('bar')
  db.set('bar', 'width', 4)

  t.deepEqual(db.get('bar'), {width: 4})
  t.equal(db.get('bar'), bar)

  db.merge('foo', {width: 5, size: 6})

  t.deepEqual(db.get('foo'), {width: 5, size: 6, height: 3})
  t.equal(db.get('nosuchkey'), undefined)
  t.equal(db.get('bar '), undefined)

})

test('get/set arrays', function(t) {
  t.plan(6)

  var db = indexer()

  db.set(['foo', 'baz'], 'width', 11)
  db.set(['bar', 12], 'width', 12)

  t.deepEqual(db.get(['foo', 'baz']), {width: 11})
  t.deepEqual(db.get(['bar', 12]), {width: 12})

  db.set(['bar', 12], 'height', 13)

  t.deepEqual(db.get(['bar', 12]), {width: 12, height: 13})

  t.equal(db.get([]), undefined)
  t.equal(db.get(['foo']), undefined)
  t.equal(db.get(['foo', 'baz', '']), undefined)


})

test('invalid arguments', function(t) {
  t.plan(4)

  var db = indexer()

  t.throws(function() {
    db.get()
  })

  t.throws(function() {
    db.set(null, 'foo', 'bar')
  })

  t.throws(function() {
    db.set('foo', NaN, 'bar')
  })

  t.throws(function() {
    db.merge('foo', 'bar', 10)
  })

})

