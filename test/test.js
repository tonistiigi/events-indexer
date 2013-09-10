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


test('range strings', function(t) {
  t.plan(4)

  var db = indexer()

  db.set('foo', 'width', 1)
  db.set('bar', 'width', 2)
  db.set('fao', 'width', 3)

  t.deepEqual(db.range(), [
    {k: 'bar', v: {width: 2}},
    {k: 'fao', v: {width: 3}},
    {k: 'foo', v: {width: 1}},
  ])

  t.deepEqual(db.range('fao'), [
    {k: 'fao', v: {width: 3}}
  ])

  t.deepEqual(db.range('f', 'g'), [
    {k: 'fao', v: {width: 3}},
    {k: 'foo', v: {width: 1}},
  ])

  t.deepEqual(db.range('g', 'z'), [])

})


test('range arrays', function(t) {
  t.plan(3)

  var db = indexer()

  db.set(['foo', 11], 'width', 1)
  db.set(['bar', 12], 'width', 2)
  db.set(['fao', 13], 'width', 3)
  db.set(['foo', 10], 'width', 4)

  t.deepEqual(db.range(), [
    {k: ['bar', 12], v: {width: 2}},
    {k: ['fao', 13], v: {width: 3}},
    {k: ['foo', 10], v: {width: 4}},
    {k: ['foo', 11], v: {width: 1}},
  ])

  t.deepEqual(db.range(['foo'], ['foo', '\u9999']), [
    {k: ['foo', 10], v: {width: 4}},
    {k: ['foo', 11], v: {width: 1}},
  ])

  t.deepEqual(db.range(['foo', 'a'], ['\u9999']), [])

})


test('reduce', function(t) {
  t.plan(2)

  var db = indexer()

  var avgwidth = db.reducer('avgwidth', function (values) {
    var sum = 0
    for (var i = 0; i < values.length; i++) {
      sum += values[i]
    }
    return values.length && sum / values.length
  })

  avgwidth.set('foo', 1, 10)
  avgwidth.set('bar', 2, 5)
  avgwidth.set('bar', 3, 7)
  avgwidth.set('foo', 4, 20)
  avgwidth.set('foo', 5, 6)

  db.set('foo', 'bar', 'baz')

  t.deepEqual(db.get('foo'), {
    bar: 'baz',
    avgwidth: 12
  })

  t.deepEqual(db.get('bar'), {
    avgwidth: 6
  })

})
