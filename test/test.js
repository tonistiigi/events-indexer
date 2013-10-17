var test = require('tape')
var indexer = require('../')


test('get/set strings', function(t) {
  t.plan(7)

  var db = indexer()

  db.set('foo', {width: 1})
  db.set('bar', {width: 2})
  db.set('foo', {height: 3})

  t.deepEqual(db.getValue('foo'), {width: 1, height: 3})
  t.deepEqual(db.getValue('bar'), {width: 2})

  var bar = db.getValue('bar')
  db.set('bar', {width: 4})

  t.deepEqual(db.getValue('bar'), {width: 4})
  t.equal(db.getValue('bar'), bar)

  db.set('foo', {width: 5, size: 6})

  t.deepEqual(db.getValue('foo'), {width: 5, size: 6, height: 3})
  t.equal(db.getValue('nosuchkey'), undefined)
  t.equal(db.getValue('bar '), undefined)

})

test('get/set arrays', function(t) {
  t.plan(6)

  var db = indexer()

  db.set(['foo', 'baz'], {width: 11})
  db.set(['bar', 12], {width: 12})

  t.deepEqual(db.getValue(['foo', 'baz']), {width: 11})
  t.deepEqual(db.getValue(['bar', 12]), {width: 12})

  db.set(['bar', 12], {height: 13})

  t.deepEqual(db.getValue(['bar', 12]), {width: 12, height: 13})

  t.equal(db.getValue([]), undefined)
  t.equal(db.getValue(['foo']), undefined)
  t.equal(db.getValue(['foo', 'baz', '']), undefined)


})

test('invalid arguments', function(t) {
  t.plan(9)

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
    db.getRange(NaN)
  })

  t.throws(function() {
    db.getRange('foo', null)
  })

  t.throws(function() {
    db.subscribe(NaN)
  })

  t.throws(function() {
    db.subscribe('foo', null)
  })

  t.throws(function() {
    db.createReducedField(10, function() {})
  })

  t.throws(function() {
    db.createReducedField('foo', 'bar')
  })

})


test('range strings', function(t) {
  t.plan(4)

  var db = indexer()

  db.set('foo', {width: 1})
  db.set('bar', {width: 2})
  db.set('fao', {width: 3})

  t.deepEqual(db.getRange(), [
    {k: 'bar', v: {width: 2}},
    {k: 'fao', v: {width: 3}},
    {k: 'foo', v: {width: 1}},
  ])

  t.deepEqual(db.getRange('fao'), [
    {k: 'fao', v: {width: 3}}
  ])

  t.deepEqual(db.getRange('f', 'g'), [
    {k: 'fao', v: {width: 3}},
    {k: 'foo', v: {width: 1}},
  ])

  t.deepEqual(db.getRange('g', 'z'), [])

})

test('range arrays', function(t) {
  t.plan(3)

  var db = indexer()

  db.set(['foo', 11], {width: 1})
  db.set(['bar', 12], {width: 2})
  db.set(['fao', 13], {width: 3})
  db.set(['foo', 10], {width: 4})

  t.deepEqual(db.getRange(undefined, undefined, {order: 'desc', limit: 3}), [
    {k: ['foo', 11], v: {width: 1}},
    {k: ['foo', 10], v: {width: 4}},
    {k: ['fao', 13], v: {width: 3}}
  ])

  t.deepEqual(db.getRange(['foo'], ['foo', '\u9999']), [
    {k: ['foo', 10], v: {width: 4}},
    {k: ['foo', 11], v: {width: 1}},
  ])

  t.deepEqual(db.getRange(['foo', 'a'], ['\u9999']), [])

})


test('reduce', function(t) {
  t.plan(4)

  var db = indexer()

  var call_count = 0
  db.default_.reduce('avgwidth', function (values) {
    call_count++
    if (call_count === 1 || call_count === 2) t.pass()
    else t.fail()

    var sum = 0
    for (var i = 0; i < values.length; i++) {
      sum += values[i]
    }
    return values.length && sum / values.length
  })

  db.set('foo', {avgwidth: [1, 10]})
  db.set('bar', {avgwidth: [2, 5]})
  db.set('bar', {avgwidth: [3, 7]})
  db.set('foo', {avgwidth: [4, 20]})
  db.set('foo', {avgwidth: [5, 6]})

  db.set('foo', {bar: 'baz'})

  t.deepEqual(db.getValue('foo'), {
    bar: 'baz',
    avgwidth: 12
  })

  t.deepEqual(db.getValue('bar'), {
    avgwidth: 6
  })

})

test('subscribe', function(t) {
  t.plan(7)

  var db = indexer()

  db.set(['foo', 11], {width: 1})
  db.set(['bar', 12], {width: 2})

  var foo = db.subscribe(['foo'], ['foo', '\u9999'])
  var all = db.subscribe()
  var bar = db.subscribe(['bar'], ['baz'])

  var foo_ = 0
  foo.on('data', function(items) {
    foo_++
    if (foo_ === 1) {
      t.deepEqual(items, [
        {k: ['foo', 11], v: {width: 2}}
      ])
    }
    else if (foo_ === 2) {
      t.deepEqual(items, [
        {k: ['foo', 12], v: {height: 2}}
      ])
    }
    else {
      t.fail()
    }
  })

  var all_ = 0
  all.on('data', function(items) {
    all_++
    if (all_ === 1) {
      t.deepEqual(items, [
        {k: ['bat', 10], v: {width: 10}}
      ])
    }
    else if (all_ === 2) {
      t.deepEqual(items, [
        {k: ['bar', 12], v: {width: 2, height: 12}}
      ])
    }
    else if (all_ === 3) {
      t.deepEqual(items, [
        {k: ['foo', 11], v: {width: 2}}
      ])
    }
    else {
      t.fail()
    }
  })

  var bar_ = 0
  bar.on('data', function(items) {
    bar_++
    if (bar_ === 1) {
      t.deepEqual(items, [
        {k: ['bat', 10], v: {width: 10}}
      ])
    }
    else if (bar_ === 2) {
      t.deepEqual(items, [
        {k: ['bar', 12], v: {width: 2, height: 12}}
      ])
    }
    else {
      t.fail()
    }
  })

  db.set(['bat', 10], {width: 10})
  db.set(['bar', 12], {height: 12})
  db.set(['foo', 11], {width: 2})

  all.close()

  db.set(['fob', 10], {width: 10})
  db.set(['foo', 12], {height: 2})
  db.set(['fob', 10], {width: 10})

  db.set(['foo', 11], {width: 2})

  foo.close()
  bar.close()

  db.set(['foo', 11], {width: 20})

})

test('subscribe reducers', function(t) {
  t.plan(3)

  var db = indexer()

  db.default_.reduce('totalWidth', function (values) {
    var sum = 0
    for (var i = 0; i < values.length; i++) {
      sum += values[i]
    }
    return sum
  })

  var foo = db.subscribe(['foo'])

  var count = 0
  foo.on('data', function(items) {
    count++
    if (count === 1) {
      t.deepEqual(items, [
        {k: ['foo'], v: {totalWidth: 10}}
      ])
    }
    else if (count === 2) {
      t.deepEqual(items, [
        {k: ['foo'], v: {totalWidth: 30}}
      ])
    }
    else if (count === 3) {
      t.deepEqual(items, [
        {k: ['foo'], v: {totalWidth: 26}}
      ])
    }
    else {
      t.fail()
    }
  })

  db.set(['foo'], {totalWidth: [1, 10]})
  db.set(['bar'], {totalWidth: [2, 5]})
  db.set(['bar'], {totalWidth: [3, 7]})
  db.set(['foo'], {totalWidth: [4, 20]})
  db.set(['foo'], {totalWidth: [1, 6]})

  foo.close()
  db.set(['foo'], {totalWidth: [1, 8]})

})
/*
test("throttled subscribe", function(t) {
  t.plan(2)

  var db = indexer({throttle: 100})

  db.set(['foo', 11], 'width', 1)
  db.set(['bar', 12], 'width', 2)

  var foo = db.subscribe(['foo'], ['foo', '\u9999'])

  var foo_ = 0
  foo.on('data', function(items) {
    foo_++
    if (foo_ === 1) {
      t.deepEqual(items, [
        {k: ['foo', 11], v: {width: 2, height: 10}},
        {k: ['foo', 12], v: {width: 10}}
      ])
    }
    else if (foo_ === 2) {
      t.deepEqual(items, [
        {k: ['foo', 12], v: {width: 15}}
      ])
    }
    else {
      t.fail()
    }
  })

  db.set(['bat', 10], 'width', 10)
  db.set(['foo', 11], {width: 2})
  db.set(['foo', 11], {height: 10})

  setTimeout(function() {
    db.set(['foo', 12], {width: 10})
  }, 10)

  setTimeout(function() {
    db.set(['foo', 12], 'width', 15)
  }, 140)

  setTimeout(function() {
    foo.close()
  }, 150)

  setTimeout(function() {
    db.set(['foo', 12], {width: 10})
  }, 300)

})
*/
test("range query without end argument", function(t) {
  t.plan(3)

  var db = indexer()

  db.set(['foo', 1], {a: 1})
  db.set(['foo', 2], {a: 2})
  db.set(['foo', 3], {a: 3})

  t.deepEqual(db.getRange(['foo']), [
    { k: [ 'foo', 1 ], v: { a: 1 } },
    { k: [ 'foo', 2 ], v: { a: 2 } },
    { k: [ 'foo', 3 ], v: { a: 3 } }
  ])

  var foostar = db.subscribe('foo')

  var count = 0
  foostar.on('data', function(items) {
    count++
    if (count === 1) {
      t.deepEqual(items, [
        { k: 'foobar', v: {b: 1} }
      ])
    }
    else if (count === 2) {
      t.deepEqual(items, [
        { k: 'foo', v: {b: 2} }
      ])
    }
    else {
      t.fail()
    }
  })

  db.set('bar', {b: 3})
  db.set('foobar', {b: 1})
  db.set('foo', {b: 2})
  db.set('fozbar', {b: 4})


})