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
  t.plan(14)

  var db = indexer()

  db.set(['foo', 11], {width: 1})
  db.set(['bar', 12], {width: 2})

  var foo = db.subscribe(['foo'], ['foo', '\u9999'])
  var all = db.subscribe()
  var bar = db.subscribe(['bar'], ['baz'])

  var foo_ = 0
  foo.on('update', function(k, v) {
    foo_++
    if (foo_ === 1) {
      t.deepEqual(k, ['foo', 11])
      t.deepEqual(v, {width: 2})
    }
    else if (foo_ === 2) {
      t.deepEqual(k, ['foo', 12])
      t.deepEqual(v, {height: 2})
    }
    else {
      t.fail()
    }
  })

  var all_ = 0
  all.on('update', function(k, v) {
    all_++
    if (all_ === 1) {
      t.deepEqual(k, ['bat', 10])
      t.deepEqual(v, {width: 10})
    }
    else if (all_ === 2) {
      t.deepEqual(k, ['bar', 12])
      t.deepEqual(v, {width: 2, height: 12})
    }
    else if (all_ === 3) {
      t.deepEqual(k, ['foo', 11])
      t.deepEqual(v, {width: 2})
    }
    else {
      t.fail()
    }
  })

  var bar_ = 0
  bar.on('update', function(k, v) {
    bar_++
    if (bar_ === 1) {
      t.deepEqual(k, ['bat', 10])
      t.deepEqual(v, {width: 10})
    }
    else if (bar_ === 2) {
      t.deepEqual(k, ['bar', 12])
      t.deepEqual(v, {width: 2, height: 12})
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
  t.plan(6)

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
  foo.on('update', function(k, v) {
    count++
    if (count === 1) {
      t.deepEqual(k, ['foo'])
      t.deepEqual(v, {totalWidth: 10})
    }
    else if (count === 2) {
      t.deepEqual(k, ['foo'])
      t.deepEqual(v, {totalWidth: 30})
    }
    else if (count === 3) {
      t.deepEqual(k, ['foo'])
      t.deepEqual(v, {totalWidth: 26})
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

test("throttled subscribe", function(t) {
  t.plan(2)

  var db = indexer()

  db.set(['foo', 11], {width: 1})
  db.set(['foo', 12], {width: 2})

  var foo = db.subscribe(['foo'], ['foo', '\u9999'])

  var foo_ = 0
  foo.throttle(.1).on('data', function(items) {
    foo_++
    if (foo_ === 1) {
      t.deepEqual(items, [
        {t: 'u', k: ['foo', 11], v: {width: 2, height: 10}},
        {t: 'u', k: ['foo', 12], v: {width: 10}}
      ])
    }
    else if (foo_ === 2) {
      t.deepEqual(items, [
        {t: 'u', k: ['foo', 12], v: {width: 15}}
      ])
    }
    else {
      t.fail()
    }
  })

  db.set(['bat', 10], {width: 10})
  db.set(['foo', 11], {width: 2})
  db.set(['foo', 11], {height: 10})

  setTimeout(function() {
    db.set(['foo', 12], {width: 10})
  }, 10)

  setTimeout(function() {
    db.set(['foo', 12], {width: 15})
  }, 140)

  setTimeout(function() {
    foo.close()
  }, 150)

  setTimeout(function() {
    db.set(['foo', 12], {width: 10})
  }, 300)

})

test("range query without end argument", function(t) {
  t.plan(5)

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
  foostar.on('update', function(k, v) {
    count++
    if (count === 1) {
      t.deepEqual(k, 'foobar')
      t.deepEqual(v, {b: 1})
    }
    else if (count === 2) {
      t.deepEqual(k, 'foo')
      t.deepEqual(v, {b: 2})
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

test("map definitions", function(t) {
  t.plan(6)

  var db = indexer()

  var foos = db.define(['foo', ':id'])

  foos.map(['foo2', ':id', 'bar'])
  foos.map(['foow', ':width'], ['id'])

  var count = 0
  foos.on('create', function(a) {
    count++
    if (count === 1) {
      t.deepEqual(a.getValue(), {id: 1})
    }
    else if (count === 2) {
      t.deepEqual(a.getValue(), {id: 3})
    }
    else {
      t.fail()
    }
    a.data.default = 2
  })

  var count2 = 0
  foos.on('change', function(a) {
    count2++
    if (count2 === 1) {
      t.deepEqual(a.getValue(), {id: 1, width: 10, default: 2})
    }
    else if (count2 === 2) {
      t.deepEqual(a.getValue(), {id: 3, width: 30, default: 2})
    }
    else {
      t.fail()
    }
  })

  db.set(['foo', 1], {width: 10})
  db.set(['bar', 2], {width: 20})
  db.set(['foo', 3], {width: 30})

  t.deepEqual(db.getRange(['foo2']), [
    {k: ['foo2', 1, 'bar'], v: {id: 1, width: 10, default: 2}},
    {k: ['foo2', 3, 'bar'], v: {id: 3, width: 30, default: 2}}
  ])

  t.deepEqual(db.getRange(['foow'], undefined, {order: 'desc'}), [
    {k: ['foow', 30], v: {id: 3}},
    {k: ['foow', 10], v: {id: 1}}
  ])

})

test("reducers in map", function(t) {
  t.plan(3)

  var db = indexer()

  var foos = db.define(['foo', ':id'])

  foos.reduce('width', function(v) {
    return v.reduce(function(memo, a) {
      return memo + a
    }, 0)
  })

  foos.map(function(t) {
    return ['bar', t.bar.toUpperCase()]
  }, ['width'])

  foos.on('add', function(k) {
    console.log('add', k)
  })
  foos.on('update', function(k) {
    console.log('add', k)
  })

  db.set(['foo', 1], {bar: 'bb', width: [1, 3]})
  db.set(['foo', 1], {width: [2, 7]})
  db.set(['foo', 2], {bar: 'aa', width: [1, 9]})
  db.set(['foo', 1], {width: [3, 11]})

  t.deepEqual(db.getRange(['bar']), [
    { k: [ 'bar', 'AA' ], v: { width: 9 } },
    { k: [ 'bar', 'BB' ], v: { width: 21 } }
  ])

  db.set(['foo', 1], {width: [2, 7]})
  t.deepEqual(db.getRange(['bar']), [
    { k: [ 'bar', 'AA' ], v: { width: 9 } },
    { k: [ 'bar', 'BB' ], v: { width: 21 } }
  ])

  db.set(['foo', 1], {width: [4, 7]})
  t.deepEqual(db.getRange(['bar']), [
    { k: [ 'bar', 'AA' ], v: { width: 9 } },
    { k: [ 'bar', 'BB' ], v: { width: 28 } }
  ])

})