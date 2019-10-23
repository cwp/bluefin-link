const test = require('ava')
const Link = require('../src')
const common = require('./lib/common')

test.beforeEach(t => {
  t.context.Link = Link.mock()
  t.context.db = new t.context.Link('pg:///test', __dirname, 'sql')
})

common(test)

test('rejects a table mock that is not an array', t => {
  t.context.Link.fn.selectSeries = 42
  return t.context.db.connect(sql => sql.selectSeries(8)).catch(error => {
    t.is(error.message, 'mock does not return a table')
  })
})

test('rejects a table mock that does not contain rows', t => {
  t.context.Link.fn.selectSeries = [42]
  return t.context.db.connect(sql => sql.selectSeries(8)).catch(error => {
    t.is(error.message, 'mock does not return rows')
  })
})

test('rejects a row mock that does not return a row', t => {
  t.context.Link.fn.selectIntegerAndString = 42
  return t.context.db.connect(sql => sql.selectIntegerAndString(8)).catch(error => {
    t.is(error.message, 'mock does not return a row')
  })
})

test('rejects a row mock that returns a row with no columns', t => {
  t.context.Link.fn.selectIntegerAndString = {}
  return t.context.db.connect(sql => sql.selectIntegerAndString(8)).catch(error => {
    t.is(error.message, 'mock row should have at least one column')
  })
})

test('accepts a undefined mock row', t => {
  t.context.Link.fn.selectIntegerAndString = undefined
  return t.context.db
    .connect(sql => sql.selectIntegerAndString(8))
    .then(row => t.true(row === undefined))
})

test('includes the error name and message in the stack', t => {
  const { db, Link } = t.context
  Link.fn.errorWithArguments = () => {
    throw new Error('whiffle')
  }
  return db.connect(sql => sql.errorWithArguments(42, 21, 96)).catch(effect => {
    const {cause} = effect
    t.is(cause.message, 'whiffle')
    t.regex(cause.stack, /^Error: whiffle\n/)

    t.deepEqual(effect.arguments, [42, 21, 96])
    t.true(effect.source.endsWith('test/sql/errorWithArguments.sql'))
    t.is(effect.return, 'row')
  })
})

test('clears mocks', t => {
  const { db, Link } = t.context
  Link.fn.nurp = () => {
    t.fail('should never be called')
  }
  Link.clearMocks()
  return db
    .connect(sql => sql.nurp())
    .then(
      () => t.fail('the promise must be rejected'),
      e => t.is(e.message, 'sql.nurp is not a function')
    )
})
