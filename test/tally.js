const test = require('ava')
const PgLink = require('../src')
const sinon = require('sinon')
const {Jot, DebugTarget} = require('bluefin-jot')

const m = sinon.match


test.beforeEach(t => {
  const target = new DebugTarget()
  t.context.jot = new Jot(target)
  t.context.spy = sinon.spy(target, 'finish')
})

test('mock sends query spans to a custom log', async t => {
  const {jot, spy} = t.context
  const Link = PgLink.mock()
  Link.fn.selectInteger = 42
  const db = new Link('pg:///test', __dirname, 'sql')
  db.log = jot

  await db.connect(sql => sql.selectInteger(1))

  t.is(spy.callCount, 3)
  t.true(spy.calledWith(m({name: 'mock.connect', duration: m.number}), m({host: 'localhost'})))
  t.true(spy.calledWith(m({name: 'mock.connection', duration: m.number}), m({host: 'localhost'})))
  t.true(
    spy.calledWith(
      m({name: 'mock.query', duration: m.number}),
      m({
        host: 'localhost',
        arguments: [1],
        query: 'selectInteger',
        source: `${db.directory}/selectInteger.sql`,
        return: 'value',
      }),
    ),
  )
})

test('pg sends query metrics to a custom log', async t => {
  const {jot, spy} = t.context
  const db = new PgLink('pg:///test', __dirname, 'sql')
  db.log = jot

  await db.connect(sql => sql.selectInteger(1))

  t.is(spy.callCount, 3)
  t.true(spy.calledWith(m({name: 'pg.connect', duration: m.number}), m({host: 'localhost'})))
  t.true(spy.calledWith(m({name: 'pg.connection', duration: m.number}), m({host: 'localhost'})))
  t.true(
    spy.calledWith(
      m({name: 'pg.query', duration: m.number}),
      m({
        host: 'localhost',
        arguments: [1],
        query: 'selectInteger',
        source: `${db.directory}/selectInteger.sql`,
        return: 'value',
      }),
    ),
  )

})
