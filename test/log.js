const test = require('ava')
const PgLink = require('../src')
const sinon = require('sinon')
const {Jot, DebugTarget} = require('bluefin-jot')

let target
let jot
let spy
test.before(t => {
  target = new DebugTarget()
  jot = new Jot(target)
  spy = sinon.spy(target, 'finish')
})

test.after(t => {
  spy.restore()
})

test.beforeEach(t => {
  spy.resetHistory()
})

test('mock logs queries to a custom log', async t => {
  const Link = PgLink.mock()
  Link.fn.selectInteger = 42
  const db = new Link('pg:///test', __dirname, 'sql')
  db.log = jot
  await db.connect(sql => sql.selectInteger(1))
  t.true(spy.called)
  t.true(
    spy.calledWith(
      sinon.match({name: 'mock.query'}),
      sinon.match({source: `${db.directory}/selectInteger.sql`, return: 'value', arguments: [1]}),
    ),
  )
})

test('pg logs queries to a custom log', async t => {
  const db = new PgLink('pg:///test', __dirname, 'sql')
  db.log = jot
  await db.connect(sql => sql.selectInteger(1))
  t.true(spy.called)
  t.true(
    spy.calledWith(
      sinon.match({name: 'pg.query'}),
      sinon.match({source: `${db.directory}/selectInteger.sql`, return: 'value', arguments: [1]}),
    ),
  )
})
