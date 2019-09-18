const test = require('ava')

const PgLink = require('../src')
const options = {
  connectionString: 'pg://localhost:5430/test',
  connectionTimeoutMillis: 3,
  retries: 2,
  randomize: false,
  minTimeout: 1,
  maxTimeout: 10
}

test('connect rejects when connection fails', async t => {
  const db = new PgLink(options, __dirname, 'sql')
  const err = await t.throwsAsync(() => db.connect(sql => {}))

  t.true(typeof err.message === 'string')
  t.is(err.attempts, 3)
  t.is(err.messages.length, 3)
  for (let m of err.messages) {
    t.true(typeof m === 'string')
  }
})
