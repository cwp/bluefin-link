const test = require('ava')
const Link = require('../src')

const dbUrl = 'postgres:///test'

test('requires a URL', t => {
  t.throws(() => new Link())
})

test('requires at least one path segment', t => {
  t.throws(() => new Link(dbUrl))
})

test('requires the query directory to exist', t => {
  t.throws(() => new Link(dbUrl, __dirname, 'does-not-exist'))
})
