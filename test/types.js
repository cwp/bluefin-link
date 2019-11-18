'use strict'

const test = require('ava')
const pg = require('pg')

const Link = require('../src')

const dbUrl = 'pg:///test'
const db = new Link(dbUrl, __dirname, 'sql')

class Custom {
  constructor (buf) {
    this.buf = buf
  }

  toSql () {
    return this.buf
  }
}

test('materializes counts as BigInt', t => {
  return db
    .connect(sql => sql.selectCount())
    .then(val => t.is(val, 1n))
})

test('serializes Buffers correctly', t => {
  const buf = Buffer.from('010203', 'hex')
  return db.connect(sql => sql.selectBytes(buf)).then(out => {
    t.true(Buffer.isBuffer(out))
    t.true(buf.equals(out))
  })
})

test('serializes custom objects with toSql()', t => {
  const obj = new Custom(Buffer.alloc(12))
  return db.connect(sql => sql.getByte(obj)).then(byte => {
    t.is(byte, 0)
  })
})

test('serializes outgoing arrays of objects with toSql()', t => {
  const obj1 = new Custom(Buffer.from('010203', 'hex'))
  const obj2 = new Custom(Buffer.from('040506', 'hex'))
  return db.connect(sql => sql.getArrayByte([obj1, obj2])).then(table => {
    t.is(table[0].byte, 1)
    t.is(table[1].byte, 4)
  })
})

test('serializes null correctly', t => {
  return db
    .connect(sql => sql.selectString(null))
    .then(result => t.is(result, null))
})

test('serializes undefined correctly', t => {
  return db
    .connect(sql => sql.selectString(undefined))
    .then(result => t.is(result, null))
})
