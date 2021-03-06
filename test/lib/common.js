const path = require('path')
const Readable = require('stream').Readable
const sourceDir = path.join(__dirname, '..', 'sql')

module.exports = test => {
  test('initializes correctly', t => {
    t.is(t.context.db.options.database, 'test')
    t.is(t.context.db.directory, sourceDir)
  })

  test('exposes existence of functions', async t => {
    t.plan(1)
    t.context.Link.fn.selectInteger = 42
    await t.context.db.connect(sql => t.true('selectInteger' in sql))
  })

  test('executes value function', async t => {
    t.context.Link.fn.selectInteger = 43
    const i = await t.context.db.connect(sql => sql.selectInteger(43))
    t.is(i, 43)
  })

  test('executes a row function', async t => {
    t.context.Link.fn.selectIntegerAndString = {number: 42, str: 'abc'}
    const r = await t.context.db.connect(sql => sql.selectIntegerAndString(42, 'abc'))
    t.is(r.number, 42)
    t.is(r.str, 'abc')
  })

  test('executes a column function', async t => {
    t.context.Link.fn.selectColumn = [0, 1, 2, 3]
    const column = await t.context.db.connect(sql => sql.selectColumn(3))
    t.deepEqual(column, [0, 1, 2, 3])
  })

  test('executes a table function', async t => {
    t.context.Link.fn.selectSeries = [
      {num: 0},
      {num: 1},
      {num: 2},
      {num: 3},
      {num: 4},
      {num: 5},
      {num: 6},
      {num: 7},
      {num: 8},
    ]
    const rows = await t.context.db.connect(sql => sql.selectSeries(8))
    t.true(Array.isArray(rows))
    t.is(rows.length, 9)
    for (let i = 0; i < 9; i++) {
      t.is(rows[i].num, i)
    }
  })

  test('executes a result function', async t => {
    t.context.Link.fn.selectResult = {
      command: 'SELECT',
      rowCount: 9,
      rows: [
        {num: 0},
        {num: 1},
        {num: 2},
        {num: 3},
        {num: 4},
        {num: 5},
        {num: 6},
        {num: 7},
        {num: 8},
      ],
      fields: [{name: 'num'}],
    }

    const result = await t.context.db.connect(sql => sql.selectResult(8))
    t.is(result.command, 'SELECT')
    t.is(result.rowCount, 9)
    t.true(Array.isArray(result.rows))
    t.is(result.rows.length, 9)
    t.true(Array.isArray(result.fields))
    t.is(result.fields.length, 1)
  })

  test('executes a writestream function', async t => {
    const expectedTimeSeries = [
      {day: new Date(2019, 0, 1), value: 1},
      {day: new Date(2019, 0, 2), value: 2},
      {day: new Date(2019, 0, 3), value: 3},
    ]

    t.context.Link.fn.createTsTable = () => {}
    t.context.Link.fn.copyToTs = fn => {
      t.is(typeof fn, 'function')
      return 3
    }
    t.context.Link.fn.selectAllTs = expectedTimeSeries

    let rowCount
    const rows = await t.context.db.txn(async sql => {
      await sql.createTsTable()
      rowCount = await sql.copyToTs(ws => {
        ws.write('2019-01-01\t1\n')
        ws.write('2019-01-02\t2\n')
        ws.write('2019-01-03\t3\n')
        ws.end()
      })
      return sql.selectAllTs()
    })

    t.is(rowCount, 3)
    t.deepEqual(rows, expectedTimeSeries)
  })

  test('executes a readstream function', async t => {
    const expectedTsv = '2019-02-01\t11\n2019-02-02\t12\n2019-02-03\t13\n'

    t.context.Link.fn.createTsTable = () => {}
    t.context.Link.fn.insertTs = () => {}
    t.context.Link.fn.copyFromTs = fn => {
      rs = new Readable()
      rs.push(expectedTsv)
      rs.push(null)
      fn(rs)
      return 3
    }

    let rowCount
    const str = await t.context.db.txn(async sql => {
      await sql.createTsTable()
      await sql.insertTs('2019-02-01', 11)
      await sql.insertTs('2019-02-02', 12)
      await sql.insertTs('2019-02-03', 13)

      let tsv = ''
      rowCount = await sql.copyFromTs(rs => {
        rs.on('data', buf => (tsv += buf.toString('utf8')))
      })
      return tsv
    })

    t.is(rowCount, 3)
    t.is(str, expectedTsv)
  })

  test('executes a dynamic value function', async t => {
    const query = {sql: 'select $1::int', return: 'value', args: [3]}
    t.context.Link.fn.$selectInteger = 3
    const i = await t.context.db.connect(sql => sql.$selectInteger(query))
    t.is(i, 3)
  })

  test('executes a dynamic row function', async t => {
    const query = {sql: 'select $1::int as number', return: 'row', args: [3]}
    t.context.Link.fn.$selectInteger = {number: 3}
    const row = await t.context.db.connect(sql => sql.$selectInteger(query))
    t.deepEqual(row, {number: 3})
  })

  test('executes a dynamic table function', async t => {
    const query = {sql: 'select * from generate_series(0, $1) AS num', return: 'table', args: [3]}

    t.context.Link.fn.$selectSeries = [{num: 0}, {num: 1}, {num: 2}, {num: 3}]

    const rows = await t.context.db.connect(sql => sql.$selectSeries(query))
    t.true(Array.isArray(rows))
    t.is(rows.length, 4)
    for (let i = 0; i < 4; i++) {
      t.is(rows[i].num, i)
    }
  })

  test('executes a dynamic result function', async t => {
    t.context.Link.fn.$selectResult = {
      command: 'SELECT',
      rowCount: 3,
      rows: [{num: 0}, {num: 1}, {num: 2}],
      fields: [{name: 'num'}],
    }

    const query = {sql: 'select * from generate_series(0, $1) AS num', args: [2]}
    const result = await t.context.db.connect(sql => sql.$selectResult(query))

    t.is(result.command, 'SELECT')
    t.is(result.rowCount, 3)
    t.true(Array.isArray(result.rows))
    t.is(result.rows.length, 3)
    t.true(Array.isArray(result.fields))
    t.is(result.fields.length, 1)
  })

  test('executes a dynamic writestream function', async t => {
    const expectedTimeSeries = [
      {day: new Date(2019, 0, 1), value: 1},
      {day: new Date(2019, 0, 2), value: 2},
      {day: new Date(2019, 0, 3), value: 3},
    ]

    t.context.Link.fn.createTsTable = () => {}
    t.context.Link.fn.$copyToTs = fn => {
      t.is(typeof fn, 'function')
      return 3
    }
    t.context.Link.fn.selectAllTs = expectedTimeSeries

    const writeFn = ws => {
      ws.write('2019-01-01\t1\n')
      ws.write('2019-01-02\t2\n')
      ws.write('2019-01-03\t3\n')
      ws.end()
    }
    const query = {
      sql: 'copy timeseries(day, value) from stdin',
      args: [writeFn],
      return: 'writestream',
    }

    let rowCount
    const rows = await t.context.db.txn(async sql => {
      await sql.createTsTable()
      rowCount = await sql.$copyToTs(query)
      return sql.selectAllTs()
    })

    t.is(rowCount, 3)
    t.deepEqual(rows, expectedTimeSeries)
  })

  test('executes a dynamic readstream function', async t => {
    const expectedTsv = '2019-02-01\t11\n2019-02-02\t12\n2019-02-03\t13\n'

    t.context.Link.fn.createTsTable = () => {}
    t.context.Link.fn.insertTs = () => {}
    t.context.Link.fn.$copyFromTs = fn => {
      rs = new Readable()
      rs.push(expectedTsv)
      rs.push(null)
      fn(rs)
      return 3
    }

    let tsv = ''
    const readFn = rs => rs.on('data', buf => (tsv += buf.toString('utf8')))
    const query = {
      sql: 'copy timeseries(day, value) to stdout',
      return: 'readstream',
      args: [readFn],
    }

    let rowCount
    const str = await t.context.db.txn(async sql => {
      await sql.createTsTable()
      await sql.insertTs('2019-02-01', 11)
      await sql.insertTs('2019-02-02', 12)
      await sql.insertTs('2019-02-03', 13)

      rowCount = await sql.$copyFromTs(query)
      return tsv
    })

    t.is(rowCount, 3)
    t.is(str, expectedTsv)
  })

  test('executes queries in parallel', async t => {
    t.context.Link.fn.selectInteger = 3

    const [one, two, three] = await t.context.db.all(
      c => c.selectInteger(3),
      c => c.selectInteger(3),
      c => c.selectInteger(3),
    )

    t.is(one, 3)
    t.is(two, 3)
    t.is(three, 3)
  })

  test('executes queries in a transaction', async t => {
    const {Link, db} = t.context
    Link.fn.insertN = () => {}
    Link.fn.zeroN = () => {}
    Link.fn.sumN = 42

    await db.txn(async one => {
      await one.insertN(42)
      await db.txn(two => two.zeroN())
      const sum = await one.sumN()
      t.is(sum, 42)
    })
  })

  test('automatically rolls back transactions', async t => {
    const {db, Link} = t.context
    Link.fn.error = () => {
      throw new Error('column "this_column_doesnt_exist" does not exist')
    }

    try {
      await t.context.db.txn(async sql => {
        await sql.error()
        await sql.selectInteger(2)
        t.fail('the promise should be rejected')
      })
    } catch (e) {
      t.is(e.source, `${db.directory}/error.sql`)
    }
  })

  test('QueryFailed includes context', async t => {
    const {db, Link} = t.context

    Link.fn.errorWithArguments = () => {
      throw new Error('whiffle')
    }

    try {
      await db.connect(sql => sql.errorWithArguments(42, 21, 96))
    } catch (e) {
      t.deepEqual(e.arguments, [42, 21, 96])
      t.is(e.return, 'row')
      t.true(e.source.includes(`${sourceDir}/errorWithArguments.sql`))
    }
  })

  test('executes queries in a transaction with specific characteristics', async t => {
    const {Link, db} = t.context
    Link.fn.insertN = () => {
      throw new Error('transaction is read-only')
    }

    try {
      await db.txn('read only', async one => one.insertN(42))
      t.fail('the query should fail, because the transaction is read only')
    } catch (e) {
      t.is(e.query, 'insertN')
    }
  })

  test('executes queries in a serializable transaction', async t => {
    const {Link, db} = t.context
    Link.fn.selectInteger = () => (4)

    const sum = await db.serialize(sql => sql.selectInteger(4))
    t.true(typeof sum === 'number')
  })
}
