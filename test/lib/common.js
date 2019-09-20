const path = require('path')

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
}
