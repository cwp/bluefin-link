const fs = require('fs')
const path = require('path')
const {Jot} = require('bluefin-jot')

const MockStrategy = require('./mock.js')
const PgStrategy = require('./pg.js')

const checkLinkArgs = (options, segments, cb) => {
  if (segments.length < 1) throw new Error('No query directory specified')

  options = typeof options === 'string' ? {connectionString: options} : options
  options.directory = path.resolve.apply(path, segments)
  fs.accessSync(options.directory, fs.R_OK)
  return options
}

class Link {
  static disconnect() {
    return PgStrategy.disconnect()
  }

  constructor(strategy) {
    this.strategy = strategy
    this.log = this.constructor.log
  }

  get options() {
    return this.strategy.options
  }

  get directory() {
    return this.strategy.directory
  }

  connect(fn) {
    return this.strategy.withConnection(this.log, connection => {
      const handler = new Handler(this.strategy)
      const proxy = new Proxy(connection, handler)
      return fn(proxy)
    })
  }

  disconnect() {
    return this.strategy.disconnect()
  }

  all() {
    const results = [...arguments].map(ea => this.connect(ea))
    return Promise.all(results)
  }

  txn(fn) {
    return this.connect(async sql => {
      await sql.begin()
      try {
        var result = await fn(sql)
        await sql.commit()
      } catch (err) {
        await sql.rollback()
        throw err
      }
      return result
    })
  }
}

Link.log = new Jot()

class PgLink extends Link {
  static mock() {
    const MockLink = class extends Link {
      static clearMocks() {
        for (let name of Object.keys(this.fn)) {
          delete this.fn[name]
        }
      }

      constructor(options, ...segments) {
        options = checkLinkArgs(options, segments)
        const strategy = new MockStrategy(options, MockLink.fn)
        super(strategy)
      }
    }
    MockLink.fn = {}
    if ('log' in PgLink) MockLink.log = PgLink.log
    return MockLink
  }

  constructor(options, ...segments) {
    options = checkLinkArgs(options, segments)
    super(new PgStrategy(options))
  }
}

class Handler {
  constructor(strategy) {
    this.strategy = strategy
  }

  has(target, name) {
    if (name in target) return true
    if (this.strategy.hasMethod(name)) return true
    this.strategy.create(name)
    return this.strategy.hasMethod(name)
  }

  get(target, name) {
    if (name in target) return target[name]
    if (name in this.strategy.methods) return this.strategy.methods[name]
    return this.strategy.create(name)
  }
}

module.exports = PgLink
