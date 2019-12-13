'use strict'

const crypto = require('crypto')
const fs = require('fs')
const path = require('path')
const parseConnectionString = require('pg-connection-string').parse
const util = require('util')
const randomBytes = util.promisify(crypto.randomBytes)

class BaseStrategy {
  static async disconnect() {}

  constructor(_options) {
    this.directory = _options.directory
    delete _options.directory

    const options = Object.assign({}, _options)
    if ('connectionString' in options) {
      const parsed = parseConnectionString(options.connectionString)
      delete options.connectionString
      parsed.host = parsed.host || process.env.PGHOST || 'localhost'
      parsed.port = parsed.port || process.env.PGPORT
      parsed.user = parsed.user || process.env.PGUSER
      parsed.database = parsed.database || process.env.PGDATABASE
      parsed.password = parsed.password || process.env.PGPASSWORD
      Object.assign(options, parsed)
    }

    this.options = options
    this.methods = {
      begin: this.createTxnMethod('begin'),
      commit: this.createTxnMethod('commit'),
      rollback: this.createTxnMethod('rollback'),
    }
  }

  async disconnect() {}

  hasMethod(name) {
    return name in this.methods
  }

  create(name) {
    var text
    var source
    try {
      source = path.join(this.directory, name + '.sql')
      text = fs.readFileSync(source, 'utf8')
    } catch (e) {
      return undefined
    }

    const meta = {source}
    this.extractMetaData(text, meta)

    const fn = this.createMethod(name, meta, text)
    this.methods[name] = fn
    return this.methods[name]
  }

  extractMetaData(text, meta) {
    const pattern = /^--\*\s+(\w+)\s+(\w+)/g

    var match
    while ((match = pattern.exec(text)) !== null) {
      meta[match[1]] = match[2]
    }

    return meta
  }

  desc(options) {
    return Object.assign({url: this.url}, options)
  }

  async genId() {
    const buf = await randomBytes(3)
    return buf.toString('hex')
  }
}

const findCaller = stack => {
  let foundBluefin = false
  return stack.find(site => {
    const filename = site.getFileName()
    if (!filename) return false
    const includesBluefin = filename.includes('bluefin-link')
    if (includesBluefin) foundBluefin = true
    return foundBluefin && !includesBluefin
  })
}

module.exports = BaseStrategy
