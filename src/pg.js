'use strict'

const crypto = require('crypto')
const pg = require('pg')
const pretry = require('promise-retry')
const copyStreams = require('pg-copy-streams')
const copyFrom = copyStreams.from
const copyTo = copyStreams.to

const BaseStrategy = require('./base')

const pools = {}
const ignoreProperties = ['name', 'severity', 'file', 'line', 'routine']

class PgStrategy extends BaseStrategy {
  static disconnect() {
    const vows = []
    for (let url in pools) {
      vows.push(pools[url].end())
      delete pools[url]
    }
    return Promise.all(vows)
  }

  get poolKey() {
    const hash = new crypto.Hash('md5')
    hash.update(JSON.stringify(this.options))
    return hash.digest('base64')
  }

  getPool(log) {
    const key = this.poolKey
    if (key in pools) return pools[key]

    const poolOpts = Object.assign({connectionTimeoutMillis: 30000}, this.options)
    const p = new pg.Pool(poolOpts)
    p.on('error', e => log.error(e))
    pools[key] = p

    return p
  }

  async withConnection(log, fn) {
    var txnEnd
    var failures = []

    const idvow = this.genId()
    const pool = this.getPool(log)
    const dimensions = {host: this.options.host}
    const context = {}
    this.addCallsite(log, context)
    const info = (id, ms) => {
      const info = {clients: pool.totalCount, idle: pool.idleCount, waiting: pool.waitingCount}
      if (id !== undefined) info['connection-id'] = id
      if (ms !== undefined) info.ms = ms
      return Object.assign(info, context, dimensions)
    }

    const connectEnd = log.begin('pg.connect.duration')

    const retryOpts = Object.assign({randomize: true, maxTimeout: 8000}, this.options)
    const cvow = pretry(retryOpts, retry => {
      log.debug('connecting', info())
      return pool.connect().catch(err => {
        failures.push(err.message)
        retry(err)
      })
    }).catch(err => {
      const context = Object.assign(retryOpts, {attempts: failures.length, messages: failures})
      context.ms = connectEnd(info())
      throw log.fail('Failed to connect to database', err, context)
    })

    const [_id, _client] = await Promise.all([idvow, cvow])
    try {
      const ms = connectEnd(dimensions)
      log.debug('connected', info(_id, ms))
      log.count('pg.connect.retries', failures.length, dimensions)
      txnEnd = log.begin('pg.connection.duration')
      var result = await fn({_id, _client, _log: log})
    } finally {
      const ms = txnEnd(dimensions)
      _client.release()
      log.debug('disconnected', info(_id, ms))
    }

    return result
  }

  async disconnect() {
    const key = this.poolKey
    if (!(key in pools)) return
    const pool = pools[key]
    delete pools[key]
    await pool.end()
  }

  createMethod(name, meta, text) {
    if (meta.return === 'writestream') return this.createStreamMethod(name, meta, text, copyFrom)
    if (meta.return === 'readstream') return this.createStreamMethod(name, meta, text, copyTo)

    var fn
    switch (meta.return) {
      case 'value':
        fn = r => {
          for (let p in r.rows[0]) {
            return r.rows[0][p]
          }
        }
        break
      case 'row':
        fn = r => (r.rows[0] ? Object.assign({}, r.rows[0]) : r.rows[0])
        break
      case 'column':
        fn = r => {
          const name = r.fields[0].name
          return r.rows.map(ea => ea[name])
        }
        break
      case 'table':
        fn = r => r.rows.map(ea => (ea ? Object.assign({}, ea) : ea))
        break
      case undefined:
        fn = result => result
        break
      default:
        throw new Error('Unrecognized return kind', meta.return)
    }
    return this.createMethodWithCallback(name, meta, text, fn)
  }

  createStreamMethod(name, meta, text, wrap) {
    const {addCallsite, logQuery, options} = this

    const method = async function (...args) {
      if (args.length < 1) throw new Error('Stream queries require a callback function')
      const fn = args[args.length - 1]
      if (typeof fn !== 'function') throw new Error('The last argument must be a function')
      args = args.slice(0, -1)

      const queryEnd = this._log.begin('pg.query.duration')
      args = args.map(format)
      const context = Object.assign({arguments: args}, meta)
      addCallsite(this._log, context)
      Error.captureStackTrace(context, method)

      try {
        const result = await new Promise((resolve, reject) => {
          const stream = this._client.query(wrap(text), args)
          stream.on('error', reject)
          stream.on('end', resolve)
          return fn(stream)
        })

        const ms = queryEnd({host: options.host, query: name})
        logQuery(this, meta, context, ms)
        return result
      } catch (cause) {
        throw rebuildError(cause, context)
      }
    }

    return method
  }

  createMethodWithCallback(name, meta, text, extract) {
    const {addCallsite, logQuery, options} = this

    const method = async function(...args) {
      const queryEnd = this._log.begin('pg.query.duration')
      args = args.map(format)
      const context = Object.assign({arguments: args}, meta)
      addCallsite(this._log, context)
      Error.captureStackTrace(context, method)

      try {
        const result = await this._client.query(text, args)
        const ms = queryEnd({host: options.host, query: name})
        logQuery(this, meta, context, ms)
        return extract(result)
      } catch (cause) {
        throw rebuildError(cause, context)
      }
    }

    return method
  }

  createTxnMethod(sql) {
    return async function() {
      const context = {'connection-id': this._id, callsite: undefined}
      this._log.info(sql, context)
      try {
        return this._client.query(sql)
      } catch (cause) {
        throw rebuildError(cause, context)
      }
    }
  }
}

module.exports = PgStrategy

function format(v) {
  if (v === null || v === undefined) return null
  else if (v instanceof Array) return v.map(format)
  else if (v instanceof Buffer) return '\\x' + v.toString('hex')
  else if (typeof v.toSql === 'function') return format(v.toSql())
  else return v
}

function rebuildError(cause, context) {
  const error = new Error(cause.message)
  error.stack = context.stack.replace('Error\n', `Error: ${cause.message}`)

  for (let p in cause) {
    if (ignoreProperties.includes(p)) continue
    if (cause[p] === undefined) continue
    if (p === 'position') {
      error[p] = parseInt(cause[p])
    } else {
      error[p] = cause[p]
    }
  }

  for (let p in context) {
    if (p !== 'stack') {
      error[p] = context[p]
    }
  }

  return error
}
