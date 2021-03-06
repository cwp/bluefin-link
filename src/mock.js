'use strict'

const BaseStrategy = require('./base')

class MockStrategy extends BaseStrategy {
  constructor(options, mocks) {
    super(options)
    this.mocks = mocks
  }

  async withConnection(jot, fn) {
    const connecting = jot.start('mock.connect', {host: this.options.host})
    try {
      const _id = await this.genId()
      connecting.count('mock.connect.retries', 0)
      connecting.finish()
      var connected = jot.start('mock.connection', {host: this.options.host})
      return fn({_id, _log: connected})
    } finally {
      connected.finish()
    }
  }

  createMethod(name, meta, text) {
    const checkResult = this.createCheckResultFn(name, meta)
    const {options, mocks} = this

    const method = function(...args) {
      const context = {arguments: args, host: options.host, query: name}
      Object.assign(context, meta)
      const querying = this._log.start('mock.query', context)
      Error.captureStackTrace(context, method)

      // make sure we have a mock for this query
      if (!(name in mocks)) {
        throw new Error(`no mock for method ${name}`)
      }

      const mock = mocks[name]

      return new Promise((resolve, reject) => {
        process.nextTick(() => {
          let result = mock
          querying.finish()
          if (typeof mock === 'function') {
            try {
              result = mock.apply(this, args)
            } catch (cause) {
              const effect = wrap(`mock ${name}() threw error`, cause, context)
              return reject(effect)
            }
          }
          checkResult(this._log, result, context, resolve, reject)
        })
      })
    }
    return method
  }

  createCheckResultFn(name, meta) {
    return function(log, result, context, resolve, reject) {
      const fail = msg => {
        const error = wrap(msg, undefined, context, {result})
        reject(error)
      }

      if (meta.return === 'table') {
        if (!(result instanceof Array)) {
          return fail('mock does not return a table')
        }
        for (let ea of result) {
          if (typeof ea !== 'object') {
            return fail('mock does not return rows')
          }
        }
      } else if (meta.return === 'row') {
        // if no rows were returned, undefined is legit
        if (result === undefined) return resolve(result)

        // if we're looking for row, null is not a valid value
        if (result === null) return fail('mock returns null, not a row')

        // if it's some scalar value, that's also wrong
        if (typeof result !== 'object') {
          return fail('mock does not return a row')
        }

        // check columns
        if (Object.keys(result).length < 1) {
          return fail('mock row should have at least one column')
        }
      } else if (meta.return === 'column') {
        // if no rows were returned, undefined is legit
        if (result === undefined) return resolve(result)

        // null is not a valid value
        if (result === null) return fail('mock returns null, not a column')

        // if it's some scalar value, that's also wrong
        if (!Array.isArray(result)) {
          return fail('mock does not return a column')
        }
      }

      // if we made it here, the mock is fine
      return resolve(result)
    }
  }

  createTxnMethod(sql) {
    return function() {
      this._log.info(sql, {'connection-id': this._id})
      return new Promise((resolve, reject) => {
        process.nextTick(() => {
          resolve()
        })
      })
    }
  }
}

module.exports = MockStrategy

const wrap = (message, cause, context, ...rest) => {
  const effect = new Error(message)
  Object.assign(effect, context, ...rest)
  if ('stack' in context)
    effect.stack = context.stack.replace(/Error:?\s*\n/, `Error: ${message}\n`)
  Object.defineProperty(effect, 'cause', {value: cause, enumerable: false})
  return effect
}
