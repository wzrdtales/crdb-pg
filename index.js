'use strict';

let pg = require('pg');

function retry (savepoint = 'cockroach_restart') {
  this.query(`BEGIN; SAVEPOINT ${savepoint}`);

  let query = (...args) => {
    const cb = args[args.length - 1];

    if (typeof cb === 'function') {
      const retry = (err, res) => {
        if (err && err.code === '40001') {
          return this.client.query(`ROLLBACK TO SAVEPOINT ${savepoint}`, retry);
        }

        return cb(err, res);
      };
      args[args.length - 1] = retry;

      // call through
      this.query.apply(this, args);
    } else {
      return this.query.apply(this, args).catch(err => {
        if (err.code === '40001') {
          return this.client.query(`ROLLBACK TO SAVEPOINT ${savepoint}`);
        }

        return Promise.reject(err);
      });
    }
  };

  return { query };
}

module.exports = class CRDB {
  constructor (config) {
    this._config = { ...config };

    if (this._config.native) {
      pg = pg.native;
    }
  }

  connect () {
    this._discover();
    let connection = new pg.Client(this._config);
    connection.connect();
    connection.retry = retry;
    return connection;
  }

  pool () {
    this._discover();
    let pool = new pg.Pool(this._config);
    pool.retry = retry;
    pool._realConnect = pool.connect;

    pool.connect = function (...args) {
      if (typeof args[0] === 'function') {
        pool._realConnect((err, client, release) => {
          client.retry = retry;
          args[0](err, client, release);
        });
      } else {
        return pool._realConnect.apply(pool, args).then(client => {
          client.retry = retry;
          return client;
        });
      }
    };
    return pool;
  }

  _discover () {
    if (this._config.discovery) {
    }
  }
};

module.exports = CRDB;
