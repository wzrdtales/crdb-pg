'use strict';

let pg = require('pg');
const SQL = require('sql-template-strings');

async function retry(callQueries, limit = 11) {
  const client = await this.connect();
  let counter = 0;
  let running = true;
  async function handleError(err) {
    if (err.code === '40001' && counter < limit) {
      await client.query(SQL`ROLLBACK TO SAVEPOINT cockroach_restart`);
      return true;
    }

    running = false;
    await client.query(SQL`ROLLBACK`);
    throw err;
  }

  async function abort() {
    running = false;
    return client.query(SQL`ROLLBACK`);
  }

  async function exec() {
    const result = await callQueries(client, { abort });
    if (running) {
      await client.query(SQL`RELEASE SAVEPOINT cockroach_restart`);
      await client.query(SQL`COMMIT`);
    }
    return result;
  }

  await client.query('BEGIN; SAVEPOINT cockroach_restart');
  while (running) {
    try {
      ++counter;
      const res = await exec();
      client.release();
      return res;
    } catch (err) {
      await handleError(err);
    }
  }
}

module.exports = class CRDB {
  constructor(config) {
    this._config = { ...config };

    if (this._config.native) {
      pg = pg.native;
    }
  }

  connect() {
    this._discover();
    let connection = new pg.Client(this._config);
    connection.connect();
    connection.retry = retry;
    return connection;
  }

  pool() {
    this._discover();
    let pool = new pg.Pool(this._config);
    pool.retry = retry; // patch in retry

    return pool;
  }

  _discover() {
    if (this._config.discovery) {
    }
  }
};
