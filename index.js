'use strict';

let pg = require('pg');
const SQL = require('sql-template-strings');

async function retry (callQueries) {
  const client = await this.connect();
  async function handleError (err) {
    if (err.code === '40001') {
      await client.query(SQL`ROLLBACK TO SAVEPOINT cockroach_restart`);
      return exec();
    }

    await client.query(SQL`ROLLBACK`);
    throw err;
  }
  async function exec () {
    const result = await callQueries(client);
    await client.query(SQL`RELEASE SAVEPOINT cockroach_restart`);
    await client.query(SQL`COMMIT`);
    return result;
  }

  try {
    await client.query(SQL`BEGIN; SAVEPOINT cockroach_restart`);
    const res = await exec();
    client.release();
    return res;
  } catch (err) {
    const res = await handleError(err);
    client.release();
    return res;
  }
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
    pool.retry = retry; // patch in retry

    return pool;
  }

  _discover () {
    if (this._config.discovery) {
    }
  }
};
