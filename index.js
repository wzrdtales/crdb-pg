'use strict';

let pg = require('pg');
const fs = require('fs');
const SQL = require('sql-template-strings');

async function retry (callQueries, limit = 11) {
  const client = await this.connect();
  let counter = 0;
  let running = true;
  async function handleError (err) {
    if (err.code === '40001' && counter < limit) {
      await client.query(SQL`ROLLBACK TO SAVEPOINT cockroach_restart`);
      return true;
    }

    running = false;
    await client.query(SQL`ROLLBACK`);
    throw err;
  }

  async function abort () {
    running = false;
    return client.query(SQL`ROLLBACK`);
  }

  async function exec () {
    const result = await callQueries(client, { abort });
    if (running) {
      await client.query(SQL`RELEASE SAVEPOINT cockroach_restart`);
      await client.query(SQL`COMMIT`);
      running = false;
    }
    return result;
  }

  await client.query('BEGIN; SAVEPOINT cockroach_restart');
  while (running) {
    try {
      ++counter;
      const res = await exec();
      return res;
    } catch (err) {
      await handleError(err);
    } finally {
      if(!running) {
        client.release();
      }
    }
  }
}

module.exports = class CRDB {
  constructor (config) {
    this._config = JSON.parse(JSON.stringify(config));

    if (this._config.native) {
      pg = pg.native;
    } else if (this._config.ssl?.sslmode) {
      if (this._config.ssl.sslrootcert) this._config.ssl.ca = fs.readFileSync(this._config.ssl.sslrootcert).toString();
      if (this._config.ssl.sslcert) this._config.ssl.cert = fs.readFileSync(this._config.ssl.sslcert).toString();
      if (this._config.ssl.sslkey) this._config.ssl.key = fs.readFileSync(this._config.ssl.sslkey).toString();
    }
  }

  connect () {
    this._discover();
    const connection = new pg.Client(this._config);
    connection.connect();
    connection.retry = retry;
    return connection;
  }

  pool () {
    this._discover();
    const pool = new pg.Pool(this._config);
    pool.retry = retry; // patch in retry

    return pool;
  }

  _discover () {
    if (this._config.discovery) {
    }
  }
};

