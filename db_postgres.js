const { Pool } = require('pg'); //a Postgres Connection Pool
var connPool = null; //new approach REQUIRES a call to connect before using query
var types = require('pg').types
/*
 * Fix date display error.
 * Simply by adding a function to return the raw value, we
 * override the pg_postgres default behavior, which mucks
 * date-only values by ‘correcting’ for local TZ. This is
 * confusing, because pg_postgres lives on the server, whose
 * TZ is UTC. It may be that moment(), running on the client
 * cannot properly process dates that contain an explicit
 * UTC TZ indicator with time set to 00:00, causing a correction
 * for TZ EST by subtracting 5 hours from midnight. In theory,
 * this would set the UI time back to the previous evening at 7P.
 *
 * pg_types uses postgres OIDs to identify db data types:
 *
 * date OID=1082
 * timestamp OID=1114
 *
*/
parseDate = function(val) {
  //NOTE: this log is hit 2x per row. HUGE API performance hit.
  //console.log('db_postgres.parseDate', val);
  return val;
}

types.setTypeParser(1082, parseDate);

/*
  This is it.
 */
module.exports = {

  connect: async (db_params) => {
    console.log(`db_postgres.js::connect()`, db_params);

    connPool = await new Pool(db_params);

    //NEW: test the connection and return a promise.
    return new Promise((resolve, reject) => {
      connPool.connect((err, client, release) => {
        if (err) {
          reject('Error acquiring DB client')
        } else {
          resolve('DB Connection Success')
        }
        release();
      })
    })
  },

  close: async () => {
    return await connPool.end();
  },

  test: () => {
    connPool.query('SELECT version();')//test the connection by querying database version
      .then(res => {
        console.log('Test database connection by checking version:', res.rows[0]);
      })
      .catch(err => {
        console.log(err);
      });

  },

  query: (text, params) => {
    if (connPool) {
      return connPool.query(text, params);
    } else {
      console.log('VAL_Utilities/db_postgres.js::query | ERROR: No db connection, connPool is NULL | call db_postgres::connect')
    }
  }

};
