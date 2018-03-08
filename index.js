const
  pg  = require ('pg'),
  R   = require ('ramda'),
  T   = require ('data.task'),

  getPool = function (conf) {
    return new pg.Pool(conf);
  },

  defaultHandler = R.curry (function (reject, resolve, err, data) {
    if (err) { return reject (err) }
    return resolve (data);
  }),

  endConnection = R.curry (function (client, f, x) {
    client.end (); return f (x);
  }),

  query = R.curry (function (confOrClient, sql, params) {
    return new T (function (reject, resolve) {
      let
        client,
        isConnected = false,
        isClient    = false;

      // Attempt to reuse client
      if (typeof confOrClient.connect === 'function') {
        client = confOrClient;
        isConnected = confOrClient._connected;
        isClient = true;
      } else {
        client = new pg.Client (confOrClient);
      }

      const handler = defaultHandler (endConnection (client, reject), endConnection (client, resolve));
      const ROLLBACK = ' ROLLBACK; ';

      if (isConnected) {
        client.query (sql, params, function (error, results) {
          if (error) { client.query (ROLLBACK); return reject (error); }
          // pg returns 'undefined' for query errors instead of an empty array, this fixes it
          results1 = (typeof results == 'undefined') ? [] : R.prop ('rows', results)
          return resolve (results1);
        })
      }
      else {
        client.connect (function (err) {

          if (err) { client.query (ROLLBACK); return reject (err); }

          client.query (sql, params, function (error, results) {
            if (error) { client.query (ROLLBACK); return reject (error); }
            // pg returns 'undefined' for query errors instead of an empty array, this fixes it
            results1 = (typeof results == 'undefined') ? [] : R.prop ('rows', results)

            if (isClient === true) {
              if (error) { return reject (error); }
              return resolve (results);
            } else {
              return handler (error, results1)
            }
          })
        })
      }
    })
  }),

  client = pg.Client,

  nil = null;

module.exports = {
  query : query,
  getPool: getPool,
  client: client
}
