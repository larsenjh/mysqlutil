"use strict";
var _ = require('lodash');
var async = require('async');
var mysql = require('mysql');

var insertModes = require('./insertModes.js');
var updateBuilder = require('./updateBuilder.js');
var insertBuilder = require('./insertBuilder.js');
var deadlockRetry = require('./deadlockRetry.js');

var concurrencyLimit = 2;
var bulkInsertBatchSize = 1000;
var hiLoBatchSize = 101;
var defaultMaxLo = hiLoBatchSize - 1;
var minBatchCount = 100;

module.exports = function (settings) {
	var id = -1;
	var pool = createPool();
	var openTransaction = false;
	var hiloRef = hilo();

	function createPool() {
		var pooling;
		if(settings.cluster) {
			pooling = mysql.createPoolCluster();
			_.each(settings.cluster.nodes, function(node) {
				pooling.add(node.name, node);
			});
		} else {
			pooling = mysql.createPool(settings);
		}
		return pooling;
	}
	
	function preFillPool(cb) {
		if(settings.cluster) {
			async.each(settings.cluster.nodes,
				function(node, iCb) {
					async.each(
						new Array(node.amtToPrefill + 1).join(0).split(''),
						function (i, eCb) {
							query("SELECT 1", null, eCb, {
								useClusteredNode: node.name
							});
						}, iCb);
				}, cb);
		} else {
			async.each(
				new Array(settings.amtToPrefill + 1).join(0).split(''),
				function (i, eCb) {
					query("SELECT 1", null, eCb);
				}, cb);
		}
	}

	function addPingToConnection(conn) {
		conn.pingIntervalId = setInterval(function () {
			if (settings.debugging.ping)
				console.log('conn', conn.id, 'ping');
			conn.ping.apply(conn);
		}, settings.connectionPingIntervalSeconds * 1000);

		conn.on('error', function (err) {
			clearInterval(conn.pingIntervalId);
			if (settings.debugging.connectionError)
				console.error('conn', conn.id, err);
			throw err;
		});

		conn.on('end', function (err) {
			if (settings.debugging.connectionEnd)
				console.log('conn', conn.id, 'end, err', err);
			clearInterval(conn.pingIntervalId);
		});
	}

	function getConnection(cb, options) {
		options = options || {};

		if (settings.debugging.poolPerf)
			console.time('Get connection from pool');

		var getConn = pool.getConnection;
		// accept optional parameters to use a cluster node name, iff cluster has been established
		// note: supplying no options or default when using a cluster will default to ALL nodes using round robin
		if(settings.cluster) {
			var node = options.useClusteredNode || settings.cluster.defaultNode;
			var order = options.useClusteredOrder || settings.cluster.defaultOrder;
			if(node) {
				getConn = _.partial(getConn, node);
				if (order) {
					getConn = _.partial(getConn, order);
				}
			}
		}

		getConn.call(pool, (function (err, conn) {
			if (settings.debugging.poolPerf)
				console.timeEnd('Get connection from pool');

			if (err) return cb(err);
			if (!conn) return cb(new Error('Could not get a connection from the pool'));

			if (!conn.id)
				conn.id = ++id;
			if (!conn.pingIntervalId)
				addPingToConnection(conn);

			return cb(null, conn);
		}));
	}

	function query(sql, queryParams, queryCb, options) {
		options = options || {};

		if (_.isFunction(queryParams) && !queryCb) {
			queryCb = queryParams;
			queryParams = null;
		}
		queryCb = queryCb || function () {
		};

		if(options.useConnection) {
			queryWithConnection(options.useConnection, sql, queryParams, queryCb, options);
		} else {
			getConnection(function (err, conn) {
				if (err) return queryCb(err);
				queryWithConnection(conn, sql, queryParams, queryCb, options);
			}, options);
		}
	}
	
	function queryWithConnection(conn, sql, queryParams, queryCb, options) {
		options = options || {};

 		var queryStart;
		if (settings.debugging.queryPerf)
			queryStart = Date.now();

		conn.query(sql, queryParams, function (err, result) {
			if (settings.debugging.queryPerf) {
				var queryEnd = (Date.now() - queryStart) / 1000;
				if (queryEnd > settings.debugging.queryPerfSlowQueryThresholdSec)
					console.log('Slow query (' + queryEnd + ' s), conn.id', conn.id, 'Query:', sql);
				else
					console.log('Query (' + queryEnd + ' s), conn.id', conn.id);
			}

			if(!options.useConnection)
				conn.release();

			if (err) {
				err.result = result;
				err.sql = sql;
				err.queryParams = queryParams;
				result = [];
			}

			if (result) {
				if (!_.isArray(result) && result.columnLength === 0)
					result = [];
			}

			if (settings.debugging.queryResult)
				console.log('conn:', conn.id, 'query:', sql, 'params:', queryParams, 'err:', err, 'result:', result);

			queryCb(err, result);
		});
	}

	function queryWithDeadlockRetry(sql, queryParams, retryAmount, queryCb, options) {
		deadlockRetry({
			fn: query,
			args: [sql, queryParams],
			options: options,
			retryAmount: retryAmount
		}, queryCb);
	}

	function insert(tableName, items, insertCb, options) {
		items = _.isArray(items) ? items : [items];
		options = _.defaults(options || {}, {
			insertMode: obj.defaultInsertMode,
			enforceRules: true,
			ignore: false,
			upsert: false
		});

		var idx = 0;
		var insertedItems = [];
		async.whilst(
			function () {
				return idx < items.length;
			},
			function (wCb) {
				var chunk = items.slice(idx, idx + bulkInsertBatchSize);
				_insert(tableName, chunk, function (err, result) {
					if (err) return wCb(err);
					insertedItems = insertedItems.concat(result);
					idx += bulkInsertBatchSize;
					wCb();
				}, options);
			},
			function (err) {
				insertCb(err, insertedItems);
			}
		);
	}

	function _insert(tableName, items, insertCb, options) {
		async.each(items,
			function (item, eachCb) {
				if (options.insertMode !== insertModes.hilo)
					return eachCb();

				hiloRef.computeNextKey(obj, function hiloCb(nextKey) {
					item.$insertId = item[hiloRef.keyName] = nextKey;
					eachCb();
				});
			},
			_.partial(insertItems, options)
		);

		function insertItems(options, err) {
			if (err) return insertCb(err);
			insertBuilder({
				upsert: options.upsert,
				ignore: options.ignore,
				tableName: tableName,
				items: items,
				insertRules: options.enforceRules ? obj.insertRules : null,
				updateRules: options.enforceRules ? obj.updateRules : null
			}, function (err, insertInfo) {
				if (err) return insertCb(err);

				queryWithDeadlockRetry(insertInfo.sql, insertInfo.values, 5, function (err, result) {
					if (err) return insertCb(err);

					// InnoDB guarantees sequential numbers for AUTO INCREMENT when doing bulk inserts
					var startingInsertId = result.insertId - 1;

					// $insertId -> insertId
					items = _.map(items, function (item) {
						if (item.$insertId) {
							item.insertId = item.$insertId;
							delete item.$insertId;
						}
						else if (options.insertMode === insertModes.identity) {
							item.insertId = ++startingInsertId;
						}
						return item;
					});
					insertCb(err, items);
				}, options);
			});
		}
	}

	function update(tableName, items, updateCb, options) {
		items = _.isArray(items) ? items : [items];
		options = _.defaults(options || {}, {
			enforceRules: true
		});

		var stack = [];

		async.eachLimit(items, concurrencyLimit,
			function (item, eachCb) {
				updateBuilder({
					item: item,
					tableName: tableName,
					rules: options.enforceRules ? obj.updateRules : null,
					defaultKeyName: obj.defaultKeyName
				}, function (err, updateInfo) {
					if (err) return updateCb(err);

					queryWithDeadlockRetry(updateInfo.sql, updateInfo.values, 5, function (err, result) {
						if (err) return updateCb(err);

						stack.push(result);
						eachCb();
					}, options);
				});
			},
			function finalEachCb(err) {
				if (err) updateCb(err);
				updateCb(err, stack.length > 1 ? stack : stack[0]);
			}
		);
	}

	function reserveHiLoIds(amount, cb) {
		var batchesCount = Math.floor(amount / hiLoBatchSize);
		if (amount % hiLoBatchSize != 0) {
			batchesCount++;
		}
		obj.queryOne('CALL getNextHi(?);', [batchesCount], function (err, dbResult) {
			if (err) return cb(err);
			var lo = dbResult[0].NextHi == 0 ? 1 : 0;
			var hi = dbResult[0].NextHi * (defaultMaxLo + 1);
			var startId = hi + lo;
			cb(null, startId);
		});
	}

	function hilo() {
		var maxLo = defaultMaxLo;
		var lo = maxLo + 1;
		var hi = 0;

		var deferredCallbacks = [];
		var queryPending = false;

		return {
			keyName: 'id',
			type: 'hilo',
			computedKey: true,
			computeNextKey: function computeNextKey(mysql, cb) {
				if (lo <= maxLo) {
					var result = hi + lo;
					lo++;
					if (settings.debugging.hilo)
						console.log('Handing out id ' + result);
					return cb(result);
				}

				deferredCallbacks.push(cb);
				if (settings.debugging.hilo)
					console.log('Deferring while waiting for a new ID', deferredCallbacks.length, queryPending);

				if (!queryPending) {
					queryPending = true;
					mysql.queryOne('CALL getNextHi(?)', [minBatchCount], function (err, result) {
						if (err) return cb(err);

						var hival = result[0].NextHi;
						if (settings.debugging.hilo)
							console.log('New id range', hival);

						lo = hival == 0 ? 1 : 0;
						hi = hival * (defaultMaxLo + 1);
						maxLo = minBatchCount * hiLoBatchSize - 1;

						queryPending = false;

						var runnableCallbacks = deferredCallbacks;
						deferredCallbacks = [];
						if (settings.debugging.hilo)
							console.log('Running deferred', runnableCallbacks.length);
						_.each(runnableCallbacks, function (cb) {
							computeNextKey(mysql, cb);
						});
					});
				}

			}
		};
	}
	
	function startTransaction(cb, options) {
		if(!options || !options.useConnection) return cb(new Error("transaction calls must supply an existing connection"));
		query("START TRANSACTION", function (err, res) {
			if (err) {
				openTransaction = false;
				return cb(err);
			}
			openTransaction = true;
			cb();
		}, options);
	}

	function commit(cb, options) {
		if(!options || !options.useConnection) return cb(new Error("transaction calls must supply an existing connection"));
		query("COMMIT", function (err, res) {
			if (err) return cb(err);
			openTransaction = false;
			cb();
		}, options);
	}

	function rollback(cb, options) {
		if(!options || !options.useConnection) return cb(new Error("transaction calls must supply an existing connection"));
		query("ROLLBACK", function (err, res) {
			if (err) return cb(err);
			openTransaction = false;
			cb();
		}, options);
	}

	var obj = {
		defaultInsertMode: insertModes.hilo,
		defaultKeyName: 'id',

		insertRules: [],
		updateRules: [],

		query: function (sql, queryParams, cb, options) {
			query(sql, queryParams, cb, options);
		},
		queryWithDeadlockRetry: function (sql, queryParams, retryAmount, cb, options) {
			queryWithDeadlockRetry(sql, queryParams, retryAmount, cb, options);
		},
		getConnection: getConnection,
		queryOne: function (sql, queryParams, cb, options) {
			if (_.isFunction(queryParams) && !cb) {
				cb = queryParams;
				queryParams = null;
			}
			cb = cb || function () {
			};

			query(sql, queryParams, function (err, result) {
				result = _.isEmpty(result) ? null : result[0];
				cb(err, result)
			}, options);
		},
		insert: insert,
		update: update,
		upsert: function (tableName, items, insertCb, options) {
			options = options || {};
			options.upsert = true;
			insert(tableName, items, insertCb, options);
		},

		startTransaction: startTransaction,
		commit: commit,
		rollback: rollback,

		disconnect: function (cb) {
			if (!pool) return cb();
			pool.end(cb);
			// current version of mysql PoolCluster does not invoke cb
			if(settings.cluster) cb();
		}, // for tests

		logging: false,
		preFillPool: preFillPool,
		reserveHiLoIds: reserveHiLoIds
	};
	return obj;
};
