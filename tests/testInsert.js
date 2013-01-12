"use strict";
var test = require('tap').test;
var async = require('async');
var mysqlUtil = require('../');
var mysqlSession;

test('an insert using hilo should not fail', function (t) {
	var newItem = {
		created: new Date(),
		name: "This is a test"
	};
	async.series([
		setup,
		function test(seriesCb) {
			mysqlSession.insert('tmp', newItem, function (err, result) {
				t.notOk(err, "no errors should be thrown on insert");
				t.ok(result.insertId, "result.insertId should be passed back on insert");
				seriesCb();
			});
		},
		function (cb) {
			tearDown(t);
			cb();
		}
	]);
});

test('an insert supplying a UTC date should return that date when selected', function (t) {
	var now = new Date();
	var nowUtc = new Date(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate(), now.getUTCHours(),
		now.getUTCMinutes(), now.getUTCSeconds());

	var newItem = {
		created: nowUtc,
		name: "This is a test"
	};
	var newId = -1;

	async.series([
		setup,
		function doInsert(seriesCb) {
			mysqlSession.insert('tmp', newItem, function (err, result) {
				t.notOk(err, "no errors should be thrown on insert");
				t.ok(result.insertId, "result.insertId should be passed back on insert");
				newId = result.insertId;
				seriesCb();
			});
		},
		function checkInsert(seriesCb) {
			mysqlSession.queryOne('SELECT * FROM tmp WHERE id = ?', [newId], function (selectErr, selectResult) {
				t.equal(selectResult.created.getTime(), nowUtc.getTime(), "created should equal nowUtc when selected back out");
				seriesCb();
			});
		},
		function (cb) {
			tearDown(t);
			cb();
		}
	]);
});

test('inserts are not written inside transactions that have been rolled back', {skip: true}, function (t) {
	var newItem = {
		created: new Date(),
		name: "This is a test"
	};
	async.series([
		setup,
		function (seriesCb) {
			mysqlSession.startTransaction(seriesCb);
		},
		function test(seriesCb) {
			mysqlSession.insert('tmp', newItem, function (err, result) {
				t.notOk(err, "no errors should be thrown on insert");
				t.ok(result.insertId, "result.insertId should be passed back on insert");
				return seriesCb();
			});
		},
		function checkInsert(seriesCb) {
			mysqlSession.query('SELECT * FROM tmp', [], function (err, result) {
				t.equal(result.length, 1, "1 row should be inserted.");
				seriesCb();
			});
		},
		function (seriesCb) {
			mysqlSession.rollback(seriesCb);
		},
		function checkInsert(seriesCb) {
			mysqlSession.query('SELECT * FROM tmp', [], function (err, result) {
				t.equal(result.length, 0, "no rows should be inserted.");
				seriesCb();
			});
		},
		function (cb) {
			tearDown(t);
			cb();
		}
	]);
});

test('a bulk insert should be awesome', function (t) {
	var items = [];

	for (var i = 0; i < 100; i++)
		items[i] = {name: 'test ' + i, created: new Date()};

	async.series([
		setup,
		function test(seriesCb) {
			mysqlSession.insert('tmp', items, function (err, result) {
				t.notOk(err, "no errors should be thrown on insert");
				console.log(err, result);
				t.equal(items.length, result.affectedRows, "Expect to affect the same number of rows you insert");
				seriesCb();
			});
		},
		function (cb) {
			tearDown(t);
			cb();
		}
	]);
});

function setup(cb) {
	async.series([
		connect,
		dropTable,
		createTable
	], cb);
}
function tearDown(t) {
	async.series([
		dropTable,
		disconnect,
		function (cb) {
			t.end();
			cb();
		}
	]);
}

function createTable(cb) {
	var sql = [
		'CREATE TABLE IF NOT EXISTS tmp (',
		'id BIGINT(20) NOT NULL,',
		'created datetime NOT NULL,',
		'name varchar(150) COLLATE utf8_unicode_ci NOT NULL,',
		'PRIMARY KEY (id)',
		') ENGINE=InnoDB DEFAULT CHARSET=utf8'
	].join('\n');
	mysqlSession.query(sql, [], function (err, result) {
		cb(err, result);
	});
}

function dropTable(cb) {
	var sql = "DROP TABLE IF EXISTS tmp;";
	mysqlSession.query(sql, [], function (err, result) {
		cb(err, result);
	});
}

function connect(cb) {
	mysqlUtil.connect({
		host: process.env.MYSQL_HOST,
		user: process.env.MYSQL_USER,
		password: process.env.MYSQL_PASSWORD,
		database: process.env.MYSQL_DATABASE
	}, function (err, session) {
		mysqlSession = session;
		cb();
	});
}

function disconnect(cb) {
	mysqlSession.disconnect(cb);
}