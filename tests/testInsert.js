"use strict";
var test = require('tap').test;
var async = require('async');
var harness = require('./harness.js');

test('a bulk INSERT using hilo should not fail', function (t) {
	var items = [];
	for(var i = 0; i < 40001; i++) {
		items[i] = {
			created: new Date(),
			name: "This is a test"
		};
	}
	runTest(t, function (cb) {
		harness.mysqlSession.insert('tmp', items, function (err, result) {
			console.log(err);
			t.notOk(err, "no errors should be thrown on insert");
			cb();
		});
	});
});

/*
test('an INSERT using hilo should not fail', function (t) {
	var newItem = {
		created: new Date(),
		name: "This is a test"
	};
	runTest(t, function (cb) {
		harness.mysqlSession.insert('tmp', newItem, function (err, result) {
			console.log(err);
			t.notOk(err, "no errors should be thrown on insert");
			t.ok(result.insertId, "result.insertId should be passed back on insert");
			cb();
		});
	});
});

test('an INSERT supplying a UTC date should return that date when selected', function (t) {
	var now = new Date();
	var nowUtc = new Date(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate(), now.getUTCHours(),
		now.getUTCMinutes(), now.getUTCSeconds());

	var newItem = {
		created: nowUtc,
		name: "This is a test"
	};
	var newId = -1;

	runTest(t, function (cb) {
		async.series([
			function doInsert(seriesCb) {
				harness.mysqlSession.insert('tmp', newItem, function (err, result) {
					t.notOk(err, "no errors should be thrown on insert");
					t.ok(result.insertId, "result.insertId should be passed back on insert");
					newId = result.insertId;
					seriesCb();
				});
			},
			function checkInsert(seriesCb) {
				harness.mysqlSession.queryOne('SELECT * FROM tmp WHERE id = ?', [newId], function (selectErr, selectResult) {
					t.equal(selectResult.created.getTime(), nowUtc.getTime(), "created should equal nowUtc when selected back out");
					seriesCb();
				});
			},
			cb
		])
	});
});

test('INSERTs are not written inside transactions that have been rolled back', {skip:true}, function (t) {
	var newItem = {
		created: new Date(),
		name: "This is a test"
	};
	runTest(t, function (cb) {
		async.series([
			function (seriesCb) {
				harness.mysqlSession.startTransaction(seriesCb);
			},
			function test(seriesCb) {
				harness.mysqlSession.insert('tmp', newItem, seriesCb);
			},
			function checkInsert(seriesCb) {
				harness.mysqlSession.query('SELECT * FROM tmp', [], function (err, result1) {
					t.equal(result1.length, 1, "1 row should be inserted.");
					seriesCb();
				});
			},
			function (seriesCb) {
				harness.mysqlSession.rollback(seriesCb);
			},
			function checkInsert(seriesCb) {
				harness.mysqlSession.query('SELECT * FROM tmp', [], function (err, result2) {
					t.equal(result2.length, 0, "no rows should be inserted.");
					seriesCb();
				});
			},
			cb
		]);
	}, {tempTable: false});
});

test('bulk INSERT works', function (t) {
	var items = [];

	for (var i = 0; i < 100; i++)
		items[i] = {name: 'test ' + i, created: new Date()};

	runTest(t, function (cb) {
		harness.mysqlSession.insert('tmp', items, function (err, result) {
			t.notOk(err, "no errors should be thrown on bulk INSERT");
			t.equal(items.length, result.length, "The same number of rows INSERTed should be returned");
			cb();
		});
	});
});

test('INSERT returns insertId', function (t) {
	runTest(t, function (cb) {
		harness.mysqlSession.insert('tmp', {name: 'test ', created: new Date()}, function (err, result) {
			t.ok(result.insertId, "result.insertId should be passed back on insert");
			cb();
		});
	});
});
*/
function runTest(t, testFn, createTableOptions) {
	createTableOptions = createTableOptions || {tempTable: true};
	async.series([
		harness.connect,
		harness.dropTable,
		function(cb) {
			harness.createTable(createTableOptions, cb);
		},
		testFn,
		harness.dropTable,
		harness.disconnect
	], function(err,res) {
		t.end();
	});
}
