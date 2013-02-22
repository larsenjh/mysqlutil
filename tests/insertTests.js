"use strict";
var _ = require('underscore');
var test = require('tap').test;
var async = require('async');
var insertModes = require('../util/insertModes.js');
var harness = require('./helpers/harness.js');
var dateHelper = require('../util/dateHelper.js');

test('a bulk insert using hilo works', function (t) {
	var items = generateTestItems(40001);
	runTest(t, function (cb) {
		harness.db.insert('tmp', items, function (err, result) {
			t.notOk(err, "no errors were thrown on bulk insert");
			t.equal(items.length, result.length, "all inserted items were returned");
			cb();
		});
	});
});

test('an insert using hilo works', function (t) {
	var newItem = { created: new Date(), name: "This is a test" };
	runTest(t, function (cb) {
		harness.db.insert('tmp', newItem, function (err, result) {
			t.notOk(err, "no errors were thrown on insert, got: "+err);
			cb();
		});
	});
});

test('an insert supplied with a UTC date persists that date without altering it', function (t) {
	var nowUtc = dateHelper.utcNow();
	var newItem = {created: nowUtc, name: "This is a test"};
	var newId = -1;

	runTest(t, function (cb) {
		async.series([
			function doInsert(seriesCb) {
				harness.db.insert('tmp', newItem, function (err, result) {
					t.notOk(err, "no errors were thrown on insert, got: "+err);
					t.ok(result.insertId, "insertId was passed back after insert");
					newId = result.insertId;
					seriesCb();
				});
			},
			function checkInsert(seriesCb) {
				harness.db.queryOne('SELECT * FROM tmp WHERE id = ?', [newId], function (selectErr, selectResult) {
					t.equal(Date.parse(selectResult.created.toString()), Date.parse(nowUtc.toString()), "utc date was inserted without alterations");
					seriesCb();
				});
			}
		], cb)
	});
});

test('inserts are not written within transactions that have been rolled back', {skip: true}, function (t) {
	var newItem = {created: new Date(), name: "This is a test"};
	runTest(t, function (cb) {
		async.series([
			function (seriesCb) {
				harness.db.startTransaction(seriesCb);
			},
			function test(seriesCb) {
				harness.db.insert('tmp', newItem, seriesCb);
			},
			function checkInsert(seriesCb) {
				harness.db.query('SELECT * FROM tmp', [], function (err, result1) {
					t.equal(result1.length, 1, "a row was inserted.");
					seriesCb();
				});
			},
			function (seriesCb) {
				harness.db.rollback(seriesCb);
			},
			function checkInsert(seriesCb) {
				harness.db.query('SELECT * FROM tmp', [], function (err, result2) {
					t.equal(result2.length, 0, "no rows were inserted after the transaction was rolled back.");
					seriesCb();
				});
			},
			cb
		]);
	}, {tempTable: false});
});

test('an insert returns an insertId', function (t) {
	runTest(t, function (cb) {
		harness.db.insert('tmp', {name: 'test ', created: new Date()}, function (err, result) {
			t.ok(result.insertId, "insertId was passed back after insert");
			cb();
		});
	});
});

test('a bulk insert returns an insertId per inserted item', function (t) {
	var items = generateTestItems(1000);
	runTest(t, function (cb) {
		var insertIdMissing = false;
		harness.db.insert('tmp', items, function (err, results) {
			_.each(results, function (result) {
				if (!insertIdMissing && !result.insertId)
					insertIdMissing = true;
			});
			t.notOk(insertIdMissing, "an insertId was passed back for each result after insert");
			cb();
		});
	});
});

function generateTestItems(amt) {
	var items = [];
	for (var i = 0; i < amt; i++)
		items[i] = {name: 'test ' + i, created: dateHelper.utcNow()};
	return items;
}

function runTest(t, testFn, createTableOptions) {
	createTableOptions = createTableOptions || {tempTable: true};
	async.series([
		harness.connect,
		harness.dropTable,
		function (cb) {
			harness.createTable(createTableOptions, cb);
		},
		testFn,
		harness.dropTable,
		harness.disconnect
	], function (err, res) {
		t.end();
	});
}
