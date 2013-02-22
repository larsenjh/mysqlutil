"use strict";
var _ = require('underscore');
var test = require('tap').test;
var async = require('async');
var insertModes = require('../util/insertModes.js');
var harness = require('./helpers/harness.js');
var dateHelper = require('../util/dateHelper.js');

test('a simple upsert works', function (t) {
	var item = generateTestItems(1)[0];

	t.test('Sets up test', function (t) {
		setup(function (err, res) {
			t.end();
		});
	});

	t.test('Creates test item', function (t) {
		harness.db.insert('tmp', item, function (err, result) {
			t.notOk(err, "no errors were thrown on insert, got: " + err);
			t.end();
		}, {insertMode: insertModes.custom});
	});

	t.test('Upsert test item', function (t) {
		item = _.extend(item, {
			name: 'Test Name Updated'
		});
		delete item.insertId;

		harness.db.upsert('tmp', item, function (err, result) {
			t.notOk(err, "no errors were thrown on upsert, got: " + err);

			harness.db.query("SELECT name FROM tmp WHERE id = ?", [item.id], function (err, res) {
				t.equal(item.name, res[0].name, "upsert updated test item's name");
				t.end();
			});
		}, {
			insertMode: insertModes.custom
		});
	});

	t.test('Tears down test', function (t) {
		tearDown(function (err, res) {
			t.end();
		});
	});
});

test('a multiple upsert works', function (t) {
	var items = generateTestItems(5);

	t.test('Sets up test', function (t) {
		setup(function (err, res) {
			t.end();
		});
	});

	t.test('Creates test item', function (t) {
		harness.db.insert('tmp', items, function (err, result) {
			t.notOk(err, "no errors were thrown on insert, got: " + err);
			t.end();
		}, {insertMode: insertModes.custom});
	});

	t.test('Upsert test item', function (t) {
		var newName = 'Test Name Updated';
		items = _.map(items, function (item) {
			delete item.insertId;
			item.name = newName;
			return item;
		});
		harness.db.insert('tmp', items, function (err, result) {
			t.notOk(err, "no errors were thrown on upsert, got: " + err);

			harness.db.upsert("SELECT name FROM tmp", [], function (err, res) {
				_.each(res, function (newItem) {
					t.equal(newItem.name, newName, "upsert updated test item's name");
				});
				t.end();
			});
		}, {
			insertMode: insertModes.custom
		});
	});

	t.test('Tears down test', function (t) {
		tearDown(function (err, res) {
			t.end();
		});
	});
});

function generateTestItems(amt) {
	var items = [];
	for (var i = 0; i < amt; i++)
		items[i] = {id: i, name: 'test ' + i, created: dateHelper.utcNow()};
	return items;
}

function setup(cb, createTableOptions) {
	createTableOptions = createTableOptions || {tempTable: true};
	async.series([
		harness.connect,
		harness.dropTable,
		function (cb) {
			harness.createTable(createTableOptions, cb);
		}
	], cb);
}

function tearDown(cb) {
	async.series([
		harness.dropTable,
		harness.disconnect
	], cb);
}
