"use strict";
var _ = require('underscore');
var test = require('tap').test;
var async = require('async');
var insertModes = require('../util/insertModes.js');
var harness = require('./helpers/harness.js');
var dateHelper = require('../util/dateHelper.js');

test("Connects to the database", function (t) {
	harness.connect(function (err, res) {
		t.end();
	});
});

test('a simple update works', function (t) {
	var item = generateTestItems(1)[0];

	t.test('Setup', setup);

	t.test('Creates test item', function(t) {
		harness.db.insert('tmp', item, function (err, result) {
			t.notOk(err, "no errors were thrown on insert");
			t.end();
		}, {insertMode:insertModes.custom});
	});

	t.test('Updates test item', function(t) {
		item = _.extend(item, {
			name: 'Test Name Updated',
			$key: 'id'
		});
		delete item.insertId;

		harness.db.update('tmp', item, function (err, res) {
			t.notOk(err, "no errors were thrown on update");
			t.end();
		}, {enforceRules: false});
	});

	t.test('Teardown', tearDown);
});

test("Disconnects from the database", function (t) {
	harness.disconnect(function (err, res) {
		t.end();
	});
});

function generateTestItems(amt) {
	var items = [];
	for (var i = 0; i < amt; i++)
		items[i] = {id: i, name: 'test ' + i, created: dateHelper.utcNow()};
	return items;
}

function setup(t, createTableOptions) {
	createTableOptions = createTableOptions || {tempTable: true};
	t.test("Drops test table", function (t) {
		harness.dropTable(function (err, res) {
			t.end();
		});
	});
	t.test("Creates test table", function (t) {
		harness.createTable(createTableOptions, function (err, res) {
			t.end();
		});
	});
	t.end();
}

function tearDown(t) {
	t.test("Drops test table", function (t) {
		harness.dropTable(function (err, res) {
			t.end();
		});
	});
	t.end();
}

