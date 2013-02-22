"use strict";
var _ = require('underscore');
var test = require('tap').test;
var async = require('async');
var insertModes = require('../util/insertModes.js');
var harness = require('./helpers/harness.js');
var dateHelper = require('../util/dateHelper.js');

test('a simple update works', function (t) {
	var item = generateTestItems(1)[0];

	t.test('Sets up test', function(t) {
		setup(function(err,res) {
			t.end();
		});
	});

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

		harness.db.update('tmp', item, function (err, res) {
			t.notOk(err, "no errors were thrown on update");
			t.end();
		}, {enforceRules: false});
	});

	t.test('Tears down test', function(t) {
		tearDown(function(err,res) {
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
