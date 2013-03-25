"use strict";
var _ = require('lodash');
var test = require('tap').test;
var async = require('async');
var insertModes = require('../lib/insertModes.js');
var harness = require('./helpers/harness.js');
var dateHelper = require('../lib/dateHelper.js');

test("Connects to the database", harness.connect);
test("Drops Hilo table and proc", harness.dropHiLoTableAndProc);
test("Creates Hilo table and proc", harness.createHiLoTableAndProc);
test("Setup test table", harness.createTestTempTable);

test('a simple update works', function (t) {
	var item = harness.generateTestItems(1)[0];

	t.test('Creates test item', function(t) {
		harness.db.insert('tmp', item, function (err, result) {
			t.notOk(err, "no errors were thrown on insert, received: " + err);
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
			t.notOk(err, "no errors were thrown on update, received: " + err);
			t.end();
		}, {enforceRules: false});
	});

	t.end();
});

test("Drops test table", harness.dropTestTable);
test("Drops Hilo table and proc", harness.dropHiLoTableAndProc);
test("Disconnects from the database", harness.disconnect);
