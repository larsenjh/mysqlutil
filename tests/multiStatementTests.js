"use strict";
var _ = require('underscore');
var test = require('tap').test;
var harness = require('./helpers/harness.js');

test("Connects to the database", harness.connect);

test('multiple SELECT statements work', function (t) {
	var sql = "SELECT 1; SELECT 2;";

	harness.db.query(sql, null, function (err, result) {
		t.notOk(err, "no errors were thrown on multi-statement select, received: " + err);
		t.end();
	});
});

test("Disconnects from the database", harness.disconnect);
