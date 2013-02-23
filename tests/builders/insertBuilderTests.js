/** TODO - test returned values */
"use strict";
var test = require('tap').test;
var updateBuilder = require('../../sqlBuilders/insertBuilder.js');

test('Constructs a simple insert', function (t) {
	var item = { id: 1, name: 'Test', color: 'Blue' };

	var expectedSQL = 'INSERT INTO test (id, name, color) VALUES (?, ?, ?)';

	updateBuilder({
		item: item,
		tableName: 'test',
		defaultKeyName: 'id'
	}, function(err,res) {
		t.notOk(err, "Does not return an error, received: "+err);
		t.equal(res.sql, expectedSQL, "Returns the expected SQL");
		t.end();
	});
});

test('Constructs a simple insert with rules', function (t) {
	var item = { id: 1, name: 'Test', color: 'Blue' };

	var rules = [function(item, fields, values, expressions, tableName) {
		fields.push('modified');
		if(expressions)
			expressions.push('UTC_TIMESTAMP');
		else
			values.push('UTC_TIMESTAMP');
	}];

	var expectedSQL = 'INSERT INTO test (id, name, color, modified) VALUES (?, ?, ?, UTC_TIMESTAMP)';

	updateBuilder({
		item: item,
		tableName: 'test',
		rules: rules,
		defaultKeyName: 'id'
	}, function(err,res) {
		t.equal(res.sql, expectedSQL, "Returns columns-values section for UPDATE");
		t.end();
	});
});
