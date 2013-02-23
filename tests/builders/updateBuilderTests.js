"use strict";
var test = require('tap').test;
var updateBuilder = require('../../sqlBuilders/updateBuilder.js');

test('Constructs a simple update', function (t) {
	var item = { id: 1, name: 'Test', color: 'Blue' };
	var expectedSQL = 'UPDATE test SET id=?,name=?,color=? WHERE id=?;';

	updateBuilder({
		item: item,
		tableName: 'test',
		defaultKeyName: 'id'
	}, function(err,res) {
		t.notOk(err, "Does not return an error, received: "+err);
		t.equal(res, expectedSQL, "Returns the expected SQL");
		t.end();
	});
});
/*
test('Constructs a simple update with rules', function (t) {
	var item = { id: 1, name: 'Test', color: 'Blue' };
	var updateRules = [function(item, fields, values, expressions, tableName) {
		fields.push('modified');
		if(expressions)
			expressions.push('UTC_TIMESTAMP');
		else
			values.push('UTC_TIMESTAMP');
	}];
	var expectedString = 'id=?,name=?,color=?,modified=UTC_TIMESTAMP';

	var res = updateBuilder({
		item: item,
		tableName: 'test',
		rules: updateRules
	});

	t.equal(res, expectedString, "Returns columns-values section for UPDATE");
	t.end();
});
*/
