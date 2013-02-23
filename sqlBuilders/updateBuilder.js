"use strict";
var _ = require('underscore');

module.exports = function (params, cb) {
	var sql = [];
	var fields = [];
	var values = [];
	var expressions = [];

	sql.push('UPDATE ', params.tableName, ' SET ');

	// col = value clause
	_.each(params.item, function (value, field) {
		if (field.charAt(0) === '$') return;
		fields.push(field);
		expressions.push('?');
		values.push(value);
	});

	if (params.rules) {
		_.each(params.rules, function (rule) {
			rule(params.item, fields, values, expressions, params.tableName);
		});
	}

	for (var i = fields.length; i--;)
		fields[i] += '=' + expressions[i];

	sql.push(fields.join(','));

	// WHERE clause
	if (params.item.$where) {
		if (_.isArray(params.item.$where)) {
			values = values.concat(_.rest(params.item.$where));
			params.item.$where = params.item.$where[0];
		}
	}
	else if (params.item.$key) {
		params.item.$where = params.item.$key + '=?';
		values.push(params.item.$key);
	}
	else {
		params.item.$where = params.defaultKeyName + '=?';
		values.push(params.item.$key);
	}

	sql.push(' WHERE ', params.item.$where, ';');

	cb(null, {
		sql: sql.join(''),
		values: values
	});
};
