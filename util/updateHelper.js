"use strict";
var _ = require('underscore');

module.exports.buildColsValues = function(params) {
	var sql = [];
	var fields = [];
	var values = [];
	var expressions = [];

	_.each(params.item, function (value, field) {
		if (field.charAt(0) !== '$') {
			fields.push(field);
			expressions.push('?');
			values.push(value);
		}
	});

	if (params.updateRules) {
		_.each(params.updateRules, function (rule) {
			rule(params.item, fields, values, expressions, params.tableName);
		});
	}

	for (var i = fields.length; i--;)
		fields[i] += '=' + expressions[i];

	return fields.join(',');
};
