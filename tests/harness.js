"use strict";
var mysqlUtil = require('../');
exports.mysqlSession = null;

exports.createTable = function(options, cb) {
	var sql = [
		'CREATE'+(options.tmpTable ? ' TEMPORARY ' : ' ')+'TABLE IF NOT EXISTS tmp (',
		'id BIGINT(20) NOT NULL,',
		'created datetime NOT NULL,',
		'name varchar(150) COLLATE utf8_unicode_ci NOT NULL,',
		'PRIMARY KEY (id)',
		') ENGINE=InnoDB DEFAULT CHARSET=utf8'
	].join('\n');
	exports.mysqlSession.query(sql, cb);
}

exports.connect = function(cb) {
	mysqlUtil.connect({
		host: process.env.MYSQL_HOST || 'localhost',
		user: process.env.MYSQL_USER || 'root',
		password: process.env.MYSQL_PASSWORD || '',
		database: process.env.MYSQL_DATABASE || 'beaches_int'
	}, function (err, session) {
		exports.mysqlSession = session;
		cb();
	});
}

exports.dropTable = function(cb) {
	exports.mysqlSession.query('DROP TABLE IF EXISTS tmp;', cb);
};

exports.disconnect = function(cb) {
	exports.mysqlSession.disconnect(cb);
};

exports.utils = mysqlUtil.utils;
