"use strict";
var mysqlUtil = require('../');

exports.mysqlSession = {};

exports.createTempTable = function(cb) {
	var sql = [
		'CREATE TEMPORARY TABLE IF NOT EXISTS tmp (',
		'id BIGINT(20) NOT NULL,',
		'created datetime NOT NULL,',
		'name varchar(150) COLLATE utf8_unicode_ci NOT NULL,',
		'PRIMARY KEY (id)',
		') ENGINE=InnoDB DEFAULT CHARSET=utf8'
	].join('\n');
	exports.mysqlSession.query(sql, [], function (err, result) {
		cb(err, "tmp");
	});
}

exports.createTable = function(cb) {
	var sql = [
		'CREATE TABLE IF NOT EXISTS tmpTable (',
		'id BIGINT(20) NOT NULL,',
		'created datetime NOT NULL,',
		'name varchar(150) COLLATE utf8_unicode_ci NOT NULL,',
		'PRIMARY KEY (id)',
		') ENGINE=InnoDB DEFAULT CHARSET=utf8'
	].join('\n');
	exports.mysqlSession.query(sql, [], function (err, result) {
		cb(err, "tmpTable");
	});
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

exports.disconnect = function(cb) {
	exports.mysqlSession.disconnect(cb);
}
