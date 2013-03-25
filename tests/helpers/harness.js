"use strict";
var async = require('async');
var mysqlUtil = require('../../');
var dateHelper = require('../../lib/dateHelper.js');
exports.db = null;

function createTestTable(options, cb) {
	var sql = "CREATE" + (options.tmpTable ? ' TEMPORARY ' : ' ') + "TABLE IF NOT EXISTS tmp ( \
	id BIGINT(20) NOT NULL, \
	created datetime NOT NULL, \
	name varchar(150) COLLATE utf8_unicode_ci NOT NULL, \
	PRIMARY KEY (id) \
	) ENGINE=InnoDB DEFAULT CHARSET=utf8;";
	exports.db.query(sql, cb);
};

exports.connect = function (t) {
	mysqlUtil({
		host: process.env.MYSQL_HOST || '127.0.0.1',
		user: process.env.MYSQL_USER || 'root',
		password: process.env.MYSQL_PASSWORD || null,
		database: process.env.MYSQL_DATABASE || 'mysqlutil_test'
	}, function (err, session) {
		t.notOk(err, "No errors should be thrown when connecting to the database, received: " + err);
		t.ok(session, "A db session was received");
		exports.db = session;
		t.end();
	});
};

exports.createHiLoTableAndProc = function (t) {
	var sql = "CREATE TABLE HiLoID ( \
	NextHi bigint(20) NOT NULL, \
	PRIMARY KEY (`NextHi`) \
	) ENGINE=InnoDB; \
	INSERT INTO HiLoID(NextHi) values (1); \
	\
	CREATE PROCEDURE getNextHi(IN numberOfBatches INT) \
	BEGIN \
	START TRANSACTION; \
	SELECT NextHi FROM HiLoID FOR UPDATE; \
	UPDATE HiLoID SET NextHi = NextHi + numberOfBatches; \
	COMMIT; \
	END";
	exports.db.query(sql, function (err, res) {
		t.notOk(err, "No errors were thrown when creating getNextHi proc, received: " + err);
		t.end();
	});
};

exports.dropHiLoTableAndProc = function (t) {
	var sql = "DROP TABLE IF EXISTS HiLoID; \
	DROP PROCEDURE IF EXISTS getNextHi;";
	exports.db.query(sql, function (err, res) {
		t.notOk(err, "No errors were thrown when dropping getNextHi proc and HiLoID table, received: " + err);
		t.end();
	});
};

exports.getItemsInTestTable = function (cb) {
	exports.db.query('SELECT * FROM tmp', [], cb);
};

exports.disconnect = function (t) {
	exports.db.disconnect(function (err, res) {
		t.notOk(err, "No errors were thrown when disconecting from the db, received: " + err);
		t.end();
	});
};

exports.generateTestItems = function (amt) {
	var items = [];
	for (var i = 0; i < amt; i++)
		items[i] = {id: i, name: 'test ' + i, created: dateHelper.utcNow()};
	return items;
};

exports.createTestTempTable = function (t) {
	createTestTable({tempTable: true}, function (err, res) {
		t.notOk(err, "No errors were thrown when creating test table, received: " + err);
		t.end();
	});
};

exports.truncateTestTable = function(t) {
	exports.db.query('TRUNCATE TABLE tmp;', function (err, res) {
		t.notOk(err, "No errors were thrown when truncating test table, received: " + err);
		t.end();
	});
};

exports.dropTestTable = function (t) {
	t.test("Drops test table", function (t) {
		exports.db.query('DROP TABLE IF EXISTS tmp;', function (err, res) {
			t.notOk(err, "No errors were thrown when dropping test table, received: " + err);
			t.end();
		});
	});
	t.end();
};
