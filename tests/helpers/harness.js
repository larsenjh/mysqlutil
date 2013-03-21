"use strict";
var async = require('async');
var mysqlUtil = require('../../');
var dateHelper = require('../../lib/dateHelper.js');
exports.db = null;

function createTmpTable(options, cb) {
	var sql = "CREATE" + (options.tmpTable ? ' TEMPORARY ' : ' ') + "TABLE IF NOT EXISTS tmp ( \
	id BIGINT(20) NOT NULL, \
	created datetime NOT NULL, \
	name varchar(150) COLLATE utf8_unicode_ci NOT NULL, \
	PRIMARY KEY (id) \
	) ENGINE=InnoDB DEFAULT CHARSET=utf8;";
	exports.db.query(sql, cb);
}

exports.connect = function (t) {
	mysqlUtil.setup({
		host: process.env.MYSQL_HOST || '127.0.0.1',
		user: process.env.MYSQL_USER || 'root',
		password: process.env.MYSQL_PASSWORD || null,
		database: process.env.MYSQL_DATABASE || 'mysqlutil_test'
	}, function (err, session) {
		t.notOk("No errors should be thrown when connecting to the database, received: " + err);
		exports.db = session;
		t.end();
		//exports.db.logging = true;
	});
};

exports.createHiLoTable = function (t) {
	var sql = "CREATE TABLE hiloid ( \
	NextHi bigint(20) NOT NULL, \
	PRIMARY KEY (`NextHi`) \
	) ENGINE=InnoDB; \
	INSERT INTO `hiloid`(`NextHi`) values (1);";
	exports.db.query(sql, function (err, res) {
		t.notOk("No errors should be thrown when creating hiloid table, received: " + err);
		t.end();
	});
};

exports.createHiLoProc = function (t) {
	var sql = "CREATE PROCEDURE getNextHi(IN numberOfBatches INT) \
	BEGIN \
	START TRANSACTION; \
	SELECT NextHi FROM HiLoID FOR UPDATE; \
	UPDATE HiLoID SET NextHi = NextHi + numberOfBatches; \
	COMMIT; \
	END";
	exports.db.query(sql, function (err, res) {
		t.notOk("No errors should be thrown when creating getNextHi proc, received: " + err);
		t.end();
	});
};

exports.dropHiLoTableAndProc = function (t) {
	var sql = "DROP TABLE hiloid; \
	DROP PROCEDURE getNextHi;";
	exports.db.query(sql, function (err, res) {
		t.notOk("No errors should be thrown when dropping getNextHi proc and hiloid table, received: " + err);
		t.end();
	});
};

exports.dropTable = function (cb) {
	exports.db.query('DROP TABLE IF EXISTS tmp;', cb);
};

exports.getItemsInTmpTable = function (cb) {
	exports.db.query('SELECT * FROM tmp', [], cb);
};

exports.disconnect = function (t) {
	exports.db.disconnect(function (err, res) {
		t.end();
	});
};

exports.generateTestItems = function (amt) {
	var items = [];
	for (var i = 0; i < amt; i++)
		items[i] = {id: i, name: 'test ' + i, created: dateHelper.utcNow()};
	return items;
}

exports.setupTmpTable = function (t) {
	t.test("Drops test table", function (t) {
		exports.dropTable(function (err, res) {
			t.end();
		});
	});
	t.test("Creates test table", function (t) {
		createTmpTable({tempTable: true}, function (err, res) {
			t.end();
		});
	});
	t.end();
}

exports.tearDown = function (t) {
	t.test("Drops test table", function (t) {
		exports.dropTable(function (err, res) {
			t.end();
		});
	});
	t.end();
}
