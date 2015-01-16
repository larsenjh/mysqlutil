"use strict";
var _ = require('lodash');
var test = require('tape');
var async = require('async');
var insertModes = require('../lib/insertModes.js');
var harness = require('./helpers/harness.js');

test("Connects to the database", _.partial(harness.connectClustered, null, null));

test("asking for a connection from a specific node", function(t) {
	async.each(_.range(4),
		function(i, cb) {
			harness.db.getConnection(function(err, conn) {
				t.notOk(err, "no errors were thrown getting connection, received: " + err);
				t.equal(conn._clusterId, 'node1', "should get connection from correct node, got: " + conn._clusterId);
				conn.release();
				cb();
			}, {
				useClusteredNode: 'node1'
			})
		}, function() {
			t.end();
		});
});

test("asking for a connection from a specific node", function(t) {
	async.each(_.range(4),
		function(i, cb) {
			harness.db.getConnection(function(err, conn) {
				t.notOk(err, "no errors were thrown getting connection, received: " + err);
				t.equal(conn._clusterId, 'node2', "should get connection from correct node, got: " + conn._clusterId);
				conn.release();
				cb();
			}, {
				useClusteredNode: 'node2'
			})
		}, function() {
			t.end();
		});
});

test("Disconnects from the database", harness.disconnect);
