"use strict";
var _ = require('lodash');
var test = require('tape');
var deadlockRetry = require('../lib/deadlockRetry.js');

test("Retries the requested amount of time on deadlock errors", function (t) {
	var amtCalls = 0;
	var amtCallsExpected = 3;

	function f(p, cb) {
		amtCalls++;

		var err = new Error();
		err.code = 'ER_LOCK_DEADLOCK';

		return cb(err);
	}

	deadlockRetry({fn: f, args: [1], retryAmount: amtCallsExpected}, function (err, res) {
		t.equal(amtCalls, amtCallsExpected, amtCallsExpected + ' calls should have been made, received: ' + amtCalls);
		t.end();
	});
});

test("Doesn't retry for non-errors", function (t) {
	var amtCalls = 0;
	var amtCallsExpected = 1;

	function f(p, cb) {
		amtCalls++;
		return cb(new Error());
	}

	deadlockRetry({fn: f, args: [1], retryAmount: amtCallsExpected}, function (err, res) {
		t.equal(amtCalls, amtCallsExpected, amtCallsExpected + ' calls should have been made, received: ' + amtCalls);
		t.end();
	});
});

test("Handles missing args", function (t) {
	var amtCalls = 0;
	var amtCallsExpected = 1;

	function f(cb) {
		amtCalls++;
		return cb(new Error());
	}

	deadlockRetry({fn: f}, function (err, res) {
		t.equal(amtCalls, amtCallsExpected, amtCallsExpected + ' calls should have been made, received: ' + amtCalls);
		t.end();
	});
});
