"use strict";
var _ = require('lodash');
var test = require('tape');
var deadlockRetry = require('../lib/deadlockRetry.js');

test("Retries the requested amount of time on deadlock errors", function (t) {
	var amtCalls = 0;
	var amtCallsExpected = 3;

	function f(p, cb) {
		amtCalls++;

		return setImmediate(function () {
			var err = new Error();
			err.code = 'ER_LOCK_DEADLOCK';
			cb(err);
		});
	}

	deadlockRetry({
		fn: f,
		args: [1],
		retryAmount: amtCallsExpected
	}, function (err, res) {
		t.equal(amtCalls, amtCallsExpected, amtCallsExpected + ' calls should have been made, received: ' + amtCalls);
		t.end();
	});
});

test("Doesn't retry for non-deadlock errors", function (t) {
	var amtCalls = 0;
	var amtCallsExpected = 1;

	function f(p, cb) {
		amtCalls++;
		return setImmediate(function () {
			cb(new Error());
		});
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

		return setImmediate(function () {
			var err = new Error();
			err.code = 'ER_LOCK_DEADLOCK';
			cb(err);
		});
	}

	deadlockRetry({fn: f, retryAmount: amtCallsExpected}, function (err, res) {
		t.equal(amtCalls, amtCallsExpected, amtCallsExpected + ' calls should have been made, received: ' + amtCalls);
		t.end();
	});
});

test("Doesn't retry on success", function (t) {
	var amtCalls = 0;
	var amtCallsExpected = 1;

	function f(cb) {
		amtCalls++;

		return setImmediate(function () {
			cb();
		});
	}

	deadlockRetry({fn: f}, function (err, res) {
		t.equal(amtCalls, amtCallsExpected, amtCallsExpected + ' calls should have been made, received: ' + amtCalls);
		t.end();
	});
});

test("Faithfully returns result", function (t) {
	var amtCalls = 0;
	var amtCallsExpected = 1;
	var expectedResult = {name: 'Test'};

	function f(cb) {
		amtCalls++;

		return setImmediate(function () {
			cb(null, expectedResult);
		});
	}

	deadlockRetry({fn: f}, function (err, res) {
		t.deepEqual(res, expectedResult, "Should have returned: " + JSON.stringify(expectedResult) + " received: " + JSON.stringify(res));
		t.end();
	});
});
