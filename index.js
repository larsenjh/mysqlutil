var _ = require('lodash');
var mysql = require('mysql');

module.exports = MysqlUtil;
module.exports.utils = require('./lib/dateHelper.js');
module.exports.insertModes = require('./lib/insertModes.js');

function MysqlUtil(settings, cb) {
	cb = cb || function(){};

	_.defaults(settings, {
		host:'localhost',
		port:3306,
		user:'root',
		multipleStatements:true,
		timezone:'Z',
		connectionPingIntervalSeconds: 15 * 60 // 15 minutes
	});

	var connection = mysql.createConnection(settings);

	var intervalId;

	connection.on('error', function(err) {
		clearInterval(intervalId);
		throw err;
	});

	connection.on('end', function(err) {
		clearInterval(intervalId);
	});

	connection.connect(function (err) {
		if (err) return cb(err);
		_.extend(exports.session, require('./lib/session.js')(connection));
		intervalId = setInterval(ping, settings.connectionPingIntervalSeconds * 1000);
		cb(err, exports.session);
	});

	function ping() {
		connection.ping(function(err, res) {
			console.log(new Date().toString(), ' ping.');
			if(err) throw err;
		});
	}
}
exports.session = {};
