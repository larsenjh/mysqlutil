var _ = require('underscore');
var mysql = require('mysql'); // npm install mysql@2.0.0-alpha3
var session = require('./session');

exports.connect = function (settings, cb) {
	console.log("mysqlUtil attempting connection");
	_.defaults(settings, {host:'localhost', port:3306, user:'root', multipleStatements:true});

	var connection = mysql.createConnection(settings);
	connection.connect(function (err) {
		if (err) {
			console.log("mysqlUtil could not connect");
			return cb(err);
		}
		console.log("mysqlUtil connected");
		_.extend(exports.session, session(connection));
		cb(err, exports.session);
	});
};
exports.insertModes = require('./insertModes.js');
exports.util = require('./utils.js');
exports.session = {};
