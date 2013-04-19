var _ = require('lodash');
var mysql = require('mysql');

module.exports = MysqlUtil;
module.exports.utils = require('./lib/dateHelper.js');
module.exports.insertModes = require('./lib/insertModes.js');

function MysqlUtil(settings, cb) {
	cb = cb || function(){};

	var connection = mysql.createConnection(_.defaults(settings, {
		host:'localhost',
		port:3306,
		user:'root',
		multipleStatements:true,
		timezone:'Z'
	}));

	connection.on('error', function(err) {
		throw err;
	});

	connection.connect(function (err) {
		if (err) return cb(err);
		_.extend(exports.session, require('./lib/session.js')(connection));
		cb(err, exports.session);
	});
}
exports.session = {};
