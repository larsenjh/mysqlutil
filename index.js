var _ = require('lodash');

module.exports = MysqlUtil;

function MysqlUtil(settings, cb) {
	cb = cb || function(){};

	this.config = _.defaults(settings, {host:'localhost', port:3306, user:'root', multipleStatements:true, waitForConnections:true});
	this.session = require('./lib/session.js')(this.config);
	this.insertModes = require('./lib/insertModes.js');
	this.utils = require('./lib/dateHelper.js');

	cb(null, this.session);
}
