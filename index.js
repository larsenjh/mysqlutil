var _ = require('lodash');

module.exports = MysqlUtil;
module.exports.utils = require('./lib/dateHelper.js');
module.exports.insertModes = require('./lib/insertModes.js');

function MysqlUtil(settings, cb) {
	cb = cb || function(){};

	this.config = _.defaults(settings, {
		host:'localhost',
		port:3306,
		user:'root',
		multipleStatements:true,
		waitForConnections:true,
		connectionLimit: 20,
		preFillPool: true
	});
	this.session = require('./lib/session.js')(this.config);

	if(!this.config.preFillPool)
		return cb(null, this.session);

	var amtToPrefill = settings.amtToPrefill || 5;
	this.session.preFillPool(amtToPrefill, function() {
		cb(null, this.session);
	});
}
