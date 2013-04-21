var _ = require('lodash');

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
		waitForConnections:true,
		connectionLimit: 10,
		timezone:'Z',
		amtToPrefill: 10,
		connectionPingIntervalSeconds: 15 * 60, // 15 minutes
		debugging:{
			hilo:false,
			ping:true,
			connectionEnd:false,
			connectionError:true,
			poolPerf:true,
			queryPerf:true,
			queryResult:false
		}
	});

	this.session = require('./lib/session.js')(settings);

	if(!settings.amtToPrefill)
		return cb(null, this.session);

	this.session.preFillPool(function() {
		cb(null, this.session);
	});
}
