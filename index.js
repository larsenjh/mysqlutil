var _ = require('lodash');

module.exports = MysqlUtil;
module.exports.utils = require('./lib/dateHelper.js');
module.exports.insertModes = require('./lib/insertModes.js');

function MysqlUtil(settings, cb) {
	cb = cb || function () {};

	_.defaults(settings, {
		host: 'localhost',
		port: 3306,
		user: 'root',
		multipleStatements: true,
		waitForConnections: true,
		connectionLimit: 5,
		timezone: 'Z',
		amtToPrefill: 5,
		charset: 'UTF8MB4_GENERAL_CI',
		connectionPingIntervalSeconds: 15 * 60, // 15 minutes
		debugging: {
			hilo: false,
			ping: false,
			connectionEnd: false,
			connectionError: true,
			poolPerf: false,
			queryPerf: false,
			queryPerfSlowQueryThresholdSec: 1,
			queryResult: false
		}
	});

	// cluster settings, if present (optional)
	if(settings.cluster) {
		_.each(settings.cluster.nodes, function (node) {
			_.defaults(node, {
				host: 'localhost',
				port: 3306,
				user: 'root',
				multipleStatements: settings.multipleStatements,
				waitForConnections: settings.waitForConnections,
				timezone: settings.timezone,
				charset: settings.charset,
				connectionLimit: settings.connectionLimit,
				amtToPrefill: settings.amtToPrefill
			});
			if (!node.name) return cb("cluster entry must have a name");
		});
	}
	
	this.session = require('./lib/session.js')(settings);

	if (!settings.amtToPrefill)
		return cb(null, this.session);

	this.session.preFillPool(function () {
		cb(null, this.session);
	});
}
