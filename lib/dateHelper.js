var dateFormat = require('dateformat');

module.exports.formatDateForQuery = function (params) {
	var date = params.date;
	var utc = !!params.utc;
	return dateFormat(date, "yyyy-mm-dd HH:MM:ss", utc);
};
module.exports.utcNow = function () {
	return exports.formatDateForQuery({
		date: new Date(),
		utc: true
	});
};
