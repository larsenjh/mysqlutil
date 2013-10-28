var dateFormat = require('dateformat');

exports.utcNow = function() {
	return exports.formatDateForQuery({
		date: new Date(),
		utc: true
	});
};
exports.formatDateForQuery = function(params) {
	var date = params.date;
	var utc = !!params.utc;
	return dateFormat(date, "yyyy-mm-dd HH:MM:ss", utc);
};
