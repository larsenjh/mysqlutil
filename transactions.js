module.exports = function (conn) {

	function startTransaction(cb) {
		conn.query("START TRANSACTION", null, function(err,result) {
			if(err) {
				pub.openTransaction = false;
				return cb(err);
			}
			pub.openTransaction = true;
			cb();
		});
	}

	function commit(cb) {
		if(!pub.openTransaction) return cb();
		conn.query("COMMIT", null, function(err,result) {
			if(err) return cb(err);
			pub.openTransaction = false;
			cb();
		});
	}

	function rollback(cb) {
		if(!pub.openTransaction) return cb();
		conn.query("ROLLBACK", null, function(err,result) {
			if(err) return cb(err);
			pub.openTransaction = false;
			cb();
		});
	}

	var pub = {
		openTransaction:false,
		startTransaction:startTransaction,
		commit:commit,
		rollback:rollback
	};
	return pub;
};
