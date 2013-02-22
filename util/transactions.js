module.exports = function (conn) {

	function startTransaction(cb) {
		conn.query("START TRANSACTION", function (err, res) {
			if (err) {
				pub.openTransaction = false;
				return cb(err);
			}
			pub.openTransaction = true;
			cb();
		});
	}

	function commit(cb) {
		if (!pub.openTransaction) return cb();
		conn.query("COMMIT", function (err, res) {
			if (err) return cb(err);
			pub.openTransaction = false;
			cb();
		});
	}

	function rollback(cb) {
		if (!pub.openTransaction) return cb();
		conn.query("ROLLBACK", function (err, res) {
			if (err) return cb(err);
			pub.openTransaction = false;
			cb();
		});
	}

	var pub = {
		openTransaction: false,
		startTransaction: startTransaction,
		commit: commit,
		rollback: rollback
	};
	return pub;
};
