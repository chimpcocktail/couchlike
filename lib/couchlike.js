var EventEmitter = require('events').EventEmitter;
var url = require('url');
var util = require('util');
var couchlikeViews = require('./couchlikeViews');
var couchbase = require('couchbase');
var nano = require('nano');
var PouchDB = require('pouchdb');

var engineType = {
	mock: "mock",
	couchbase: "couchbase",
	couchDB: "couchDB",
	pouchDB: "pouchDB",
	couchbaseSyncGateway: "couchbaseSyncGateway"
};

function copiedCouchbaseDocument(document) {
	var newDocument = JSON.parse(JSON.stringify(document));
	delete newDocument._rev;
	delete newDocument._id;
	return newDocument;
}

function Couchlike(config) {
	this.config = config ? config : { couchlike: { type: engineType.mock } };
	if (this.config.config) {
		if (!this.config.config.bucket) { this.config.config.bucket = this.config.config.database; }
		if (this.config.config.database) { delete this.config.config.database; }
	}

	this.engine = null;
	if (this.config && this.config.couchlike && this.config.couchlike.type === engineType.mock) {
		this.engine = new couchbase.Mock.Connection();
	} else if (this.config && this.config.couchlike && this.config.couchlike.type === engineType.couchbase) {
		this.engine = new couchbase.Connection(this.config.config);
	} else if (this.config && this.config.couchlike && (this.config.couchlike.type === engineType.couchDB || this.config.couchlike.type === engineType.couchbaseSyncGateway)) {
		var urlConn = url.parse(this.config.config.host);
		if (this.config.config.username) { urlConn.auth = this.config.config.username+':'+this.config.config.password; }
		this.engine = nano(url.format(urlConn));
		this.engine.bucket = this.engine.use(this.config.config.bucket);
		if (this.config.couchlike.type === engineType.couchbaseSyncGateway && this.config.config.couchbase) {
			this.engine.couchbase = new couchbase.Connection(this.config.config.couchbase);
		}
	} else if (this.config && this.config.couchlike && this.config.couchlike.type === engineType.pouchDB) {
		this.engine = new PouchDB(this.config.config.bucket);
	} else {
		throw new Error('Unrecognised configuration');
	}

	this.views = new couchlikeViews.CouchlikeViews(this);
}

util.inherits(Couchlike, EventEmitter);

function engineTest(couch, methods, callback) {
	if (!couch.engine) {
		callback(new Error('Engine not configured'));
		return false;
	} else if (!couch.config.couchlike || !couch.config.couchlike.type) {
		callback(new Error('Invalid config'));
		return false;
	} else if (!methods[couch.config.couchlike.type]) {
		callback(new Error('Method not configured'));
		return false;
	} else { return true; }
}

function documentTestForSet(document, callback) {
	if (!document._id) {
		callback(new Error('Document must contain an _id'));
		return false;
	} else { return true; }
}

Couchlike.prototype.isCouchbasey = function(callback) {
	return (this.config && this.config.couchlike && (this.config.couchlike.type === engineType.mock || this.config.couchlike.type === engineType.couchbase || this.config.couchlike.type === engineType.couchbaseSyncGateway));
};

Couchlike.prototype.ping = function(callback) {
	var that = this;
	var methods = {
		mock: function() {
			if (callback) { callback(null, { ping: "ok" }); }
		},
		couchbase: function() {
			that.engine.stats(callback);
		},
		couchDB: function() {
			that.engine.request({}, callback);
		},
		pouchDB: function() {
			if (callback) { callback(null, { ping: "ok" }); }
		},
		couchbaseSyncGateway: function() {
			that.engine.request({}, callback);
		}
	};
	if (!engineTest(this, methods, callback)) { return null; }
	methods[this.config.couchlike.type]();
};

Couchlike.prototype.get = function(id, callback) {
	function parseResult(err, result) {
		var response = (result && result.value) ? result.value : result;
		if (err) {
			if (callback) {
				if (err.code === couchbase.errors.keyNotFound || err.status === 404 || err.status_code === 404) { callback(null, null); }
				else { callback(err); }
			} else { throw err; }
		} else {
			if (!response._id) { response._id = id;	}
			if (result.cas && !response._rev) { response._rev = result.cas;	}
			if (callback) { callback(null, response); }
		}
	}

	var that = this;
	var methods = {
		mock: function() {
			that.engine.get(id, parseResult);
		},
		couchbase: function() {
			that.engine.get(id, parseResult);
		},
		couchDB: function() {
			that.engine.bucket.get(id, parseResult);
		},
		pouchDB: function() {
			that.engine.get(id, parseResult);
		},
		couchbaseSyncGateway: function() {
			that.engine.bucket.get(id, parseResult);
		}
	};
	if (!engineTest(this, methods, callback)) { return null; }
	methods[this.config.couchlike.type]();
};

Couchlike.prototype.set = function(document, callback) {
	function parseResult(err, result) {
		if (err) { return callback(err); }
		if (result && result.cas) { document._rev = result.cas; }
		else if (result && result.rev) { document._rev = result.rev; }
		if (callback) { callback(err, document); }
	}

	var that = this;
	var methods = {
		mock: function() {
			var options = {
				cas: document._rev
			};
			that.engine.set(document._id, copiedCouchbaseDocument(document), options, parseResult);
		},
		couchbase: function() {
			var options = {
				cas: document._rev
			};
			that.engine.set(document._id, copiedCouchbaseDocument(document), options, parseResult);
		},
		couchDB: function() {
			that.engine.bucket.insert(document, parseResult);
		},
		pouchDB: function() {
			that.engine.put(document, parseResult);
		},
		couchbaseSyncGateway: function() {
			that.engine.bucket.insert(document, document._id, parseResult);
		}
	};
	if (!engineTest(this, methods, callback)) { return null; }
	if (!documentTestForSet(document, callback)) { return null; }
	methods[this.config.couchlike.type]();
};

Couchlike.prototype.remove = function(id, callback) {
	function parseResult(err, result) {
		if (callback) { callback(err); }
	}

	var that = this;
	var methods = {
		mock: function() {
			that.engine.remove(id, parseResult);
		},
		couchbase: function() {
			that.engine.remove(id, parseResult);
		},
		couchDB: function() {
			that.engine.bucket.get(id, function(err, result){
				if (err) {
					if (callback) { callback(err, result); }
				}
				else { that.engine.bucket.destroy(result._id, result._rev, parseResult); }
			});
		},
		pouchDB: function() {
			that.engine.get(id, function(err, result){
				if (err) {
					if (callback) { callback(err, result); }
				}
				else { that.engine.remove(result, parseResult); }
			});
		},
		couchbaseSyncGateway: function() {
			that.engine.bucket.get(id, function(err, result){
				if (err) {
					if (callback) { callback(err, result); }
				}
				else { that.engine.bucket.destroy(result._id, result._rev, parseResult); }
			});
		}
	};
	if (!engineTest(this, methods, callback)) { return null; }
	methods[this.config.couchlike.type]();
};

exports.Couchlike = Couchlike;
exports.engineType = engineType;
