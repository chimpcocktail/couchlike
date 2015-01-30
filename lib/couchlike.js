var EventEmitter = require('events').EventEmitter;
var url = require('url');
var util = require('util');
var couchlikeViews = require('./couchlikeViews');
var couchlikeChanges = require('./couchlikeChanges');
var couchbase = require('couchbase');
var nano = require('nano');
var PouchDB = require('pouchdb');

/**
 * @global
 * @desc Enumeration of couchlike engine types.
 * @readonly
 * @enum {string}
 */
var engineType = {
	/** Couchbase Mock server **/
	mock: "mock",
	/** Couchbase server **/
	couchbase: "couchbase",
	/** CouchDB server **/
	couchDB: "couchDB",
	/** PouchDB server **/
	pouchDB: "pouchDB",
	/** Couchbase Sync Gateway server **/
	couchbaseSyncGateway: "couchbaseSyncGateway"
};

/**
 * @typedef {Object} CouchlikeCapability
 * @desc Capability of a couchbase connection
 * @property {boolean} changes - TODO
 * @property {boolean} views - TODO
 * @example
 * TODO
 */

/**
 * @global
 * @desc Enumeration of couchlike capabilities.
 * @readonly
 * @enum {string}
 */
var capability = {
	/**
	 * @desc Default couchlike capability
	 * @type {CouchlikeCapability}
	 **/
	default: { },
	/**
	 * @desc Couchbase Mock couchlike capability
	 * @type {CouchlikeCapability}
	 **/
	mock: {
		changes: false,
		views: true
	},
	/**
	 * @desc Couchbase couchlike capability
	 * @type {CouchlikeCapability}
	 **/
	couchbase: {
		changes: false,
		views: false
	},
	/**
	 * @desc CouchDB couchlike capability
	 * @type {CouchlikeCapability}
	 **/
	couchDB: {
		changes: true,
		views: true
	},
	/**
	 * @desc PouchDB couchlike capability
	 * @type {CouchlikeCapability}
	 **/
	pouchDB: {
		changes: true,
		views: true
	},
	/**
	 * @desc Couchbase Sync Gateway couchlike capability
	 * @type {CouchlikeCapability}
	 **/
	couchbaseSyncGateway: { // This is not working since the password was set on the mirrored bucket (https://groups.google.com/forum/#!topic/mobile-couchbase/6_S4BWW2a4I)
		changes: true,
		views: true
	}
};

var DEFAULT_COUCHBASE_PORT = 8092;
var DEFAULT_COUCHDB_PORT = 5984;
var DEFAULT_COUCHBASESYNCGATEWAY_PORT = 4984;

function copiedCouchbaseDocument(document) {
	var newDocument = JSON.parse(JSON.stringify(document));
	delete newDocument._rev;
	delete newDocument._id;
	return newDocument;
}

/**
 * @typedef {Object} CouchlikeDocument
 * @desc Couchlike document
 * @property {String} _id - TODO
 * @property {String} _rev - TODO
 * @example
 * TODO
 */
/**
 * @typedef {Object} CouchlikeConfiguration
 * @desc Couchlike configuration
 * @property {engineType} type - TODO
 * @property {CouchlikeConnection} connection - TODO
 * @example
 * TODO
 */
/**
 * @typedef {Object} CouchlikeConnection
 * @desc Connection details for a couchlike server
 * @property {String=} host - TODO
 * @property {Number=} port - TODO
 * @property {String=} username - TODO
 * @property {String=} password - TODO
 * @property {String=} bucket - TODO
 * @property {String=} bucketPassword - TODO
 * @example
 * TODO
 */
/**
 * @callback PingCallback
 * @desc Invoked following a {@link Couchlike#ping} attempt on a couchlike server.
 * @param {undefined|Error} error - If an error occurred during the operation it is returned here.
 * @param {Object} response - TODO.
 */
/**
 * @callback GetCallback
 * @desc Invoked following a get request on a couchlike server.
 * @param {undefined|Error} error - If an error occurred during the operation it is returned here.
 * @param {Object} response - TODO.
 */
/**
 * @callback SetCallback
 * @desc Invoked following a set request on a couchlike server.
 * @param {undefined|Error} error - If an error occurred during the operation it is returned here.
 * @param {Object} response - TODO.
 */
/**
 * @callback RemoveCallback
 * @desc Invoked following a remove request on a couchlike server.
 * @param {undefined|Error} error - If an error occurred during the operation it is returned here.
 * @param {Object} response - TODO.
 */

/**
 * @classdesc Root class for all couchlike functionality. Instantiate a new Couchlike instance, passing a {@link CouchlikeConfiguration} to the constructor.
 * @desc TODO.
 * @constructor
 * @augments EventEmitter
 * @property {CouchlikeConfiguration} config - TODO
 * @property {CouchlikeViews} views - TODO
 * @property {CouchlikeChanges} changes - TODO
 * @param {CouchlikeConfiguration=} config - TODO
 * @example
 * TODO
 */
function Couchlike(config) {
	this.config = config ? config : { type: engineType.mock };
	this.capability = capability.default;
	if (this.config.connection) {
		if (!this.config.connection.bucket) { this.config.connection.bucket = this.config.connection.database; }
		if (this.config.connection.database) { delete this.config.connection.database; }
	}

	this.engine = null;
	if (this.config && this.config.type === engineType.mock) {
		this.engine = new couchbase.Mock.Connection();
		this.capability = capability.mock;
	} else if (this.config && this.config.type === engineType.couchbase) {
		if (!/^http/.test(this.config.connection.host)) { this.config.connection.host = 'http://'+this.config.connection.host; }
		this.engine = new couchbase.Connection(this.config.connection);
		this.capability = capability.couchbase;
	} else if (this.config && (this.config.type === engineType.couchDB || this.config.type === engineType.couchbaseSyncGateway)) {
		if (!/^http/.test(this.config.connection.host)) { this.config.connection.host = 'http://'+this.config.connection.host; }
		var urlConn = url.parse(this.config.connection.host);
		urlConn.host = null;
		if (this.config.connection.username && this.config.connection.password) { urlConn.auth = this.config.connection.username+':'+this.config.connection.password; }
		if (this.config.connection.port) { urlConn.port = this.config.connection.port; }
		if (!urlConn.port) { urlConn.port = (this.config.type === engineType.couchDB) ? DEFAULT_COUCHDB_PORT : DEFAULT_COUCHBASESYNCGATEWAY_PORT; }
		var couchDBUrl = url.format(urlConn);
		this.engine = nano(couchDBUrl);
		this.engine.bucket = this.engine.use(this.config.connection.bucket);
		this.capability = capability.couchDB;
		if (this.config.type === engineType.couchbaseSyncGateway) {
			urlConn.auth = null;
			if (this.config.connection.bucketPassword) { urlConn.auth = this.config.connection.bucket+':'+this.config.connection.bucketPassword; }
			urlConn.port = DEFAULT_COUCHBASE_PORT;
			var couchbaseUrl = url.format(urlConn);
			this.engine.couchbase = nano(couchbaseUrl);
			if (!this.config.connection.couchbaseBucket) { this.config.connection.couchbaseBucket = this.config.connection.bucket; }
			this.engine.couchbase.bucket = this.engine.couchbase.use(this.config.connection.couchbaseBucket);
			this.capability = capability.couchbaseSyncGateway;
		}
	} else if (this.config && this.config.type === engineType.pouchDB) {
		this.engine = new PouchDB(this.config.connection.bucket);
			this.capability = capability.pouchDB;
	} else {
		throw new Error('Unrecognised configuration');
	}

	var that = this;
	this.views = new couchlikeViews.CouchlikeViews(this);
	this.changes = new couchlikeChanges.CouchlikeChanges(this);
	this.changes.on('change', function(change) {
		that.emit('change', change);
	});
}

util.inherits(Couchlike, EventEmitter);

function engineTest(couch, methods, callback) {
	if (!couch.engine) {
		callback(new Error('Engine not configured'));
		return false;
	} else if (!couch.config || !couch.config.type) {
		callback(new Error('Invalid config'));
		return false;
	} else if (!methods[couch.config.type]) {
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

function bulkDocumentsTestForSet(documents, callback) {
	if (!documents.forEach) {
		callback(new Error('Must be an array'));
		return false;
	} else {
		var result = true;
		documents.forEach(function(document) {
			if (!result) { return result; }
			result = documentTestForSet(document);
		});
		return result;
	}
}

Couchlike.prototype.isCouchbasey = function() {
	return (this.config && (this.config.type === engineType.mock || this.config.type === engineType.couchbase || this.config.type === engineType.couchbaseSyncGateway));
};

/**
 * @desc TODO
 * @param {PingCallback=} callback
 * @example
 * TODO
 */
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
	methods[this.config.type]();
};

/**
 * @desc TODO
 * @param {String} id - TODO
 * @param {GetCallback=} callback
 * @example
 * TODO
 */
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
			/*
			that.engine.bucket.get(id, parseResult);

			The following is the workaround for	https://github.com/couchbase/sync_gateway/issues/324
			Once this issue has been resolved, the above simple implementation should be reinstated.

			*/
			var req = {
				db: that.config.connection.bucket,
				path: id,
				method: 'get'
			};
			that.engine.request(req, parseResult);
		}
	};
	if (!engineTest(this, methods, callback)) { return null; }
	methods[this.config.type]();
};

/**
 * @desc TODO
 * @param {CouchlikeDocument} document - TODO
 * @param {SetCallback=} callback
 * @example
 * TODO
 */
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
			/*
			that.engine.bucket.insert(document, document._id, parseResult);

			The following is the workaround for	https://github.com/couchbase/sync_gateway/issues/324
			Once this issue has been resolved, the above simple implementation should be reinstated.

			*/
			var req = {
				db: that.config.connection.bucket,
				path: document._id,
				body: document,
				method: 'put'
			};
			that.engine.request(req, function(err, result) {
				if (result) { document._rev = result.rev; }
				callback(err, document);
			});
		}
	};
	if (!engineTest(this, methods, callback)) { return null; }
	if (!documentTestForSet(document, callback)) { return null; }
	methods[this.config.type]();
};

/**
 * @desc TODO
 * @param {CouchlikeDocument} document - TODO
 * @param {SetCallback=} callback
 * @example
 * TODO
 */
Couchlike.prototype.bulkSet = function(documents, callback) {
	function parseResult(err, result) {
/*
		if (err) { return callback(err); }
		if (result && result.cas) { document._rev = result.cas; }
		else if (result && result.rev) { document._rev = result.rev; }
*/
		if (callback) { callback(err, result); }
	}

	var that = this;
	var methods = {
		mock: function() {
			callback(new Error('Not Implemented'));
		},
		couchbase: function() {
			callback(new Error('Not Implemented'));
		},
		couchDB: function() {
			that.engine.bucket.bulk({docs: documents}, parseResult);
		},
		pouchDB: function() {
			callback(new Error('Not Implemented'));
		},
		couchbaseSyncGateway: function() {
			callback(new Error('Not Implemented'));
		}
	};
	if (!engineTest(this, methods, callback)) { return null; }
	if (!bulkDocumentsTestForSet(documents, callback)) { return null; }
	methods[this.config.type]();
};

/**
 * @desc TODO
 * @param {String} id - TODO
 * @param {RemoveCallback=} callback
 * @example
 * TODO
 */
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
	methods[this.config.type]();
};

exports.Couchlike = Couchlike;
exports.engineType = engineType;
