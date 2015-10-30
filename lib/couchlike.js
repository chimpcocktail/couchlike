var EventEmitter = require('events').EventEmitter;
var url = require('url');
var util = require('util');
var couchlikeViews = require('./couchlikeViews');
var couchlikeChanges = require('./couchlikeChanges');
var nano = require('nano');
var PouchDB = require('pouchdb');

/**
 * @global
 * @desc Enumeration of couchlike engine types.
 * @readonly
 * @enum {string}
 */
var engineType = {
	/** CouchDB server **/
	couchDB: "couchDB",
	/** PouchDB server **/
	pouchDB: "pouchDB",
	/** Couchbase Sync Gateway server **/
	couchbaseSyncGateway: "couchbaseSyncGateway"
};

/**
 * @typedef {Object} CouchlikeCapability
 * @desc Capability of a couchlike connection
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
	 * @desc CouchDB couchlike capability
	 * @type {CouchlikeCapability}
	 **/
	couchDB: {
		changes: true,
		views: true,
		viewIncludeDocs: true
	},
	/**
	 * @desc PouchDB couchlike capability
	 * @type {CouchlikeCapability}
	 **/
	pouchDB: {
		changes: true,
		views: true,
		viewIncludeDocs: true
	},
	/**
	 * @desc Couchbase Sync Gateway couchlike capability
	 * @type {CouchlikeCapability}
	 **/
	couchbaseSyncGateway: { // This is not working since the password was set on the mirrored bucket (https://groups.google.com/forum/#!topic/mobile-couchbase/6_S4BWW2a4I)
		changes: true,
		views: true,
		viewIncludeDocs: false
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
	this.config = config ? config : { type: engineType.pouchDB };
	this.capability = capability.default;
	if (this.config.connection) {
		if (!this.config.connection.bucket) { this.config.connection.bucket = this.config.connection.database; }
		if (this.config.connection.database) { delete this.config.connection.database; }
	}

	//  \/ \/ \/ \/ \/ HEARTBEAT \/ \/ \/ \/ \/
	var that = this;
	var HEARTBEAT_TIMEOUT = 10 * 1000; // 10 seconds
	this.heartbeatToConfigType =function(heartbeat) {
		var response = engineType.couchDB;
		if (heartbeat && heartbeat.vendor && heartbeat.vendor.name && heartbeat.vendor.name.indexOf && (heartbeat.vendor.name.indexOf('Couchbase') >= 0)) { response = engineType.couchbaseSyncGateway; }
		return response;
	};

	this.configTypeFromHeartbeat = function(callback) {
		if (this.currentHeartbeatValid()) { return callback(null, this.heartbeatToConfigType(this.currentHeartbeat())); }
		this.engine.request({}, function(err, heartbeat) {
			if (err) { return callback(err); }
			var configType = that.heartbeatToConfigType(heartbeat);
			that.updateCurrentHeartbeat(heartbeat, configType);
			return callback(null, configType);
		});
	};

	this.updateCurrentHeartbeat = function(heartbeat, configType) {
		this.doSetTypeDependentStuff(configType);
		this.currentHeartbeat.value = heartbeat;
		this.currentHeartbeat.setAt = Date.now();
	};

	this.currentHeartbeat = function() {
		return this.currentHeartbeat.value;
	};

	this.currentHeartbeatValid = function() {
		return (this.currentHeartbeat() && this.currentHeartbeat.setAt && (Date.now() < (this.currentHeartbeat.setAt + HEARTBEAT_TIMEOUT)));
	};

	this.configType = function(callback) {
		if (this.config.type) { return callback(null, this.config.type); }
		return this.configTypeFromHeartbeat(callback);
	};

	this.doSetTypeDependentStuff = function(configType) {
		that.capability = (configType === engineType.pouchDB) ? capability.pouchDB : (configType === engineType.couchbaseSyncGateway) ? capability.couchbaseSyncGateway : capability.couchDB;
		that._isCouchbasey = (configType === engineType.couchbaseSyncGateway) ? true : false;
	};

	this.setTypeDependentStuff = function() {
		this.configType(function(err, configType) {
			if (err) { throw err; }
			that.doSetTypeDependentStuff(configType);
		});
	};
	//  /\ /\ /\ /\ /\ HEARTBEAT /\ /\ /\ /\ /\

	this.engine = null;
	if (this.config && (!this.config.type || this.config.type === engineType.couchDB || this.config.type === engineType.couchbaseSyncGateway)) {
		if (!/^http/.test(this.config.connection.host)) { this.config.connection.host = 'http://'+this.config.connection.host; }
		var urlConn = url.parse(this.config.connection.host);
		urlConn.host = null;
		if (this.config.connection.username && this.config.connection.password) { urlConn.auth = this.config.connection.username+':'+this.config.connection.password; }
		if (this.config.connection.port) { urlConn.port = this.config.connection.port; }
		if (!urlConn.port) { urlConn.port = (this.config.type === engineType.couchDB) ? DEFAULT_COUCHDB_PORT : DEFAULT_COUCHBASESYNCGATEWAY_PORT; }
		var nanoConfig = { url: url.format(urlConn) };
		if (this.config.connection.strictSSL !== (void 0)) {
			nanoConfig.request_defaults = { strictSSL: this.config.connection.strictSSL };
			if (!this.config.connection.strictSSL) { process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0"; }
		} else if (this.config.connection.ca !== (void 0)) {
			nanoConfig.request_defaults = { ca: this.config.connection.ca };
		}
		this.engine = nano(nanoConfig);
		this.engine.bucket = this.engine.use(this.config.connection.bucket);
	} else if (this.config && this.config.type === engineType.pouchDB) {
		this.engine = new PouchDB(this.config.connection.bucket);
	} else {
		throw new Error('Unrecognised configuration');
	}

	this.setTypeDependentStuff();

	this.views = new couchlikeViews.CouchlikeViews(this);
	this.changes = new couchlikeChanges.CouchlikeChanges(this);
	this.changes.on('change', function(change) {
		that.emit('change', change);
	});
}

util.inherits(Couchlike, EventEmitter);

function engineTest(couch, methods, configType, callback) {
	if (!couch.engine) {
		callback(new Error('Engine not configured'));
		return false;
	} else if (!couch.config || !configType) {
		callback(new Error('Invalid config'));
		return false;
	} else if (!methods[configType]) {
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

function bulkDocumentIdsTestForGet(documentIds, callback) {
	if (!documentIds.forEach) {
		callback(new Error('Must be an array'));
		return false;
	} else { return true; }
}

Couchlike.prototype.isCouchbasey = function() {
	return this._isCouchbasey;
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
	this.configType(function(err, configType) {
		if (err) { return callback(err); }
		if (!engineTest(that, methods, configType, callback)) { return null; }
		methods[configType]();
	});
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
				var useStatusCode = err.statusCode ? err.statusCode : err.status_code ? err.status_code : err.status ? err.status : 0;
				if (useStatusCode === 404) { return callback(null, null); }
				// workarround for https://github.com/couchbase/sync_gateway/issues/844
				if (useStatusCode === 500 && err.reason && err.reason.indexOf && err.reason.indexOf('404') > 0 ) { return callback(null, null); }
				callback(err);
			} else { throw err; }
		} else {
			if (!response._id) { response._id = id;	}
			if (result.cas && !response._rev) { response._rev = result.cas;	}
			if (callback) { callback(null, response); }
		}
	}

	var that = this;
	var methods = {
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
				method: 'GET'
			};
			that.engine.request(req, parseResult);
		}
	};
	this.configType(function(err, configType) {
		if (err) { return callback(err); }
		if (!engineTest(that, methods, configType, callback)) { return null; }
		methods[configType]();
	});
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
				method: 'PUT'
			};
			that.engine.request(req, function(err, result) {
				if (result) { document._rev = result.rev; }
				callback(err, document);
			});
		}
	};
	this.configType(function(err, configType) {
		if (err) { return callback(err); }
		if (!engineTest(that, methods, configType, callback)) { return null; }
		if (!documentTestForSet(document, callback)) { return null; }
		methods[configType]();
	});
};

/**
 * @desc TODO
 * @param {CouchlikeDocumentIds} documentIds - TODO
 * @param {BulkGetCallback=} callback
 * @example
 * TODO
 */
Couchlike.prototype.bulkGet = function(documentIds, callback) {
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
		couchDB: function() {
			that.engine.bucket.fetch({keys: documentIds}, parseResult);
		},
		pouchDB: function() {
			that.engine.allDocs({keys: documentIds, include_docs: true}, parseResult);
		},
		couchbaseSyncGateway: function() {
			that.engine.bucket.fetch({keys: documentIds}, parseResult);
		}
	};
	this.configType(function(err, configType) {
		if (err) { return callback(err); }
		if (!engineTest(that, methods, configType, callback)) { return null; }
		if (!bulkDocumentIdsTestForGet(documentIds, callback)) { return null; }
		methods[configType]();
	});
};

/**
 * @desc TODO
 * @param {CouchlikeDocuments} documents - TODO
 * @param {BulkSetCallback=} callback
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
	this.configType(function(err, configType) {
		if (err) { return callback(err); }
		if (!engineTest(that, methods, configType, callback)) { return null; }
		if (!bulkDocumentsTestForSet(documents, callback)) { return null; }
		methods[configType]();
	});
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
	this.configType(function(err, configType) {
		if (err) { return callback(err); }
		if (!engineTest(that, methods, configType, callback)) { return null; }
		methods[configType]();
	});
};

exports.Couchlike = Couchlike;
exports.engineType = engineType;
