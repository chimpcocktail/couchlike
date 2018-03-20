var EventEmitter = require('events').EventEmitter;
var url = require('url');
var util = require('util');
var async = require('async');
var couchlikeViews = require('./couchlikeViews');
var couchlikeChanges = require('./couchlikeChanges');
var nano = require('nano');
var PouchDB = require('pouchdb');
var KeepAliveAgent = require('agentkeepalive');
var couchbase = require('couchbase');

const DEFAULT_MAX_SOCKETS = 100;

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
	couchbaseSyncGateway: {
		changes: true,
		views: true,
		viewIncludeDocs: false
	}
};

var DEFAULT_COUCHDB_PORT = 5984;
var DEFAULT_COUCHBASESYNCGATEWAY_PORT = 4984;

function looksLikePouchDBInstance(thing) {
	return thing && thing.adapter && thing.replicate && thing.prefix === '_pouch_';
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

	var useAgent = new KeepAliveAgent({ maxSockets: this.config.connection.maxSockets ? this.config.connection.maxSockets : DEFAULT_MAX_SOCKETS });
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
			nanoConfig.requestDefaults = { strictSSL: this.config.connection.strictSSL };
			if (!this.config.connection.strictSSL) { process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0"; }
		} else if (this.config.connection.ca !== (void 0)) {
			nanoConfig.requestDefaults = { ca: this.config.connection.ca };
		} else { nanoConfig.requestDefaults = {}; }
		nanoConfig.requestDefaults.agent = useAgent;
		this.engine = nano(nanoConfig);
		this.engine.bucket = this.engine.use(this.config.connection.bucket);
	} else if (this.config && this.config.type === engineType.pouchDB) {
		this.engine = new PouchDB(this.config.connection.bucket);
	} else if (looksLikePouchDBInstance(this.config)) {
		this.engine = this.config;
		this.config = { type: engineType.pouchDB };
	} else {
		throw new Error('Unrecognised configuration');
	}

	if (this.config && this.config.connection && this.config.connection.direct) {
		this.couchbase = new couchbase.Cluster(this.config.connection.direct.host);
		this.couchbaseBucket = this.couchbase.openBucket(this.config.connection.bucket);
		this.couchbaseBucket.operationTimeout = 100000;
		this.couchbaseBucket.viewTimeout = 100000;
		this.couchbaseBucket.httpAgent = useAgent;
	}

	this.setTypeDependentStuff();

	this.views = new couchlikeViews.CouchlikeViews(this);
	this.changes = new couchlikeChanges.CouchlikeChanges(this);
	this.changes.on('change', function(change) {
		that.emit('change', change);
	});
	this.changes.on('conflict', function(conflict) {
		that.emit('conflict', conflict);
	});
}

util.inherits(Couchlike, EventEmitter);

Couchlike.prototype.close = function(callback) {
	if (this.couchbaseBucket) { this.couchbaseBucket.disconnect(); }
	if (callback) { return callback(); }
};

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

function isLocalDoc(id) {
	return (id && id.startsWith('_local/'));
}

function isDesignDoc(id) {
	return (id && id.startsWith('_design/'));
}

function isUserDoc(id) {
	return (id && id.startsWith('_user/'));
}

function isRoleDoc(id) {
	return (id && id.startsWith('_role/'));
}

function isSpecialLocalUnderscoreDoc(id) {
	return (isLocalDoc(id) || isDesignDoc(id) || isUserDoc(id) || isRoleDoc(id));
}

// RESPONSES ============================================
// Get
function couchlikeResolveGetDoc(doc, useId, callback) {
	if (!doc) { if (callback) { return callback(null, null); } else { return; }}
	if (!doc._id) { doc._id = useId; }
	if (doc._sync) {
		doc._rev = doc._sync.rev;
		delete doc._sync;
	}
	if (callback) { return callback(null, doc); }
	else { return doc; }
}

function handleGetError(err, callback) {
	if (callback) {
		var useStatusCode = err.statusCode ? err.statusCode : err.status_code ? err.status_code : err.status ? err.status : 0;
		if (useStatusCode === 404) { return callback(null, null); }
		// workarounds for https://github.com/couchbase/sync_gateway/issues/844
		if (useStatusCode === 500 && err.reason && err.reason.indexOf && (err.reason.indexOf('404') > 0 || err.reason.indexOf('not_found') > 0) ) { return callback(null, null); }
		var couchbaseCode = err.code;
		if (couchbaseCode === 13) { return callback(null, null); } // The key does not exist on the server
		callback(err);
	} else { throw err; }
}

function handleGetSuccess(result, useId, callback) {
	if (callback) {
		var response = (result && result.value) ? result.value : result;
		if (Array.isArray(response)) {
			callback(null, response.map(function(row) { return couchlikeResolveGetDoc(row.ok, useId); }));
		} else {
			if (response._deleted) { return couchlikeResolveGetDoc(null, useId, callback); }
			couchlikeResolveGetDoc(response, useId, callback);
		}
	}
}

function getDocCallback(id, callback) {
	return function(err, result) {
		if (err) { handleGetError(err, callback); }
		else { handleGetSuccess(result, id, callback); }
	};
}

function getBulkCallback(isCouchbase, callback) {
	return function(err, result) {
		if (!callback) { return; }
		if (isCouchbase && result && !result.rows) {
			var response = { rows: [] };
			async.forEachOf(result, function(doc, key, callback) {
				if (!doc || !doc.value) { return callback(); }
				doc.value = couchlikeResolveGetDoc(doc.value, key);
				response.rows.push({
					key: doc.value._id,
					id: doc.value._id,
					doc: doc.value
				});
				callback();
			}, function(err) {
				callback(err, response);
			});
		} else { callback(err, result); }
	};
}

// Set
function couchlikeResolveSetDoc(doc, setResult, callback) {
	if (setResult && setResult.rev) { doc._rev = setResult.rev; }
	if (callback) { return callback(null, doc); }
	else { return doc; }
}

function handleSetError(err, callback) {
	if (callback) { return callback(err); }
	else { throw err; }
}

function handleSetSuccess(doc, setResult, callback) {
	couchlikeResolveSetDoc(doc, setResult, callback);
}

function setDocCallback(doc, callback) {
	return function(err, result) {
		if (err) { handleSetError(err, callback); }
		else { handleSetSuccess(doc, result, callback); }
	};
}

// Generic
function simpleResponseCallback(callback) {
	return function(err, result) {
		if (callback) { callback(err, result); }
	};
}

function swallowResponseCallback(callback) {
	return function(err) {
		if (callback) { callback(err); }
	};
}

Couchlike.prototype.doGet = function(options, id, callback) {
	if (!options) { options = {}; }
	var that = this;
	var methods = {
		couchDB: function() {
			that.engine.bucket.get(id, getDocCallback(id, callback));
		},
		pouchDB: function() {
			var pouchOptions = options.allRevisions ? { open_revs: 'all' } : {};
			that.engine.get(id, pouchOptions, getDocCallback(id, callback));
		},
		couchbaseSyncGateway: function() {
			if (that.couchbaseBucket && !options.allRevisions && !isSpecialLocalUnderscoreDoc(id)) { that.couchbaseBucket.get(id, getDocCallback(id, callback)); }
			else {
				/*
				that.engine.bucket.get(id, getDocCallback(id, callback));

				The following is the workaround for	https://github.com/couchbase/sync_gateway/issues/324
				Once this issue has been resolved, the above simple implementation should be reinstated.

				*/
				var req = {
					db: that.config.connection.bucket,
					path: id,
					method: 'GET'
				};
				if (options.allRevisions) {
					req.qs = { open_revs: 'all' };
				}
				that.engine.request(req, getDocCallback(id, callback));
			}
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
	this.doGet({}, id, callback);
};

Couchlike.prototype.getRevisions = function(id, callback) {
	this.doGet({ allRevisions: true }, id, callback);
};

Couchlike.prototype.doSet = function(options, document, callback) {
	if (!options) { options = {}; }
	var that = this;
	var methods = {
		couchDB: function() {
			that.engine.bucket.insert(document, setDocCallback(document, callback));
		},
		pouchDB: function() {
			var pouchOptions = options.force ? { new_edits: false } : {};
			that.engine.put(document, pouchOptions, setDocCallback(document, callback));
		},
		couchbaseSyncGateway: function() {
			/*
			that.engine.bucket.insert(document, document._id, setDocCallback(document, callback));

			The following is the workaround for	https://github.com/couchbase/sync_gateway/issues/324
			Once this issue has been resolved, the above simple implementation should be reinstated.

			*/
			var req = {
				db: that.config.connection.bucket,
				path: document._id,
				body: document,
				method: 'PUT'
			};
			if (options.force) { req.qs = { new_edits: false }; }
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
 * @param {CouchlikeDocument} document - TODO
 * @param {SetCallback=} callback
 * @example
 * TODO
 */
Couchlike.prototype.set = function(document, callback) {
	this.doSet({}, document, callback);
};

Couchlike.prototype.force = function(document, callback) {
	this.doSet({ force: true }, document, callback);
};

/**
 * @desc TODO
 * @param {CouchlikeDocumentIds} documentIds - TODO
 * @param {BulkGetCallback=} callback
 * @example
 * TODO
 */
const BULK_THRESHOLD = 1;
Couchlike.prototype.bulkGet = function(documentIds, callback) {
	var that = this;
	var isCouchbase = this.couchbaseBucket ? true : false;
	if (!documentIds || documentIds.length === 0) { return callback(null, { rows: [], total_rows: 0 }); }
	var methods = {
		couchDB: function() {
			that.engine.bucket.fetch({keys: documentIds}, getBulkCallback(isCouchbase, callback));
		},
		pouchDB: function() {
			that.engine.allDocs({keys: documentIds, include_docs: true}, getBulkCallback(isCouchbase, callback));
		},
		couchbaseSyncGateway: function() {
			if (documentIds.length > BULK_THRESHOLD) {
				if (that.couchbaseBucket) {
					that.couchbaseBucket.getMulti(documentIds, getBulkCallback(isCouchbase, callback));
				} else {
					that.engine.bucket.fetch({keys: documentIds}, getBulkCallback(isCouchbase, callback));
				}
			} else {
				async.mapLimit(documentIds, BULK_THRESHOLD, that.get.bind(that), function(err, rows) {
					var result = {
						rows: rows.map(function(item) { return { doc: item }; } ),
						total_rows: rows.length
					};
					getBulkCallback(isCouchbase, callback)(err, result);
				});
			}
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
	var that = this;
	var methods = {
		couchDB: function() {
			that.engine.bucket.bulk({docs: documents}, simpleResponseCallback(callback));
		},
		pouchDB: function() {
			that.engine.bulkDocs(documents, simpleResponseCallback(callback));
		},
		couchbaseSyncGateway: function() {
			var req = {
				db: that.config.connection.bucket,
				path: '_bulk_docs',
				body: {docs: documents},
				method: 'POST'
			};
			that.engine.request(req, simpleResponseCallback(callback));
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
	var that = this;
	var methods = {
		couchDB: function() {
			that.engine.bucket.get(id, function(err, result){
				if (err) {
					if (callback) { callback(err, result); }
				}
				else { that.engine.bucket.destroy(result._id, result._rev, swallowResponseCallback(callback)); }
			});
		},
		pouchDB: function() {
			that.engine.get(id, function(err, result){
				if (err) {
					if (callback) { callback(err, result); }
				}
				else { that.engine.remove(result, swallowResponseCallback(callback)); }
			});
		},
		couchbaseSyncGateway: function() {
			that.engine.bucket.get(id, function(err, result){
				if (err) {
					if (callback) { callback(err, result); }
				}
				else { that.engine.bucket.destroy(result._id, result._rev, swallowResponseCallback(callback)); }
			});
		}
	};
	this.configType(function(err, configType) {
		if (err) { return callback(err); }
		if (!engineTest(that, methods, configType, callback)) { return null; }
		methods[configType]();
	});
};

Couchlike.prototype.resolve = function(resolution, callback) {
	var documents = resolution.losers.map(function(revision) {
		// revision._deleted = true;
		// return revision; // retain old data
		return { _id: revision._id, _rev: revision._rev, _deleted: true }; // remove old data
	});
	if (resolution.winner) { documents.push(resolution.winner); }
	this.bulkSet(documents, callback);
};



exports.Couchlike = Couchlike;
exports.looksLikePouchDBInstance = looksLikePouchDBInstance;
exports.engineType = engineType;
exports.PouchDB = PouchDB;
