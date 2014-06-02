var EventEmitter = require('events').EventEmitter;
var util = require('util');
var couchbase = require('couchbase');
var defaultBatchSize = 10;

/**
 * @callback GetByViewCallback
 * @desc TODO
 * @param {undefined|Error} error - If an error occurred during the operation it is returned here.
 */
/**
 * @callback GetDesignDocCallback
 * @desc TODO
 * @param {undefined|Error} error - If an error occurred during the operation it is returned here.
 */
/**
 * @callback SetDesignDocCallback
 * @desc TODO
 * @param {undefined|Error} error - If an error occurred during the operation it is returned here.
 */
/**
 * @callback GetViewCallback
 * @desc TODO
 * @param {undefined|Error} error - If an error occurred during the operation it is returned here.
 */
/**
 * @callback SetViewCallback
 * @desc TODO
 * @param {undefined|Error} error - If an error occurred during the operation it is returned here.
 */
/**
 * @callback RemoveViewCallback
 * @desc TODO
 * @param {undefined|Error} error - If an error occurred during the operation it is returned here.
 */

/**
 * @classdesc TODO
 * @desc TODO
 * @constructor
 * @augments EventEmitter
 * @param {Couchlike} couchlike - TODO
 * @example
 * TODO
 */
function CouchlikeViews(couchlike) {
	this.couchlike = couchlike;
}

util.inherits(CouchlikeViews, EventEmitter);

function callbackOrThrowError(err, callback) {
	if (err) {
		if (callback) { return callback(err); }
		throw err;
	}
}

function couchbaseView(engine, designDocId, viewName) {
	var defaultSettings = {
		limit: defaultBatchSize,
		include_docs : true,
		stale: false
	};
	return engine.view(designDocId, viewName, defaultSettings);
}

function couchbaseViewWithParams (engine, designDocId, viewName, params) {
	var useView = couchbaseView(engine, designDocId, viewName);
	return (useView && params) ? useView.clone(params) : useView;
}

function queryCouchbaseView (engine, view, enumeration, callback) {
	var response = [];

	var handleResults = function(results) {
		if (results) {
			results.forEach(function(result) {
				if (result.doc && result.doc.json && result.doc.meta) {
					var retrievedDoc = result.doc.json;
					if (!retrievedDoc._id) { retrievedDoc._id = result.doc.meta.id;	}
//					if (FALSE! && !retrievedDoc._rev) { retrievedDoc._rev = WHAT?;	} // Currently no way to retrieve cas from views!
					if (enumeration) { enumeration(result.doc.json); }
					if (callback) { response.push(result.doc.json); }
				}
			});
		}
	};

	var handleQueryResponse = function(err, results, paginator) {
		var doHandleQueryResponse = function(err, results) {
			if (err) { return callbackOrThrowError(err, callback); }
			handleResults(results);
			if (paginator && paginator.hasNext) {
				if (paginator.hasNext()) {
					paginator.next(doHandleQueryResponse);
				} else if (callback) {
					callback(null, response);
				}
			}
		};
		doHandleQueryResponse(err, results);
	};
	view.firstPage(handleQueryResponse);
}

function queryCouchDBView (db, designDocId, viewName, params, enumeration, callback) {
	if (!params) { params = {}; }
	params.include_docs = true;
	db.view(designDocId, viewName, params, function(err, body) {
		if (err) {
			return callbackOrThrowError(err, callback);
		} else {
			var response = [];
			body.rows.forEach(function(row) {
				if (row.doc) {
					var useDoc = row.doc.json ? row.doc.json : row.doc;
					if (!useDoc._id && row.doc.meta) { useDoc._id = row.doc.meta.id;	}

					if (enumeration) { enumeration(useDoc); }
					if (callback) { response.push(useDoc); }
				}
			});

			if (callback) { callback(null, response); }
		}
	});
}

function queryPouchDBView (couchlike, designDocId, viewName, params, enumeration, callback) {
	if (!params) { params = {}; }
	params.include_docs = true;
	var useView = designDocId+'/'+viewName;
	couchlike.engine.query(useView, params, function(err, body) {
		if (err) {
			return callbackOrThrowError(err, callback);
		} else {
			var response = [];
			body.rows.forEach(function(row) {
				if (enumeration) { enumeration(row.doc); }
				if (callback) { response.push(row.doc); }
			});

			if (callback) { callback(null, response); }
		}
	});
}

function queryCouchbaseSyncGatewayView (db, designDocId, viewName, params, enumeration, callback) {
	queryCouchDBView(db, designDocId, viewName, params, enumeration, callback);
}

/**
 * @desc TODO
 * @param {GetByViewCallback} callback
 * @example
 * TODO
 */
CouchlikeViews.prototype.getByView = function(designDocId, viewName, params, enumeration, callback) {
	var that = this;
	var methods = {
		mock: function() {
			var useView = couchbaseViewWithParams(that.couchlike.engine, designDocId, viewName, params);
			queryCouchbaseView(that.couchlike.engine, useView, enumeration, callback);
		},
		couchbase: function() {
			var useView = couchbaseViewWithParams(that.couchlike.engine, designDocId, viewName, params);
			queryCouchbaseView(that.couchlike.engine, useView, enumeration, callback);
		},
		couchDB: function() {
			queryCouchDBView(that.couchlike.engine.bucket, designDocId, viewName, params, enumeration, callback);
		},
		pouchDB: function() {
			queryPouchDBView(that.couchlike, designDocId, viewName, params, enumeration, callback);
		},
		couchbaseSyncGateway: function() {
			if (!that.couchlike.engine.couchbase || !that.couchlike.engine.couchbase.bucket) { return callback(new Error('Cannot use views on couchbaseSyncGateway configuration with no couchbase config')); }
			if (!params) { params = {}; }
			params.stale = false;
			queryCouchbaseSyncGatewayView(that.couchlike.engine.couchbase.bucket, designDocId, viewName, params, enumeration, callback);
		}
	};
	methods[this.couchlike.config.type]();
};

function fullDesignDocId(designDocId) {
	return '_design/'+designDocId;
}

function designDocName(designDocId) {
	var vals = designDocId.split('/');
	if (vals.length === 2 && vals[0] === '_design') {
		return vals[1];
	} else { return null; }
}

function couchbaseGetDesignDoc(engine, designDocId, callback) {
	engine.getDesignDoc(designDocId, function(err, result){
		if ((err) && ((err.message === 'not_found') || (err.code === couchbase.errors.restError && (err.reason === 'deleted' || err.reason === 'missing')))) { err = null; }
		callback(err, result);
	});
}

function couchDBGetDesignDoc(couchlike, designDocId, callback) {
	couchlike.get(fullDesignDocId(designDocId), callback);
}

function couchbaseSyncGatewayGetDesignDoc(db, designDocId, callback) {
	db.get('_design/'+designDocId, function(err, result){
		if (err && (err.code === couchbase.errors.keyNotFound || err.status === 404 || err.status_code === 404)) { err = null; }
		callback(err, result);
	});
}

/**
 * @desc TODO
 * @param {GetDesignDocCallback} callback
 * @example
 * TODO
 */
CouchlikeViews.prototype.getDesignDoc = function(designDocId, callback) {
	var that = this;
	var methods = {
		mock: function() {
			couchbaseGetDesignDoc(that.couchlike.engine, designDocId, callback);
		},
		couchbase: function() {
			couchbaseGetDesignDoc(that.couchlike.engine, designDocId, callback);
		},
		couchDB: function() {
			couchDBGetDesignDoc(that.couchlike, designDocId, callback);
		},
		pouchDB: function() {
			couchDBGetDesignDoc(that.couchlike, designDocId, callback);
		},
		couchbaseSyncGateway: function() {
			if (!that.couchlike.engine.couchbase || !that.couchlike.engine.couchbase.bucket) { return callback(new Error('Cannot use views on couchbaseSyncGateway configuration with no couchbase config')); }
			couchbaseSyncGatewayGetDesignDoc(that.couchlike.engine.couchbase.bucket, designDocId, callback);
		}
	};
	methods[this.couchlike.config.type]();
};

function couchbaseSetDesignDoc(engine, designDoc, callback) {
	engine.setDesignDoc(designDocName(designDoc._id), designDoc, callback);
}

function couchDBSetDesignDoc(couchlike, designDoc, callback) {
	couchlike.set(designDoc, callback);
}

function couchbaseSyncGatewaySetDesignDoc(engine, designDoc, callback) {
	var req = {
		db: engine.bucket.config.db,
		path: designDoc._id,
		body: designDoc,
		method: 'put'
	};
	engine.request(req, callback);
}

/**
 * @desc TODO
 * @param {SetDesignDocCallback} callback
 * @example
 * TODO
 */
CouchlikeViews.prototype.setDesignDoc = function(designDoc, callback) {
	var that = this;
	var methods = {
		mock: function() {
			couchbaseSetDesignDoc(that.couchlike.engine, designDoc, callback);
		},
		couchbase: function() {
			couchbaseSetDesignDoc(that.couchlike.engine, designDoc, callback);
		},
		couchDB: function() {
			couchDBSetDesignDoc(that.couchlike, designDoc, callback);
		},
		pouchDB: function() {
			couchDBSetDesignDoc(that.couchlike, designDoc, callback);
		},
		couchbaseSyncGateway: function() {
			if (!that.couchlike.engine.couchbase || !that.couchlike.engine.couchbase.bucket) { return callback(new Error('Cannot use views on couchbaseSyncGateway configuration with no couchbase config')); }
			couchbaseSyncGatewaySetDesignDoc(that.couchlike.engine.couchbase, designDoc, callback);
		}
	};
	methods[this.couchlike.config.type]();
};

/**
 * @desc TODO
 * @param {GetViewCallback} callback
 * @example
 * TODO
 */
CouchlikeViews.prototype.get = function(designDocId, viewName, callback) {
	this.couchlike.views.getDesignDoc(designDocId, function(err, result){
		if (err) { return callbackOrThrowError(err, callback); }
		if (!result || !result.views) { return callback(); }
		callback(null, result.views[viewName]);
	});
};

function updateViewInDesignDoc(designDoc, viewName, map) {
	var view = { map: map.toString() };
	designDoc.views[viewName] = view;
}

function removeViewInDesignDoc(designDoc, viewName) {
	delete designDoc.views[viewName];
}

/**
 * @desc TODO
 * @param {SetViewCallback} callback
 * @example
 * TODO
 */
CouchlikeViews.prototype.set = function(designDocId, viewName, map, callback) {
	var that = this;
	that.couchlike.views.getDesignDoc(designDocId, function(err, result){
		if (err) { return callbackOrThrowError(err, callback); }
		if (!result) { result = { _id: fullDesignDocId(designDocId), language: "javascript", views: {} }; }
		updateViewInDesignDoc(result, viewName, map);
		that.couchlike.views.setDesignDoc(result, callback);
	});
};

/**
 * @desc TODO
 * @param {RemoveViewCallback} callback
 * @example
 * TODO
 */
CouchlikeViews.prototype.remove = function(designDocId, viewName, callback) {
	var that = this;
	that.couchlike.views.getDesignDoc(designDocId, function(err, result){
		if (err) { return callbackOrThrowError(err, callback); }
		if (!result) { return callback(); }
		removeViewInDesignDoc(result, viewName);
		that.couchlike.views.setDesignDoc(result, callback);
	});
};

exports.CouchlikeViews = CouchlikeViews;
