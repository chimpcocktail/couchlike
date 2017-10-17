var EventEmitter = require('events').EventEmitter;
var util = require('util');
var async = require('async');
var defaultBatchSize = 10;
var couchlike = require('./couchlike');
var couchbase = require('couchbase');
var ViewQuery = couchbase.ViewQuery;


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

var respondFromRows = function(body, enumeration, callback) {
	var response = [];
	if (body && body.rows && body.rows.forEach) {
		body.rows.forEach(function(row) {
			if (row && row.doc) {
				var useDoc = row.doc.json ? row.doc.json : row.doc;
				if (!useDoc._id && row.doc.meta) { useDoc._id = row.doc.meta.id;	}

				if (enumeration) { enumeration(useDoc); }
				if (callback) { response.push(useDoc); }
			}
		});
	}
	if (callback) { callback(null, response); }
};

var couchlikeViewResponse = function(err, body, couchlike, enumeration, callback) {
	if (err) { return callbackOrThrowError(err, callback); }
	if (couchlike.capability.viewIncludeDocs) { respondFromRows(body, enumeration, callback); }
	else {
		var useRows = (body && body.rows) ? body.rows : body;
		if (useRows && useRows.forEach) {
			var retrieveIds = [];
			useRows.forEach(function(row) {
				if (row && row.id && row.id.indexOf('_sync') < 0) {
					retrieveIds.push(row.id);
				}
			});
			couchlike.bulkGet(retrieveIds, function(err, result) {
				if (err) { return callbackOrThrowError(err, callback); }
				respondFromRows(result, enumeration, callback);
			});
		} else if (callback) { callback(null, []); }
	}
};

var couchlikeViewResponseCallback = function(couchlike, enumeration, callback) {
	return function(err, body) {
		couchlikeViewResponse(err, body, couchlike, enumeration, callback);
	};
};

function queryCouchDBView (couchlike, designDocId, viewName, params, enumeration, callback) {
	if (!params) { params = {}; }
	params.include_docs = couchlike.capability && couchlike.capability.viewIncludeDocs;
	couchlike.engine.bucket.view(designDocId, viewName, params, couchlikeViewResponseCallback(couchlike, enumeration, callback));
}

function queryPouchDBView (couchlike, designDocId, viewName, params, enumeration, callback) {
	if (!params) { params = {}; }
	params.include_docs = true;
	var useView = designDocId+'/'+viewName;
	couchlike.engine.query(useView, params, couchlikeViewResponseCallback(couchlike, enumeration, callback));
}

function queryCouchbaseView (couchlike, designDocId, viewName, params, enumeration, callback) {
	if (!params) { params = {}; }
	var view = new couchbase.ViewQuery.from(designDocId, viewName);
	var customOptions = {stale: false};
	if (params.startkey) { customOptions.startkey = '"'+params.startkey+'"'; }
	if (params.endkey) { customOptions.endkey = '"'+params.endkey+'"'; }
	if (params.key) { customOptions.key = '"'+params.key+'"'; }
	view.custom(customOptions);
	couchlike.couchbaseBucket.query(view, couchlikeViewResponseCallback(couchlike, enumeration, callback));

	// // under very heavy load, Couchbase tends to die with "Error: unknown error : error parsing failed". This retry can help with that!
	// async.retry({times: 3, interval: 10000}, function(callback, results) {
	// 	couchlike.couchbaseBucket.query(view, callback);
	// }, couchlikeViewResponseCallback(couchlike, enumeration, callback));
}

function queryCouchbaseSyncGatewayView (couchlike, designDocId, viewName, params, enumeration, callback) {
	if (couchlike.couchbaseBucket) { queryCouchbaseView(couchlike, designDocId, viewName, params, enumeration, callback); }
	else { queryCouchDBView(couchlike, designDocId, viewName, params, enumeration, callback); }
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
		couchDB: function() {
			queryCouchDBView(that.couchlike, designDocId, viewName, params, enumeration, callback);
		},
		pouchDB: function() {
			queryPouchDBView(that.couchlike, designDocId, viewName, params, enumeration, callback);
		},
		couchbaseSyncGateway: function() {
			if (!params) { params = {}; }
			params.stale = false;
			queryCouchbaseSyncGatewayView(that.couchlike, designDocId, viewName, params, enumeration, callback);
		}
	};
	this.couchlike.configType(function(err, configType) {
		if (err) { return callback(err); }
		methods[configType]();
	});
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

function couchDBGetDesignDoc(couchlike, designDocId, callback) {
	couchlike.get(fullDesignDocId(designDocId), callback);
}

function couchbaseSyncGatewayGetDesignDoc(db, designDocId, callback) {
	db.get('_design/'+designDocId, function(err, result){
		if (err && (err.status === 404 || err.status_code === 404 || err.statusCode === 404)) { err = null; }
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
		couchDB: function() {
			couchDBGetDesignDoc(that.couchlike, designDocId, callback);
		},
		pouchDB: function() {
			couchDBGetDesignDoc(that.couchlike, designDocId, callback);
		},
		couchbaseSyncGateway: function() {
			couchDBGetDesignDoc(that.couchlike, designDocId, callback);
		}
	};
	this.couchlike.configType(function(err, configType) {
		if (err) { return callback(err); }
		methods[configType]();
	});
};

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
		couchDB: function() {
			couchDBSetDesignDoc(that.couchlike, designDoc, callback);
		},
		pouchDB: function() {
			couchDBSetDesignDoc(that.couchlike, designDoc, callback);
		},
		couchbaseSyncGateway: function() {
			couchDBSetDesignDoc(that.couchlike, designDoc, callback);
		}
	};
	this.couchlike.configType(function(err, configType) {
		if (err) { return callback(err); }
		methods[configType]();
	});
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

CouchlikeViews.prototype.eventualMapInView = function(view) {
	var syncGatewayViewPrefix = 'function(doc,meta) { var sync = doc._sync; if (sync === undefined || meta.id.substring(0,6) == "_sync:") return; if ((sync.flags & 1) || sync.deleted) return; delete doc.sync; meta.rev = sync.rev; (';
	var syncGatewayViewSuffix = ') (doc, meta); }';
	if (!view) { return ""; }
	if (this.couchlike.isCouchbasey()) {
		return syncGatewayViewPrefix + view.map.toString() + syncGatewayViewSuffix;
	} else { return view.map.toString(); }
};

CouchlikeViews.prototype.comparableNewMapInView = function(view) {
	return this.eventualMapInView(view).replace(/\s/g, "");
};

CouchlikeViews.prototype.comparableExistingMapInView = function(view) {
	if (!view || !view.map) { return ""; }
	return view.map.toString().replace(/\s/g, "");
};

CouchlikeViews.prototype.isViewChanged = function(existingView, newView) {
	if (!existingView || !existingView.map || !newView || !newView.map ) { return true; }
	return (this.comparableExistingMapInView(existingView) !== this.comparableNewMapInView(newView));
};

CouchlikeViews.prototype.updateViewInDesignDoc = function(designDoc, viewName, map) {
	var view = { map: map.toString() };
	if (this.isViewChanged(designDoc.views[viewName], view)) {
		designDoc.views[viewName] = view;
		return true;
	}
	return false;
};

function removeViewInDesignDoc(designDoc, viewName) {
	if (designDoc.views[viewName]) {
		delete designDoc.views[viewName];
		return true;
	}
	return false;
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
		if (!result) { result = { _id: fullDesignDocId(designDocId), language: "javascript" }; }
		if (!result.views) { result.views = {}; }
		if (that.updateViewInDesignDoc(result, viewName, map)) {
			that.couchlike.views.setDesignDoc(result, callback);
		} else {
			callback();
		}
	});
};

/**
 * @desc TODO
 * @param {SetViewCallback} callback
 * @example
 * TODO
 */

CouchlikeViews.prototype.setBulk = function(designDocId, views, callback) {
	var that = this;
	that.couchlike.views.getDesignDoc(designDocId, function(err, result){
		if (err) { return callbackOrThrowError(err, callback); }
		if (!result) { result = { _id: fullDesignDocId(designDocId), language: "javascript" }; }
		if (!result.views) { result.views = {}; }

		var anyChange = false;
		for (var viewName in views) {
			var existingView = result.views[viewName];
			var newView = { map: views[viewName].toString() };
			result.views[viewName] = newView;
			anyChange = anyChange || that.isViewChanged(existingView, newView);
		}
		if (!anyChange) { return callback(); }
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
		if (removeViewInDesignDoc(result, viewName)) {
			that.couchlike.views.setDesignDoc(result, callback);
		} else { callback(); }
	});
};

exports.CouchlikeViews = CouchlikeViews;
