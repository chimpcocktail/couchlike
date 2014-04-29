var EventEmitter = require('events').EventEmitter;
var util = require('util');
var couchbase = require('couchbase');
var defaultBatchSize = 10;

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

function couchbaseView(couchlike, designDocId, viewName) {
	var defaultSettings = {
		limit: defaultBatchSize,
		include_docs : true,
		stale: false
	};
	return couchlike.engine.view(designDocId, viewName, defaultSettings);
}

function couchbaseViewWithParams (couchlike, designDocId, viewName, params) {
	var useView = couchbaseView(couchlike, designDocId, viewName);
	return (useView && params) ? useView.clone(params) : useView;
}

function queryCouchbaseView (couchlike, view, enumeration, callback) {
	var response = [];

	var handleResults = function(results) {
		if (results) {
			results.forEach(function(result) {
				if (result.doc && result.doc.json) {
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
				if (enumeration) { enumeration(row.doc); }
				if (callback) { response.push(row.doc); }
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

CouchlikeViews.prototype.getByView = function(designDocId, viewName, params, enumeration, callback) {
	var that = this;
	var methods = {
		mock: function() {
			var useView = couchbaseViewWithParams(that.couchlike, designDocId, viewName, params);
			queryCouchbaseView(that.couchlike, useView, enumeration, callback);
		},
		couchbase: function() {
			var useView = couchbaseViewWithParams(that.couchlike, designDocId, viewName, params);
			queryCouchbaseView(that.couchlike, useView, enumeration, callback);
		},
		couchDB: function() {
			queryCouchDBView(that.couchlike.engine.bucket, designDocId, viewName, params, enumeration, callback);
		},
		pouchDB: function() {
			queryPouchDBView(that.couchlike, designDocId, viewName, params, enumeration, callback);
		}
	};
	methods[this.couchlike.config.couchlike.type]();
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

function couchbaseGetDesignDoc(couchlike, designDocId, callback) {
	couchlike.engine.getDesignDoc(designDocId, function(err, result){
		if ((err) && ((err.message === 'not_found') || (err.code === couchbase.errors.restError && (err.reason === 'deleted' || err.reason === 'missing')))) { err = null; }
		callback(err, result);
	});
}

function couchDBGetDesignDoc(couchlike, designDocId, callback) {
	couchlike.get(fullDesignDocId(designDocId), callback);
}

CouchlikeViews.prototype.getDesignDoc = function(designDocId, callback) {
	var that = this;
	var methods = {
		mock: function() {
			couchbaseGetDesignDoc(that.couchlike, designDocId, callback);
		},
		couchbase: function() {
			couchbaseGetDesignDoc(that.couchlike, designDocId, callback);
		},
		couchDB: function() {
			couchDBGetDesignDoc(that.couchlike, designDocId, callback);
		},
		pouchDB: function() {
			couchDBGetDesignDoc(that.couchlike, designDocId, callback);
		}
	};
	methods[this.couchlike.config.couchlike.type]();
};

function couchbaseSetDesignDoc(couchlike, designDoc, callback) {
	couchlike.engine.setDesignDoc(designDocName(designDoc._id), designDoc, callback);
}

function couchDBSetDesignDoc(couchlike, designDoc, callback) {
	couchlike.set(designDoc, callback);
}

CouchlikeViews.prototype.setDesignDoc = function(designDoc, callback) {
	var that = this;
	var methods = {
		mock: function() {
			couchbaseSetDesignDoc(that.couchlike, designDoc, callback);
		},
		couchbase: function() {
			couchbaseSetDesignDoc(that.couchlike, designDoc, callback);
		},
		couchDB: function() {
			couchDBSetDesignDoc(that.couchlike, designDoc, callback);
		},
		pouchDB: function() {
			couchDBSetDesignDoc(that.couchlike, designDoc, callback);
		}
	};
	methods[this.couchlike.config.couchlike.type]();
};

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

CouchlikeViews.prototype.set = function(designDocId, viewName, map, callback) {
	var that = this;
	that.couchlike.views.getDesignDoc(designDocId, function(err, result){
		if (err) { return callbackOrThrowError(err, callback); }
		if (!result) { result = { _id: fullDesignDocId(designDocId), language: "javascript", views: {} }; }
		updateViewInDesignDoc(result, viewName, map);
		that.couchlike.views.setDesignDoc(result, callback);
	});
};

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
