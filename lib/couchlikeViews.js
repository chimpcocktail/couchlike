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

function couchbaseView(couchlike, designDoc, viewName) {
	var defaultSettings = {
		limit: defaultBatchSize,
		include_docs : true,
		stale: false
	};
	return couchlike.engine.view(designDoc, viewName, defaultSettings);
}

function couchbaseViewWithParams (couchlike, designDoc, viewName, params) {
	var useView = couchbaseView(couchlike, designDoc, viewName);
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

function queryCouchDBView (db, designDoc, viewName, params, enumeration, callback) {
	if (!params) { params = {}; }
	params.include_docs = true;
	db.view(designDoc, viewName, params, function(err, body) {
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

function queryPouchDBView (couchlike, database, designDoc, viewName, params, enumeration, callback) {
	if (!params) { params = {}; }
	params.include_docs = true;
	var useView = designDoc+'/'+viewName;
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

CouchlikeViews.prototype.getByView = function(database, designDoc, viewName, params, enumeration, callback) {
	var that = this;
	var methods = {
		mock: function() {
			var useView = couchbaseViewWithParams(that.couchlike, designDoc, viewName, params);
			queryCouchbaseView(that.couchlike, useView, enumeration, callback);
		},
		couchbase: function() {
			var useView = couchbaseViewWithParams(that.couchlike, designDoc, viewName, params);
			queryCouchbaseView(that.couchlike, useView, enumeration, callback);
		},
		couchDB: function() {
			var db = that.couchlike.engine.use(database);
			queryCouchDBView(db, designDoc, viewName, params, enumeration, callback);
		},
		pouchDB: function() {
			queryPouchDBView(that.couchlike, database, designDoc, viewName, params, enumeration, callback);
		}
	};
	methods[this.couchlike.config.couchlike.type]();
};

function designDocId(designDoc) {
	return '_design/'+designDoc;
}

function designDocName(designDocId) {
	var vals = designDocId.split('/');
	if (vals.length === 2 && vals[0] === '_design') {
		return vals[1];
	} else { return null; }
}

function couchbaseGetDesignDoc(couchlike, database, designDoc, callback) {
	couchlike.engine.getDesignDoc(designDoc, function(err, result){
		if ((err) && ((err.message === 'not_found') || (err.code === couchbase.errors.restError && (err.reason === 'deleted' || err.reason === 'missing')))) { err = null; }
		callback(err, result);
	});
}

function couchDBGetDesignDoc(couchlike, database, designDoc, callback) {
	couchlike.get(designDocId(designDoc), database, callback);
}

CouchlikeViews.prototype.getDesignDoc = function(database, designDoc, callback) {
	var that = this;
	var methods = {
		mock: function() {
			couchbaseGetDesignDoc(that.couchlike, database, designDoc, callback);
		},
		couchbase: function() {
			couchbaseGetDesignDoc(that.couchlike, database, designDoc, callback);
		},
		couchDB: function() {
			couchDBGetDesignDoc(that.couchlike, database, designDoc, callback);
		},
		pouchDB: function() {
			couchDBGetDesignDoc(that.couchlike, database, designDoc, callback);
		}
	};
	methods[this.couchlike.config.couchlike.type]();
};

function couchbaseSetDesignDoc(couchlike, database, designDoc, callback) {
	couchlike.engine.setDesignDoc(designDocName(designDoc._id), designDoc, callback);
}

function couchDBSetDesignDoc(couchlike, database, designDoc, callback) {
	couchlike.set(designDoc, database, callback);
}

CouchlikeViews.prototype.setDesignDoc = function(database, designDoc, callback) {
	var that = this;
	var methods = {
		mock: function() {
			couchbaseSetDesignDoc(that.couchlike, database, designDoc, callback);
		},
		couchbase: function() {
			couchbaseSetDesignDoc(that.couchlike, database, designDoc, callback);
		},
		couchDB: function() {
			couchDBSetDesignDoc(that.couchlike, database, designDoc, callback);
		},
		pouchDB: function() {
			couchDBSetDesignDoc(that.couchlike, database, designDoc, callback);
		}
	};
	methods[this.couchlike.config.couchlike.type]();
};

CouchlikeViews.prototype.get = function(database, designDoc, viewName, callback) {
	this.couchlike.views.getDesignDoc(database, designDoc, function(err, result){
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

CouchlikeViews.prototype.set = function(database, designDoc, viewName, map, callback) {
	var that = this;
	var docId = designDocId(designDoc);
	that.couchlike.views.getDesignDoc(database, designDoc, function(err, result){
		if (err) { return callbackOrThrowError(err, callback); }
		if (!result) { result = { _id: docId, language: "javascript", views: {} }; }
		updateViewInDesignDoc(result, viewName, map);
		that.couchlike.views.setDesignDoc(database, result, callback);
	});
};

CouchlikeViews.prototype.remove = function(database, designDoc, viewName, callback) {
	var that = this;
	var docId = designDocId(designDoc);
	that.couchlike.views.getDesignDoc(database, designDoc, function(err, result){
		if (err) { return callbackOrThrowError(err, callback); }
		if (!result) { return callback(); }
		removeViewInDesignDoc(result, viewName);
		that.couchlike.views.setDesignDoc(database, result, callback);
	});
};

exports.CouchlikeViews = CouchlikeViews;
