var EventEmitter = require('events').EventEmitter;
var util = require('util');
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

function couchbaseView(couchlike, designDoc, view) {
	var defaultSettings = {
		limit: defaultBatchSize,
		include_docs : true,
		stale: false
	};
	return couchlike.engine.view(designDoc, view, defaultSettings);
}

function couchbaseViewWithParams (couchlike, designDoc, view, params) {
	var useView = couchbaseView(couchlike, designDoc, view);
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

CouchlikeViews.prototype.get = function(designDoc, view, params, enumeration, callback) {
	var that = this;
	var methods = {
		mock: function() {
			callback(new Error('Not Implemented!'));
		},
		couchbase: function() {
			var useView = couchbaseViewWithParams(that.couchlike, designDoc, view, params);
			queryCouchbaseView(that.couchlike, useView, enumeration, callback);
		},
		couchDB: function() {
			callback(new Error('Not Implemented!'));
		},
		pouchDB: function() {
			callback(new Error('Not Implemented!'));
		}
	};
	methods[this.couchlike.config.couchlike.type]();
};

exports.CouchlikeViews = CouchlikeViews;
