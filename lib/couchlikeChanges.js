var EventEmitter = require('events').EventEmitter;
var util = require('util');
var couchbase = require('couchbase');

function CouchlikeChanges(couchlike) {
	this.couchlike = couchlike;
}

util.inherits(CouchlikeChanges, EventEmitter);

function nanoFollow(couchlikeChanges, since, callback) {
	var feed = couchlikeChanges.couchlike.engine.bucket.follow({since: since, include_docs: true});
	feed.on('change', function (change) {
		couchlikeChanges.emit('change', change);
	});
	feed.follow();
	if (callback) { callback(null, feed); }
}

function pouchFollow(couchlikeChanges, since, callback) {
	var feed = couchlikeChanges.couchlike.engine.changes({since: since, include_docs: true, live: true});
	feed.on('change', function (change) {
		couchlikeChanges.emit('change', change);
	});
	if (callback) { callback(null, feed); }
}

CouchlikeChanges.prototype.follow = function(since, callback) {
	var that = this;
	var methods = {
		mock: function() {
			callback(new Error('Not implemented'));
		},
		couchbase: function() {
			callback(new Error('Not implemented'));
		},
		couchDB: function() {
			nanoFollow(that, since, callback);
		},
		pouchDB: function() {
			pouchFollow(that, since, callback);
		},
		couchbaseSyncGateway: function() {
			nanoFollow(that, since, callback);
		}
	};
	methods[this.couchlike.config.couchlike.type]();
};

function nanoUnfollow(feed, callback) {
	if (feed) { feed.stop('stop'); }
	if (callback) { callback(); }
}

function pouchUnfollow(feed, callback) {
	if (feed) { feed.cancel(); }
	if (callback) { callback(); }
}

CouchlikeChanges.prototype.unfollow = function(feed, callback) {
	var methods = {
		mock: function() {
			callback(new Error('Not implemented'));
		},
		couchbase: function() {
			callback(new Error('Not implemented'));
		},
		couchDB: function() {
			nanoUnfollow(feed, callback);
		},
		pouchDB: function() {
			pouchUnfollow(feed, callback);
		},
		couchbaseSyncGateway: function() {
			nanoUnfollow(feed, callback);
		}
	};
	methods[this.couchlike.config.couchlike.type]();
};

exports.CouchlikeChanges = CouchlikeChanges;
