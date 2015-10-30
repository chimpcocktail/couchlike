var EventEmitter = require('events').EventEmitter;
var util = require('util');

/**
 * @callback FollowCallback
 * @desc TODO
 * @param {undefined|Error} error - If an error occurred during the operation it is returned here.
 */
/**
 * @callback UnfollowCallback
 * @desc TODO
 * @param {undefined|Error} error - If an error occurred during the operation it is returned here.
 */
/**
 * @desc TODO
 * @event Change
 * @param {Object} change - TODO
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

/**
 * @desc TODO
 * @param {} since
 * @param {FollowCallback} callback
 * @example
 * TODO
 */
CouchlikeChanges.prototype.follow = function(since, callback) {
	var that = this;
	var methods = {
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
	this.couchlike.configType(function(err, configType) {
		if (err) { return callback(err); }
		methods[configType]();
	});
};

function nanoUnfollow(feed, callback) {
	if (feed) { feed.stop('stop'); }
	if (callback) { callback(); }
}

function pouchUnfollow(feed, callback) {
	if (feed) { feed.cancel(); }
	if (callback) { callback(); }
}

/**
 * @desc TODO
 * @param {} feed
 * @param {UnfollowCallback} callback
 * @example
 * TODO
 */
CouchlikeChanges.prototype.unfollow = function(feed, callback) {
	var methods = {
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
	this.couchlike.configType(function(err, configType) {
		if (err) { return callback(err); }
		methods[configType]();
	});
};

exports.CouchlikeChanges = CouchlikeChanges;
