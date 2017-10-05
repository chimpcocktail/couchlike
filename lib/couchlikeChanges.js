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

function isFunction(obj) {
	return Object.prototype.toString.call(obj) === '[object Function]';
}

function nanoFollow(couchlikeChanges, since, options, callback) {
	if (isFunction(options)) {
		callback = options;
		options = null;
	}
	if (!options) { options = { excludeDeletionsAndEmitConflicts: false }; }
	var feedOptions = {
		since: since,
		include_docs: true
	};
	if (options.excludeDeletionsAndEmitConflicts) {
		feedOptions.style = 'all_docs';
		feedOptions.query_params = { active_only: true };
	}

	var feed = couchlikeChanges.couchlike.engine.bucket.follow(feedOptions);
	feed.on('change', function (change) {
		if (options.excludeDeletionsAndEmitConflicts && change && change.changes && change.changes.length > 1) {
			couchlikeChanges.emit('conflict', change);
		} else {
			couchlikeChanges.emit('change', change);
		}
	});
	feed.follow();
	if (callback) { callback(null, feed); }
}

function pouchFollow(couchlikeChanges, since, options, callback) {
	if (isFunction(options)) {
		callback = options;
		options = null;
	}
	if (!options) { options = { excludeDeletionsAndEmitConflicts: false }; }
	var feedOptions = {
		since: since,
		include_docs: true,
		live: true
	};
	if (options.excludeDeletionsAndEmitConflicts) {
		feedOptions.style = 'all_docs';
		feedOptions.conflicts = true;
	}

	var feed = couchlikeChanges.couchlike.engine.changes(feedOptions);
	feed.on('change', function (change) {
		if (options.excludeDeletionsAndEmitConflicts && change && change.doc && change.doc._conflicts && change.doc._conflicts.length > 0) {
			couchlikeChanges.emit('conflict', change);
		} else {
			couchlikeChanges.emit('change', change);
		}
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
CouchlikeChanges.prototype.follow = function(since, options, callback) {
	if (isFunction(options)) {
		callback = options;
		options = null;
	}
	if (!options) { options = { excludeDeletionsAndEmitConflicts: false }; }
	var that = this;
	var methods = {
		couchDB: function() {
			nanoFollow(that, since, options, callback);
		},
		pouchDB: function() {
			pouchFollow(that, since, options, callback);
		},
		couchbaseSyncGateway: function() {
			nanoFollow(that, since, options, callback);
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
