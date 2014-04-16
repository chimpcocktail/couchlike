var EventEmitter = require('events').EventEmitter;
var util = require('util');
var couchbase = require('couchbase');
var nano = require('nano');
var PouchDB = require('pouchdb');

var engineType = {
	mock: "mock",
	couchbase: "couchbase",
	couchDB: "couchDB",
	pouchDB: "pouchDB"
};

function Couchlike(config) {
	this.config = config ? config : { couchlike: { type: engineType.mock } };

	this.engine = null;
	if (this.config.couchlike.type === engineType.mock) {
		this.engine = new couchbase.Mock.Connection();
	} else if (this.config.couchlike.type === engineType.couchbase) {
		this.engine = new couchbase.Connection(this.config.config);
	} else if (this.config.couchlike.type === engineType.couchDB) {
		this.engine = nano(this.config.config.host);
	} else if (this.config.couchlike.type === engineType.pouchDB) {
		this.engine = new PouchDB(this.config.config.database);
	} else {
		throw new Error('Unrecognised configuration');
	}

}

util.inherits(Couchlike, EventEmitter);

function engineTest(couch, methods, callback) {
	if (!couch.engine) {
		callback(new Error('Engine not configured'));
		return false;
	} else if (!couch.config.couchlike || !couch.config.couchlike.type) {
		callback(new Error('Invalid config'));
		return false;
	} else if (!methods[couch.config.couchlike.type]) {
		callback(new Error('Method not configured'));
		return false;
	} else { return true; }
}

Couchlike.prototype.ping = function(callback) {
	var that = this;
	var methods = {
		mock: function() {
			if (callback) { callback(null, { ping: "ok" }); }
		},
		couchbase: function() {
			that.engine.stats(callback);
		},
		couchDB: function() {
			that.engine.request({}, callback);
		},
		pouchDB: function() {
			if (callback) { callback(null, { ping: "ok" }); }
		}
	};
	if (!engineTest(this, methods, callback)) { return null; }
	methods[this.config.couchlike.type]();
};

exports.Couchlike = Couchlike;
exports.engineType = engineType;
