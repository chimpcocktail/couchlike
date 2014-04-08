var EventEmitter = require('events').EventEmitter;
var util = require('util');

function Couchlike(config) {
	this.config = config;
}

util.inherits(Couchlike, EventEmitter);

exports.Couchlike = Couchlike;
