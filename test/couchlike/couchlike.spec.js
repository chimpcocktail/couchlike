var should = require("should");
var couchlike = require('./../../lib/couchlike.js');

function check(done, checkFunction) {
	try {
		checkFunction();
		done();
	} catch(error) {
		done(error);
	}
}

describe('couchlike', function(){
	it('should exist', function(){
		should.exist(couchlike);
		should.exist(couchlike.Couchlike);
	});
});

function testWithConfig(configSpec) {
	describe('an instance of Couchlike with config '+configSpec.name, function(){
		var couch;

		before(function() {
			couch = new couchlike.Couchlike(configSpec.config);
		});

		it('should be possible', function(){
			should.exist(couch);
		});

		describe('#ping', function() {
			it('should succeed', function(done){
				couch.ping(function(err, result) {
					check(done, function() {
						should.not.exist(err);
						should.exist(result);
					});
				});
			});
		});
	});
}

var configs = {
	'null': null,
/*
	couchbaseConfig: {
		couchlike: {
			type: couchlike.engineType.couchbase
		},
		config: {
			host: 'localhost:8091',
			bucket: 'test'
		}
	},
	couchDB: {
		couchlike: {
			type: couchlike.engineType.couchDB
		},
		config: {
			host: 'http://localhost:5984'
		}
	},
*/
	pouchDB: {
		couchlike: {
			type: couchlike.engineType.pouchDB
		},
		config: {
			database: 'testdata'
		}
	}
};

for (var config in configs) {
	var test = {name: config, config: configs[config]};
	testWithConfig(test);
}

