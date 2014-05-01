var should = require("should");
var couchbase = require('couchbase');
var connection = new couchbase.Connection({	host: 'localhost:8091',	bucket: 'unit_tests' });
//var connection = new couchbase.Mock.Connection();

describe('issue with copying cas', function() {
	it('should succeed', function(done){
		var testObj = { foo: "bar" };
		connection.set('test', testObj, function(err, result) { // Insert with no cas option i.e. overwrite any existing data
			should.not.exist(err);
			connection.get('test', function(err, result) { // Retrieve the just added document
				should.not.exist(err);
				should.exist(result.cas);
				should.exist(result.value);
				result.value.foo = "bar2";
				var copiedCas = JSON.parse(JSON.stringify(result.cas));
				copiedCas.should.be.eql(result.cas); // The two cas values are equal
//				connection.set('test', result.value, { cas : copiedCas }, function(err, result) { // Doing it this way fails
				connection.set('test', result.value, { cas : result.cas }, function(err, result) { // Doing it this way succedes
					should.not.exist(err);
					should.exist(result);
					done();
				});
			});
		});
	});
});

