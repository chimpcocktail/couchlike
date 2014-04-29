/* global emit */

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

		this.timeout(10000);
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

		var testData = {
			database: 'cochlear',
			setDocument: {
				_id: 'test_data',
				foo: 'bar'
			},
			designDocName: 'testDoc',
			viewName: 'testView',
			setView: {
				map: function(doc) {
					emit(doc._id, null);
				}
			}
		};

		describe('documents', function(){
			describe('before setting #get()', function(){
				it('should succeed', function(done){
					couch.get(testData.setDocument._id, testData.database, function(err, document) {
						testData.retrievedDocument = document;
						check(done, function() {
							should.not.exist(err);
						});
					});
				});
			});

			describe('…the retrieved document', function(){
				it('should not exist', function(){
					should.not.exist(testData.retrievedDocument);
				});
			});

			describe('#set()', function(){
				it('should succeed', function(done){
					couch.set(testData.setDocument, testData.database, function(err, result) {
						check(done, function() {
							should.not.exist(err);
							should.exist(result);
						});
					});
				});
			});

			describe('after setting #get()', function(){
				it('should succeed', function(done){
					couch.get(testData.setDocument._id, testData.database, function(err, document) {
						if (document._rev) { testData.setDocument._rev = document._rev; }
						testData.retrievedDocument = document;
						check(done, function() {
							should.not.exist(err);
						});
					});
				});
			});

			describe('…the retrieved document', function(){
				it('should exist', function(){
					should.exist(testData.retrievedDocument);
				});
				it('should match the set data', function(){
					testData.retrievedDocument.should.eql(testData.setDocument);
				});
			});
		});

		describe('views', function(){
			describe('before setting views#get()', function(){
				it('should succeed', function(done){
					couch.views.get(testData.database, testData.designDocName, testData.viewName, function(err, view) {
						testData.retrievedView = view;
						check(done, function() {
							should.not.exist(err);
						});
					});
				});
			});

			describe('…the retrieved view', function(){
				it('should not exist', function(){
					should.not.exist(testData.retrievedView);
				});
			});

			describe('views#set()', function(){
				it('should succeed', function(done){
					couch.views.set(testData.database, testData.designDocName, testData.viewName, testData.setView.map, function(err) {
						check(done, function() {
							should.not.exist(err);
						});
					});
				});
			});

			describe('after setting views#get()', function(){
				it('should succeed', function(done){
					couch.views.get(testData.database, testData.designDocName, testData.viewName, function(err, view) {
						testData.retrievedView = view;
						check(done, function() {
							should.not.exist(err);
						});
					});
				});
			});

			describe('…the retrieved view', function(){
				it('should exist', function(){
					should.exist(testData.retrievedView);
				});
			});

			describe('views#getByView()', function(){
				it('should succeed', function(done){
					testData.documentEnumerations = 0;
					var enumeration = function(document) {
						testData.documentEnumerations += 1;
					};
					couch.views.getByView(testData.database, testData.designDocName, testData.viewName, {key: testData.setDocument._id}, enumeration, function(err, documents) {
						testData.retrievedDocuments = documents;
						check(done, function() {
							should.not.exist(err);
						});
					});
				});
			});

			describe('…the retrieved documents', function(){
				it('should exist', function(){
					should.exist(testData.retrievedDocuments);
				});
				it('should be a single document in an array', function(){
					testData.retrievedDocuments.should.be.an.Array.and.have.length(1);
					testData.documentEnumerations.should.eql(1);
				});
			});

			describe('views#remove()', function(){
				it('should succeed', function(done){
					couch.views.remove(testData.database, testData.designDocName, testData.viewName, function(err) {
						check(done, function() {
							should.not.exist(err);
						});
					});
				});
			});

			describe('after removing views#get()', function(){
				it('should succeed', function(done){
					couch.views.get(testData.database, testData.designDocName, testData.viewName, function(err, view) {
						testData.retrievedView = view;
						check(done, function() {
							should.not.exist(err);
						});
					});
				});
			});

			describe('…the retrieved view', function(){
				it('should not exist', function(){
					should.not.exist(testData.retrievedView);
				});
			});
		});

		describe('documents', function(){
			describe('#remove()', function(){
				it('should succeed', function(done){
					couch.remove(testData.setDocument._id, testData.database, function(err) {
						check(done, function() {
							should.not.exist(err);
						});
					});
				});
			});

			describe('after removing #get()', function(){
				it('should succeed', function(done){
					couch.get(testData.setDocument._id, testData.database, function(err, document) {
						testData.retrievedDocument = document;
						check(done, function() {
							should.not.exist(err);
						});
					});
				});
			});

			describe('…the retrieved document', function(){
				it('should exist', function(){
					should.not.exist(testData.retrievedDocument);
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
			host: 'http://test:password@localhost:5984'
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

