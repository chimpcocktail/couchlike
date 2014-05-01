/* jshint expr:true */
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
			setDocument: {
				_id: 'test_data',
				foo: 'bar'
			},
			designDocName: 'testDoc',
			viewName: 'testView',
			setView: {
				couchbase: {
					map: function(doc, meta) {
						emit(meta.id, null);
					}
				},
				couch: {
					map: function(doc) {
						emit(doc._id, null);
					}
				}
			}
		};

		describe('documents', function(){
			describe('before setting #get()', function(){
				it('should succeed', function(done){
					couch.get(testData.setDocument._id, function(err, document) {
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
				it('should succeed and update the document revision', function(done){
					couch.set(testData.setDocument, function(err, result) {
						check(done, function() {
							should.not.exist(err);
							should.exist(result);
							result._rev.should.be.ok;
						});
					});
				});
			});

			describe('after setting #get()', function(){
				it('should succeed', function(done){
					couch.get(testData.setDocument._id, function(err, document) {
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
				it('should have a revision', function(){
					testData.retrievedDocument._rev.should.be.ok;
				});
				it('should match the set data', function(){
					testData.retrievedDocument.should.eql(testData.setDocument);
				});
			});

			describe('after setting #get() again', function(){
				it('should succeed', function(done){
					couch.get(testData.setDocument._id, function(err, document) {
						testData.anotherRetrievedDocument = document;
						check(done, function() {
							should.not.exist(err);
						});
					});
				});
			});

			describe('update document with #set()', function(){
				it('should succeed', function(done){
					testData.retrievedDocument.foo = 'rab';
					couch.set(testData.retrievedDocument, function(err, result) {
						check(done, function() {
							should.not.exist(err);
							should.exist(result);
							result._rev.should.be.ok;
						});
					});
				});
			});

			describe('after updating #get()', function(){
				it('should succeed', function(done){
					couch.get(testData.setDocument._id, function(err, document) {
						testData.retrievedUpdatedDocument = document;
						check(done, function() {
							should.not.exist(err);
						});
					});
				});
			});

			describe('…the retrieved document', function(){
				it('should exist', function(){
					should.exist(testData.retrievedUpdatedDocument);
				});
				it('should have a revision', function(){
					testData.retrievedUpdatedDocument._rev.should.be.ok;
				});
				it('should match the updated data', function(){
					testData.retrievedUpdatedDocument.should.eql(testData.retrievedDocument);
				});
			});

			describe('re-update original document with #set()', function(){
				it('should not succeed due to update conflicts', function(done){
					testData.anotherRetrievedDocument.foo = 'rab';
					couch.set(testData.anotherRetrievedDocument, function(err, result) {
						check(done, function() {
							should.exist(err);
						});
					});
				});
			});

		});

		describe('views', function(){
			describe('before setting views#get()', function(){
				it('should succeed', function(done){
					couch.views.get(testData.designDocName, testData.viewName, function(err, view) {
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
					var useViewMap = couch.isCouchbasey() ? testData.setView.couchbase.map : testData.setView.couch.map;
					couch.views.set(testData.designDocName, testData.viewName, useViewMap, function(err) {
						check(done, function() {
							should.not.exist(err);
						});
					});
				});
			});

			describe('after setting views#get()', function(){
				it('should succeed', function(done){
					couch.views.get(testData.designDocName, testData.viewName, function(err, view) {
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
					couch.views.getByView(testData.designDocName, testData.viewName, {key: testData.setDocument._id}, enumeration, function(err, documents) {
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
					couch.views.remove(testData.designDocName, testData.viewName, function(err) {
						check(done, function() {
							should.not.exist(err);
						});
					});
				});
			});

			describe('after removing views#get()', function(){
				it('should succeed', function(done){
					couch.views.get(testData.designDocName, testData.viewName, function(err, view) {
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
					couch.remove(testData.setDocument._id, function(err) {
						check(done, function() {
							should.not.exist(err);
						});
					});
				});
			});

			describe('after removing #get()', function(){
				it('should succeed', function(done){
					couch.get(testData.setDocument._id, function(err, document) {
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
			bucket: 'unit_tests'
		}
	},
	couchDB: {
		couchlike: {
			type: couchlike.engineType.couchDB
		},
		config: {
			host: 'http://test:password@localhost:5984',
			bucket: 'unit_tests'
		}
	},
*/
	pouchDB: {
		couchlike: {
			type: couchlike.engineType.pouchDB
		},
		config: {
			bucket: 'unit_tests'
		}
	}
};

for (var config in configs) {
	var test = {name: config, config: configs[config]};
	testWithConfig(test);
}

