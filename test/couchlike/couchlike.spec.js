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

function getWinningRev(revisions) {
	var winner;
	var max = 0;
	revisions.forEach(function(revision) {
		if (!revision._deleted) {
			// rev containing text "conflict"
			if (revision._rev.indexOf('conflict') > 0) {
				winner = revision._rev;
			}
			// // highest rev
			// var test = parseInt(revision._rev.split('-')[0], 10);
			// if (test > max) {
			// 	winner = revision._rev;
			// 	max = test;
			// }
		}
	});
	return winner;
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
		var couch = new couchlike.Couchlike(configSpec.config);
		var conflictSeq = 0;
		var changeSeq = 0;
		var setChangeSeq = 0;
		var updateChangeSeq = 0;
		var conflictChangeSeq = 0;
		var changeFeed = null;

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

		before(function() {
			couch.on('change', function(change) {
				changeSeq = change.seq;
			});
			couch.on('conflict', function(conflict) {
				conflictSeq = conflict.seq;
				testData.conflictDocument = conflict;
			});
		});

		it('should be possible', function(){
			should.exist(couch);
		});

		it('should be the correct flavour of couchbasey', function(){
			if (configSpec.config) {
				should.exist(configSpec.config.testData);
				couch.isCouchbasey().should.equal(configSpec.config.testData.isCouchbasey);
			} else {
				couch.isCouchbasey().should.be.ok; // Null is couchbasey!
			}
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

			if (couch.capability.changes) {
				describe('changes#follow() excluding deletions and with conflicts', function(){
					it('should succeed', function(done){
						changeSeq = 0;
						couch.changes.follow(changeSeq, { excludeDeletionsAndEmitConflicts: true }, function(err, feed) {
							changeFeed = feed;
							should.exist(changeFeed);
							changeSeq.should.equal(0);
							done();
						});

					});
				});
			}

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

			if (couch.capability.changes) {
				describe('changes', function(){
					it('should have been called', function(done){
						setTimeout(function() {
							changeSeq.should.be.greaterThan(0);
							setChangeSeq = changeSeq;
							done();
						}, 3000);
					});
					it('#unfollow() should succeed', function(done){
						should.exist(changeFeed);
						couch.changes.unfollow(changeFeed, function() {
							changeFeed = null;
							done();
						});
					});
				});
			}

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

			if (couch.capability.changes) {
				describe('changes#follow() excluding deletions and with conflicts', function(){
					it('should succeed', function(done){
						setChangeSeq.should.be.greaterThan(0);
						couch.changes.follow(setChangeSeq, { excludeDeletionsAndEmitConflicts: true }, function(err, feed) {
							changeFeed = feed;
							should.exist(changeFeed);
							done();
						});

					});
				});
			}

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

			if (couch.capability.changes) {
				describe('changes', function(){
					it('should have been called', function(done){
						setTimeout(function() {
							changeSeq.should.be.greaterThan(setChangeSeq);
							updateChangeSeq = changeSeq;
							done();
						}, 3000);
					});
					it('#unfollow() should succeed', function(done){
						should.exist(changeFeed);
						couch.changes.unfollow(changeFeed, function() {
							changeFeed = null;
							done();
						});
					});
				});
			}

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

		if (couch.capability.views) {
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
						testData.retrievedDocuments.should.be.an.Array;
						testData.retrievedDocuments.should.have.length(1);
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
		}

		describe('conflicts', function(){
			if (couch.capability.changes) {
				describe('changes#follow() excluding deletions and with conflicts', function(){
					it('should succeed', function(done){
						conflictSeq = 0;
						couch.changes.follow(updateChangeSeq, { excludeDeletionsAndEmitConflicts: true }, function(err, feed) {
							changeFeed = feed;
							should.exist(changeFeed);
							conflictSeq.should.equal(0);
							done();
						});

					});
				});
			}

			describe('create a conflict with #force()', function(){
				it('should succeed', function(done){
					testData.anotherRetrievedDocument.foo = 'rabbar';
					testData.anotherRetrievedDocument._rev = '1-conflict'+Math.random();
					couch.force(testData.anotherRetrievedDocument, function(err, result) {
						check(done, function() {
							should.not.exist(err);
							should.exist(result);
							result._rev.should.be.ok;
						});
					});
				});
			});

			if (couch.capability.changes) {
				describe('conflicts', function(){
					it('should have been called', function(done){
						setTimeout(function() {
							conflictSeq.should.be.greaterThan(0);
							done();
						}, 3000);
					});
				});
			}

			describe('…the conflict', function(){
				it('should exist', function(){
					should.exist(testData.conflictDocument);
				});
			});

			describe('after introducing a conflict #get()', function(){
				it('should still succeed', function(done){
					couch.get(testData.setDocument._id, function(err, document) {
						check(done, function() {
							should.not.exist(err);
						});
					});
				});
			});

			describe('#getRevisions()', function(){
				it('should succeed', function(done){
					couch.getRevisions(testData.conflictDocument.doc._id, function(err, result) {
						check(done, function() {
							testData.conflictRevisions = result;
							should.not.exist(err);
							should.exist(result);
						});
					});
				});
			});

			describe('…the conflict revisions', function(){
				it('should exist', function(){
					should.exist(testData.conflictRevisions);
				});
			});

			describe('…resolving the conflict with #resolve()', function(){
				it('should work', function(done){
					var resolution = { losers: [] };
					var winningRev = getWinningRev(testData.conflictRevisions);
					testData.conflictRevisions.forEach(function(revision) {
						// if (revision._rev === winningRev) { return; } // leave winner untouched
						if (revision._rev === winningRev) { resolution.winner = revision; } // update winner
						else if (!revision._deleted) { resolution.losers.push(revision); }
					});
					couch.resolve(resolution, function(err, result) {
						check(done, function() {
							should.not.exist(err);
							should.exist(result);
						});
					});
				});
			});

			describe('after resolving the conflict #get()', function(){
				it('should still succeed', function(done){
					couch.get(testData.setDocument._id, function(err, document) {
						testData.retrievedDocument = document;
						check(done, function() {
							should.not.exist(err);
						});
					});
				});
			});

			// Use this test just to confirm that updating the resolved conflict winner works as expected. Removed, because it muddies whetehr the subsequent tested change event was really a result of the conflict resolution
			// describe('after resolving the conflict update document with #set()', function(){
			// 	it('should succeed', function(done){
			// 		testData.retrievedDocument.foo = 'barrab';
			// 		couch.set(testData.retrievedDocument, function(err, result) {
			// 			check(done, function() {
			// 				should.not.exist(err);
			// 				should.exist(result);
			// 				result._rev.should.be.ok;
			// 			});
			// 		});
			// 	});
			// });

			if (couch.capability.changes) {
				describe('changes', function(){
					it('should have been called', function(done){
						setTimeout(function() {
							changeSeq.should.be.greaterThan(updateChangeSeq);
							conflictChangeSeq = changeSeq;
							done();
						}, 3000);
					});
					it('#unfollow() should succeed', function(done){
						should.exist(changeFeed);
						couch.changes.unfollow(changeFeed, function() {
							changeFeed = null;
							done();
						});
					});
				});
			}

		});

		describe('remove documents', function(){
			if (couch.capability.changes) {
				describe('changes#follow() including deletions', function(){
					it('should succeed', function(done){
						conflictChangeSeq.should.be.greaterThan(0);
						couch.changes.follow(conflictChangeSeq, function(err, feed) {
							changeFeed = feed;
							should.exist(changeFeed);
							done();
						});

					});
				});
			}

			describe('#remove()', function(){
				it('should succeed', function(done){
					couch.remove(testData.setDocument._id, function(err) {
						check(done, function() {
							should.not.exist(err);
						});
					});
				});
			});

			if (couch.capability.changes) {
				describe('changes', function(){
					it('should have been called', function(done){
						setTimeout(function() {
							changeSeq.should.be.greaterThan(conflictChangeSeq);
							done();
						}, 3000);
					});
					it('#unfollow() should succeed', function(done){
						should.exist(changeFeed);
						couch.changes.unfollow(changeFeed, function() {
							changeFeed = null;
							done();
						});
					});
				});
			}

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

var configs = {};
if (process.env.TEST_COUCHDB) {
	configs.couchDB = {
		type: couchlike.engineType.couchDB,
		connection: {
			host: 'localhost',
			username: 'test',
			password: 'password',
			bucket: 'unit_tests'
		},
		testData: {
			isCouchbasey: false
		}
	};
}
if (process.env.TEST_POUCHDB) {
	configs.pouchDB = {
		type: couchlike.engineType.pouchDB,
		connection: {
			bucket: 'unit_tests'
		},
		testData: {
			isCouchbasey: false
		}
	};
}
if (process.env.TEST_COUCHBASESYNCGATEWAY) {
	configs.couchbaseSyncGateway = {
		type: couchlike.engineType.couchbaseSyncGateway,
		connection: {
			host: 'http://localhost',
			username: 'atlas',
			password: 'password',
			bucket: 'unit_tests',
			port: 4985,
			strictSSL: false,
			direct: {
				host: 'couchbase://127.0.0.1'
			}
		},
		testData: {
			isCouchbasey: true
		}
	};
}

for (var config in configs) {
	var test = {name: config, config: configs[config]};
	testWithConfig(test);
}

