var lincs = require("../lib/lincs.js");
var assert = require('assert');
var couchbase = require('couchbase');

describe('LINCS funtion tests', function () {
    describe('Connectivity', function () {
        it('connects to couchbase', function (done) {
		    lincs.bucket.get("5", function(err, data) {
		    	if(err) throw err;
				assert(data.value.metadata.pert_vehicle === "DMSO");	
				done();
		    });
        });
    });

    describe('LINCS CRUD Functions', function () {
        // note that if promises are used, we need to use .catch because a 
        // failed assert will throw an error and the promise will then never be
        // fulfilled (rather it will be rejected)
    	var k = []; var c = 1;
    	while(c <= 50) {
    		k.push(c);
    		c++;
    	}

        it('retrieves all data for a set of documents', function (done) {
		    lincs.get(k)
		    .then(function(data) {
				assert.equal(data.length, 50);
				assert.ok(data[0].metadata);
				done();
		    })
			.catch(function(err) {
				done(err);
			});	
        });

        it('retrieves two metadata fields from a set of documents', function (done) {
		    lincs.get(k, ["metadata.pert_desc", "metadata.pert_id"])
		    .then(function(data) {
				assert.equal(data.length, 50);
				assert.ok(data[0].pert_desc);
				assert.ok(data[0].pert_id);
				done();
		    })
			.catch(function(err) {
				done(err);
			});	
        });

        it('retrieves all data for all vector controls on a plate', function (done) {
		    lincs.getByPlate("KDD010_HCC515_96H_X3_B5_DUO52HI53LO", 
		    			   null, {"pert_type": "ctl_vector"})
		    .then(function(data) {
				assert.equal(data[0].metadata.pert_desc, "lacZ");	
				done();
		    })
			.catch(function(err) {
				done(err);
			});	
        });

        it('retrieves vehicle for an instance', function (done) {
		    lincs.get("10", 'metadata.pert_vehicle')
		    .then(function(veh) {
				assert.equal(veh[0].pert_vehicle, "DMSO");	
				done();
		    })
			.catch(function(err) {
				done(err);
			});	
        });

        it('retrieves the second 100 gold instance for cell line A549', function (done) {
		    lincs.getByCell("A549", 
		    			   "metadata", {"is_gold": true}, 100, 100)
		    .then(function(data) {
				assert.equal(data.length, 100);	
				done();
		    })
			.catch(function(err) {
				done(err);
			});	
        });

        it('retrieves all related instances (same cell line, perturbation, dose, duration)', function (done) {
	    this.timeout(5000);
		    lincs.getByPert("BMP7", 'metadata', {pert_dose: 100, pert_time:2}, 0, 5)
		    .then(function(res) {
		    	assert.equal(res.length , 5);
				done();
		    })
		    .catch(function(e) {
		    	done(e);
		    });
        });



	  it.skip('retrieves all vehicle instances from the same plate for a given instance', function(done) {
	    this.timeout(5000);
		    lincs.instSamePlateVehicles("5")
		    .then(function(res) {
		    	assert.equal(res.length , 13);
				done();
		    })
		    .catch(function(e) {
		    	done(e);
		    });
        });

        it.skip('retrieves gene expression data', function (done) {
	    this.timeout(5000);
		    lincs.getExpression("2")
		    .then(function(res) {
		    	assert(res.data.length  == 978);
				done();
		    })
		    .catch(function(e) {
		    	done(e);
		    });
        });
    });
});
