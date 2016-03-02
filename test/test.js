var lincs = require("../lib/lincs.js");
var assert = require('assert');
var couchbase = require('couchbase');

describe('LINCS funtion tests', function () {
    describe('Connectivity test', function () {
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
        // fulfilled (rather it.skip will be rejected)
    	var k = []; var c = 1;
    	while(c <= 50) {
    		k.push(c);
    		c++;
    	}

        it('retrieves all data for a set of documents by view query', function (done) {
            this.timeout(5000);
		    lincs.zsvc(["CPC005_A375_6H_X1_B3_DUO52HI53LO:K06","CPC005_A375_6H_X2_B3_DUO52HI53LO:K06","CPC005_A375_6H_X3_B3_DUO52HI53LO:K06","CPC005_A375_6H_X1_B3_DUO52HI53LO:C19","CPC005_A375_6H_X2_B3_DUO52HI53LO:C19","CPC005_A375_6H_X3_B3_DUO52HI53LO:C19","CPC004_A375_6H_X1_B3_DUO52HI53LO:K13","CPC004_A375_6H_X2_B3_DUO52HI53LO:K13","CPC004_A375_6H_X3_B3_DUO52HI53LO:K13","CPC005_A375_6H_X1_B3_DUO52HI53LO:K20","CPC005_A375_6H_X2_B3_DUO52HI53LO:K20","CPC005_A375_6H_X3_B3_DUO52HI53LO:K20","CPC005_A375_6H_X1_B3_DUO52HI53LO:F19","CPC005_A375_6H_X2_B3_DUO52HI53LO:F19"])
		    .then(function(data) {
				assert.equal(data.length, 14);
				done();
		    })
			.catch(function(err) {
				done(err);
			});	
        });

        it('retrieves all data for a set of documents', function (done) {
            this.timeout(5000);
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
            this.timeout(5000);
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

        it('retrieves data for set of distil_ids', function (done) {
        	var ids = ['CYT001_HA1E_2H_X1_B12:C18', 
        	'CYT001_HA1E_2H_X2_B7_DUO52HI53LO:C18',
        	'CYT001_HT29_2H_X1_B7_DUO52HI53LO:C18', 
        	'CYT001_MCF7_2H_X1_B12:C18']
        	lincs.instanceQuery({distil_id: ids}, ["metadata.pert_desc", "metadata.pert_dose"])
		    .then(function(data) {
				assert.equal(data.length, 4);	
				done();
		    })
			.catch(function(err) {
				done(err);
			});	
        });

        it('retrieves all data for all vector controls on a plate', function (done) {
		    lincs.instanceQuery({det_plate: "KDD010_HCC515_96H_X3_B5_DUO52HI53LO",
		    					 pert_type: "ctl_vector",
		    					 pert_desc: "lacZ"}, 
		    					 ["metadata.pert_desc", "metadata.pert_dose"])
		    .then(function(data) {
				assert.equal(data[0].pert_desc, "lacz");	
				assert.equal(data.length, 2);	
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
		    lincs.instanceQuery({cell_id: "A549",
		    					 is_gold: true},
		    					 ["metadata.pert_desc", "metadata.pert_dose"],
		    					 100, 100)
		    .then(function(data) {
				assert.equal(data.length, 100);	
				done();
		    })
			.catch(function(err) {
				done(err);
			});	
        });

        it('retrieves a count of instances matching query (same cell line, perturbation, dose, duration)', function (done) {
		    this.timeout(5000);
		    lincs.instanceCount({cell_id: "A549",
		    					 pert_desc: "BMP7",
		    					 pert_dose: 100,
		    					 pert_time: 2 })
		    .then(function(count) {
		    	assert.equal(count, 3);
				done();
		    })
		    .catch(function(e) {
		    	done(e);
		    });
        });

        it('retrieves all related instances (same cell line, perturbation, dose, duration)', function (done) {
		    this.timeout(5000);
		    lincs.instanceQuery({cell_id: "A549",
		    					 pert_desc: "BMP7",
		    					 pert_dose: 100,
		    					 pert_time: 2 },
		    					 ["metadata.pert_desc", "metadata.pert_dose"])
		    .then(function(data) {
		    	assert.equal(data.length , 3);
				done();
		    })
		    .catch(function(e) {
		    	done(e);
		    });
        });

	  it('retrieves all vehicle instances from the same plate for a given instance', function(done) {
		    this.timeout(5000);
	    	lincs.get("5", "metadata.det_plate")
	    	.then(function(res) {
	    		var p =  lincs.instanceQuery({det_plate: res[0].det_plate, 
	    									  pert_type: "ctl_vehicle"},
			  	    					     ["metadata.pert_desc", 
			  	    					      "metadata.pert_dose"]);
  			    return(p);
	    	})
			.then(function(res) {
		    	assert.equal(res.length , 12);
				done();
		    })
		    .catch(function(e) {
		    	done(e);
		    });
        });

        it('retrieves gene expression data', function (done) {
	    this.timeout(5000);
	    	lincs.get("2", "data")
		    .then(function(res) {
		    	assert.equal(res[0].data.length, 978);
				done();
		    })
		    .catch(function(e) {
		    	done(e);
		    });
        });
    });
});
