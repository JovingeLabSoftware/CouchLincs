var Q = require('q');
var config = require('../config');
var restify = require('restify');
var lincs = require('lincs');

var server = restify.createServer({
  name: 'LINCS',
  version: '0.0.1'
});

server.use(restify.bodyParser());
server.use(restify.fullResponse());
server.use(restify.CORS());
server.use(restify.queryParser());
restify.defaultResponseHeaders = function(data) {
    this.header("Access-Control-Allow-Origin", "*");
    this.header("Access-Control-Allow-Headers", "X-Requested-With");
    this.contentType = 'json';
};


/**
 * @api {get} / Root.  Verify server up and receive useful advice.
 * @apiName Root
 * @apiGroup Root
 *
 * @apiExample {curl} Example usage:
 *           curl http://localhost:8080/
 * @apiSuccess {string} response
 * @apiSuccessExample Success-Response: 
 * HTTP/1.1 200 OK
 * OK.  Send GET to /LINCS for further documentation.
 * 
 */
server.get('/', function(req, res){
    res.send(200, "OK!  Send GET to /LINCS for further documentation.");
});

/**
 * @api {get} /LINCS  Get some documentation on endpoints.
 * @apiName Root
 * @apiGroup Root
 *
 * @apiSuccess {string} response  Some useful information.
 */
server.get('/LINCS', function(req, res){
    res.send(200, "Current endpoints: /LINCS/nidrange, /LINCS/summaries, /LINCS/summaries/nid.  See http://jovingelabsoftware.github.io/CouchLincs/api for details");
});


/**
 * @api {GET} /LINCS/instances/:distil_id/metadata Request metadata 
 * @apiName instanceMetadata
 * @apiGroup LINCS
 * @apiDescription Fetch metadata for a given instance by distil_id
 * @apiParam {string} distil_id Id of instance
 * @apiExample {curl} Example usage:
 *           curl localhost:8080/LINCS/instances/\
 *              CPC014_VCAP_6H_X2_F1B3_DUO52HI53LO:P05/metadata
 *
 * @apiSuccess {Object} metadata Metadats docs in JSON format
 * @apiSuccessExample Success-Response: 
 * HTTP/1.1 200 OK
 * Content-Type: application/json
 * {
 *   "metadata": {
 *     "det_plate": "AML001_CD34_24H_X1_F1B10",
 *     "is_gold": true,
 *     "pert_vehicle": "DMSO",
 *     "pert_type": "trt_cp",
 *     "distil_id": "AML001_CD34_24H_X1_F1B10:A03",
 *     "pert_id": "BRD-K49071277",
 *     "pert_desc": "SECURININE",
 *     "cell_id": "CD34",
 *     "pert_time": 24,
 *     "pert_time_unit": "h",
 *     "pert_dose": 10,
 *     "pert_dose_unit": "um"
 *   },
 *   "id": "1230884"
 * }   
 */

server.get('/LINCS/instances/:distil_id/metadata', function(req, res){
    var key = req.params.distill_id;
    lincs.getMetadata(key, 0, 1, function(err, data) {
        if(err) {
            res.send(400, err);
        } else {
            res.send(200, {id: data[0].id, metadata: data[0].value});
        }
    });
});

/**
 * @api {POST} /LINCS/instances/query/metadata Request metadata (multi) 
 * @apiName instancesMetadata
 * @apiGroup LINCS
 * @apiDescription Fetch metadata for given instance by distil_id.  We use POST
 * here because keys are long and could quickly exceed GET query string 
 * limit imposed by many clients.
 * @apiParam {string[]} keys distilIds of instances
 * @apiExample {curl} Example usage:
 *  curl -d '{"keys": ["CPC014_VCAP_6H_X2_F1B3_DUO52HI53LO:P05", \
 *        "CPC014_VCAP_6H_X2_F1B3_DUO52HI53LO:P05"]}' \
 *        localhost:8080/LINCS/instances/metadata \
 *        -H "Content-Type: application/json"  
 *
 * @apiSuccess {Object} metadata Metadats docs in JSON format
 * @apiSuccessExample Success-Response: 
 * HTTP/1.1 200 OK
 * Content-Type: application/json
 * [  
 * {
 *    "key": "CPC014_VCAP_6H_X2_F1B3_DUO52HI53LO:P05",
 *    "metadata": {
 *      "det_plate": "CPC014_VCAP_6H_X2_F1B3_DUO52HI53LO",
 *      "is_gold": true,
 *      "pert_vehicle": "DMSO",
 *      "pert_type": "trt_cp",
 *      "distil_id": "CPC014_VCAP_6H_X2_F1B3_DUO52HI53LO:P05",
 *      "pert_id": "BRD-A64228451",
 *      "pert_desc": "EI-328",
 *      "cell_id": "VCAP",
 *      "pert_time": 6,
 *      "pert_time_unit": "h",
 *      "pert_dose": 10,
 *      "pert_dose_unit": "um"
 *    },
 *    "id": "312240"
 *  } 
 * ... // truncated
 * ]
 */
server.post('/LINCS/instances/metadata', function(req, res){
    var keys = req.params.keys;
    lincs.get(keys, "metadata/metadata", function(err, data) {
        if(err) {
            res.send(400, err);
        } else {
            res.send(200, data);
        }
    });
});


/**
 * @api {GET} /LINCS/instances/:id retrieve instance by id
 * @apiName getInstance
 * @apiGroup LINCS
 * @apiDescription Ket instance by couchbase id (not distil_id)
 * @apiParam {string} id The id of the instance document to retrieve
 * @apiSuccess {string} instances Instance in JSON format
 * @apiSuccessExample Success-Response: 
 * HTTP/1.1 200 OK
 * {
 * }
 */
server.get('/LINCS/instances/:id', function(req, res) {
  lincs.get(req.params.id, null, function(err, data) {
      if(err) {
          res.send(400, err);
      } else {
          res.send(200, data[0]);
      }
  });
});

/**
 * @api {GET} /LINCS/instances retrieve instances by ids or query
 * @apiName queryInstances
 * @apiGroup LINCS
 * @apiDescription submit query as query string (?q={}) in JSON format.
 * Supported queries To Be Determined.  Note that at the moment, this
 * endpoint makes use of a multi-key view.  Keys for such views are
 * hierarchical.  Thus, if you specify pert, you must specify cell_line,
 * and so on.  Hierarchy is cell_line > pert > dose > duration
 * @apiParam {string} q The query parameters in JSON format
 * @apiParam {string} q.ids Array of explicit ids to fetch. If this parameter
 *                    is provided, the parameters below are ignored.
 * @apiParam {string} q.limit Number of docs to return, default 1000
 * @apiParam {string} q.skip Number of docs to skip (for paging), default 0
 * @apiParam {string} q.cell_line Cell line of interest (optional)
 * @apiParam {string} q.pert perturbagen (pert_desc) of interest (optional)
 * @apiParam {string} q.dose dose of interest (unitless, optional)
 * @apiParam {string} q.duration duration of interest (unitless, optional)
 * @apiParam {logical} q.gold Whether to limit to is_gold instances, default false
 * @apiSuccess {string} instances Instance ids  in JSON format
 * @apiSuccessExample Success-Response: 
 * HTTP/1.1 200 OK
 * {
 * }
 */
server.get('/LINCS/instances', function(req, res) {
    var query = JSON.parse(req.params.q)
    if(query.ids) {
      lincs.get(query.ids, null, function(err, data) {
          if(err) {
              res.send(400, err);
          } else {
              res.send(200, data);
          }
      });
    } else {
      var limit = query.limit || 1000;
      var skip = query.skip || 0;
      var cell_line = query.cell || null;
      var pert = query.pert || null;
      var dose = query.dose || null;
      var duration = query.duration || null;
      var gold;
      if (query.gold) {
          gold = JSON.parse(String(query.gold).toLowerCase());
      } else {
          gold = null;
      }
      lincs.getByPert(cell_line, pert, Number(dose), Number(duration), Number(skip), Number(limit), gold, 
                      function(err, data) {
          if(err) {
              res.send(400, err);
          } else {
              res.send(200, data);
          }
      });
    }
});


/**
 * @api {POST} /LINCS/data Post ids (either primary or view) and retrieve 
 *                          documents
 * @apiName retrieveData
 * @apiGroup LINCS
 * @apiDescription Retrieves multiple docs by primary or view id.  We use POST
 * here becuase keys are large and quickly exceed GET query string length
 * limit imposed by many clients
 * 
 * @apiParam {[String]} keys List of primary or view keys
 * @apiParam {String} view Name of view.  If missing will use primary index
 *
 * @apiSuccess {string} Id of created object
 * @apiSuccessExample Success-Response: 
 * HTTP/1.1 200 OK
 * {
 *  ...
 * }
 */
server.post('/LINCS/data', function(req, res) {
    var view = req.params.view || null;
    lincs.get(req.params.keys, view, function(err, data) {
        if(err) {
            res.send(400, err);
        } else {
            res.send(200, data);
        }
    });
});

/**
 * @api {POST} /LINCS/pert Create a perturbation data document
 * @apiName createPerturbation
 * @apiGroup LINCS
 * @apiDescription Creates a data document for a particular perturbation &
 *                 calculation method
 * 
 * @apiParam {String} perturbagen name of perturbagen
 * @apiParam {numeric} dose dose (unitless)
 * @apiParam {numeric} duration duration (unitless)
 * @apiParam {String} cell cell line used
 * @apiParam {String} method calculation method used, e.g. "zsvc_plate"
 * @apiParam {boolean} gold is this a gold signature score?
 * @apiParam {[String]} gene_ids from lincs
 * @apiParam {[Numeric]} data the scores (one per gene)
 *
 * @apiSuccess {string} Id of created object
 * @apiSuccessExample Success-Response: 
 * HTTP/1.1 201 Created
 * {
 *  id: 1
 * }
 */
server.post('/LINCS/pert', function(req, res) {
    if(!checkParams(req.params, ['perturbagen', 'dose', 'duration', 'cell',
                          'method', 'gold', 'gene_ids', 'data'])) {
        res.send(400, "Creating pert document requires specification of " +
                      "perturbagen, dose, cell, method, gold, gene_ids, data")                      
    } else {
        lincs.savePert(req.params, function(err, data) {
            if(err) {
                res.send(400, err);
            } else {
                res.send(200, data);
            }
        });
    }
});

                                           
/**
 * @api {POST} /LINCS/instances Save instance data to server 
 * @apiName postInstance
 * @apiGroup LINCS
 * @apiDescription Stores instance (e.g. level 2 data) on the server. Do NOT use this
 *                    endpoint to save calculated scores (use /LINCS/pert for that).
 *
 * @apiParam {String} id desired id (key) of document
 * @apiParam {String} metadata JSON from lincs
 * @apiParam {String} type  Type of data, e.g. "q2norm"
 * @apiParam {[String]} gene_ids from lincs
 * @apiParam {[Numeric]} data the expression data
 *
 * @apiSuccess {string} response response
 * @apiSuccessExample Success-Response: 
 * HTTP/1.1 200 OK
 * {
 * }
 */
server.post('/LINCS/instances', function(req, res){
    if(!checkParams(req.params, ['id', 'gene_ids', 'metadata', 'data', 'type'])) {
        res.send(400, "Creating instance document requires POSTing the following " +
                      "parameters: id, gene_ids, data, metadata, type"); 
    } else {
        var doc = {gene_ids: req.params.gene_ids, 
                   metadata: req.params.metadata,
                   data: req.params.data,
                   type: req.params.type,
                   gold: req.params.gold};

        lincs.saveInstance(req.params.id, doc, function(err, data) {
            if(err) {
                res.send(400, err);
            } else {
                res.send(200, data);
            }
        });
    }
});

/**
 * @api {GET} /LINCS/instances/:id/controls Retrieve control data 
 * @apiName getControls
 * @apiGroup LINCS
 * @apiDescription Retrieves the full data document for each instance on the 
 * same plate as :id whose perturbagen is the appropriate control for that 
 * instance (including time of exposure and cell line). 
 *
 * @apiParam {String} id of instance for which controls are desired.
 *
 * @apiSuccess {string} data Document in JSON format
 * @apiSuccessExample Success-Response (note array): 
 * HTTP/1.1 200 OK
 * {
 * ["gene_ids": [
 *     "200814_at",
 *     "222103_at",
 *     "...truncated..."
 *     ],
 * "metadata": {
 *     "bead_batch": "b3",
 *     "bead_revision": "r2",
 *     "bead_set": "dp52,dp53",
 *     "cell_id": "HCC515",
 *     "...truncated..."
 * },
 * "data": [
 *     9.15469932556152,
 *     9.05399990081787,
 *     "...truncated..."
 *     ]
 *  },
 *  "...truncated..."
 * ]
 * }
 */
server.get('/LINCS/instances/:id/controls', function(req, res){
    res.header("Access-Control-Allow-Origin", "*");
    res.header("Access-Control-Allow-Headers", "X-Requested-With");
    lincs.instSamePlateVehicles(req.params.id, function(err, data) {
        if(err) {
            res.send(400, err);
        } else {
            res.send(200, data);
        }
    });
});


/**
 * @api {GET} /LINCS/sh_controls Retrieve control data for shRNA
 * @apiName getShControls
 * @apiGroup LINCS
 * @apiDescription Retrieves the appropriate shRNA controls 
 * 
 * 
*/

server.get('/LINCS/instances/:id/sh_controls', function(req, res) {
  res.header("Access-Control-Allow-Origin", "*");
  res.header("Access-Control-Allow-Headers", "X-Requested-With");
  lincs.instShSamePlateVehicles(req.params.id, function(err, data) {
      if(err) {
          res.send(400, err);
      } else {
          res.send(200, data);
      }
  });    
});

var port = config.port; 
if(process.env.COUCHLINCS_PORT)
   port = process.env.COUCHLINCS_PORT;


server.listen(port, function() {
    console.log('\t%s listening at %s', server.name, server.url);
});

var dequote = function(x) {
  for (var key in x) {
    if (x.hasOwnProperty(key)) {
      if(typeof(x[key]) === 'string') {
        x[key] = x[key].replace(/\"/g, '');
      }
    }
  }
  return(x);
};

var checkParams = function(obj, vars) {
    var ok = true;
    vars.forEach(function(v) {
        if(typeof(obj[[v]]) == "undefined") {
            ok = false;
        } 
    });
    return(ok);
};
