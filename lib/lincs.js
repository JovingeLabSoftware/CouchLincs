var Q = require('q');
var config = require('../config');

/**
 * @class LINCS
 * @classdesc
 * Convenience class to facilitate data I/O to LINCS data on our couchbase server
 * @config {string} [ip] IP address of the couchdb server, from config.js
 * @config {string} [bucket] Name of the LINCS data bucket from config.js
 * @config {string} [password] Password, if any, for specified bucket
 * @example
 * lincs = require('lincs');
 */
var LINCS_MAXGET = 20;
var LINCS_THROTTLE = 100;

var LINCS = function () {
	var _couchbase = require('couchbase');
	var _cluster = new _couchbase.Cluster('couchbase://' + config.couchdb.ip);
	var _bucket = _cluster.openBucket(config.couchdb.bucket, config.couchdb.password, function(err, res) {
    if(err) {
      throw new Error("Failed to connect to bucket.  Please confirm connection details in config.json");
    }
    _bucket.connectionTimeout = 5000;
    _bucket.operationTimeout = 5000;
  });
  var n1ql_url = "http://" + config.couchdb.ip + ":8093";
  var _n1ql = _couchbase.N1qlQuery;
  _bucket.enableN1ql([n1ql_url]);


  
  var _view = _couchbase.ViewQuery;

   // no reason to do this now, but preparing for more logic 
   // and enforecement on accessors in the future.
   this.cluster = _cluster;
   this.bucket = _bucket;
   this.view = _view;
   this.n1ql = _n1ql;

   // query queue
   this._qq = [];
   this._throttle = 0;
};


/**
 * Get data for a set of keys (either primary or view).
 * @param {string[]} keys List of keys. 
 * @param {string} field Field to return, defaults to '*'
 * @param {function} cb Call back to be called upon completion of async call.  Optional--can use promises instead.
 * @example lincs.get("1", "metadata.pert_desc").then(function(x) { console.log(x) });
 */
LINCS.prototype.get = function(keys, fields, cb) {
  fields = fields || "*";
  fields = [].concat(fields);
  var deferred = Q.defer();
  keys = [].concat(keys);
  fields = fields.map(function(f) {
      return("LINCS." + f);
  });

	var q = this.n1ql.fromString(`SELECT META().id, ${fields.join(", ")} ` + 
	  `FROM LINCS USE KEYS ` + JSON.stringify(this._arrayStringify(keys)));
  this.bucket.query(
    q,
    function(err, data) {
      if(err) {
        deferred.reject(err);
      } else {
        deferred.resolve(data);
      }
  })
  deferred.promise.nodeify(cb);
  return deferred.promise;
};



/**
 * Get data for an entire plate or portion thereof.
 * @param {string} plate plate_id 
 * @param {string} field What to return (e.g. metadata, data, metadata.pert_id).
 *                       Defaults to '*'
 * @param {string} filter JSON formatted filter(s) to apply, in the form of 
 *                 {'field': 'value'}.  Note that field will be prepended with
 *                 'metadata.', so no need to add that (and, obviously, 
 *                 filter is restricted to the metadata.)
 * @example lincs.getPlate("1").then(function(x) { console.log(x) });
 */
LINCS.prototype.getByPlate = function(plate, fields, filter, cb) {
	var deferred = Q.defer();

  var q = this._prepare(fields, {det_plate: plate}, filter);

  this.bucket.query(
    q,
    function(err, data) {
      if(err) {
        deferred.reject(err);
      } else {
        deferred.resolve(data);
      }
  });
  deferred.promise.nodeify(cb);
  return deferred.promise;
};

/**
 * Get data for all instances of cell_id with specified perturbation.
 * @param {string} cell cell_id.  Required 
 * @param {string} field What to return (e.g. metadata, data, metadata.pert_id).
 *                       Defaults to '*'
 * @param {string} filter JSON formatted filter(s) to apply, in the form of 
 *                 {'field': 'value'}.  Note that field will be prepended with
 *                 'metadata.', so no need to add that (and, obviously, 
 *                 filter is restricted to the metadata.)
 * @param {number} skip how many records to skip (for paging, default = 0) 
 * @param {number} limit how many docs to return (for paging, default = 10000) 
 * @example lincs.getPert("1").then(function(x) { console.log(x) });
 */
LINCS.prototype.getByCell = function(cell, field, filter, skip, limit, cb) {
	var deferred = Q.defer();

	var q = this._prepare(field, {cell_id: cell}, filter, skip, limit)
  this.bucket.query(
    q,
    function(err, data) {
      if(err) {
        deferred.reject(err);
      } else {
        deferred.resolve(data);
      }
  });
  deferred.promise.nodeify(cb);
  return deferred.promise;
};

/**
 * Get data for all instances of pert_desc with specified perturbation details.
 * @param {string} pert pert_desc.  Required 
 * @param {string} field What to return (e.g. metadata, data, metadata.pert_id).
 *                       Defaults to '*'
 * @param {string} filter JSON formatted filter(s) to apply, in the form of 
 *                 {'field': 'value'}.  Note that field will be prepended with
 *                 'metadata.', so no need to add that (and, obviously, 
 *                 filter is restricted to the metadata.)
 * @param {number} skip how many records to skip (for paging, default = 0) 
 * @param {number} limit how many docs to return (for paging, default = 10000) 
 * @example lincs.getPert("1").then(function(x) { console.log(x) });
 */
LINCS.prototype.getByPert = function(pert, field, filter, skip, limit, cb) {
	var deferred = Q.defer();

	var q = this._prepare(field, {pert_desc: pert}, filter, skip, limit)
	console.log(q)
  this.bucket.query(
    q,
    function(err, data) {
      if(err) {
        deferred.reject(err);
      } else {
        deferred.resolve(data);
      }
  });
  deferred.promise.nodeify(cb);
  return deferred.promise;
};

/**
 * Get data for all instances of pert_desc with specified perturbation details.
 * @param {string} pert pert_id.  Required 
 * @param {string} field What to return (e.g. metadata, data, metadata.pert_id).
 *                       Defaults to '*'
 * @param {string} filter JSON formatted filter(s) to apply, in the form of 
 *                 {'field': 'value'}.  Note that field will be prepended with
 *                 'metadata.', so no need to add that (and, obviously, 
 *                 filter is restricted to the metadata.)
 * @param {number} skip how many records to skip (for paging, default = 0) 
 * @param {number} limit how many docs to return (for paging, default = 10000) 
 * @example lincs.getPert("1").then(function(x) { console.log(x) });
 */
LINCS.prototype.getByPertId = function(pert, field, filter, skip, limit, cb) {
	var deferred = Q.defer();

	var q = this._prepare(field, {pert_id: pert}, filter, skip, limit)
	console.log(q)
  this.bucket.query(
    q,
    function(err, data) {
      if(err) {
        deferred.reject(err);
      } else {
        deferred.resolve(data);
      }
  });
  deferred.promise.nodeify(cb);
  return deferred.promise;
};

/**
 * Get zscores (vs. vehicle control) matching specified cell line and perturbagen.
 * Note that the view this function queries has a compound key.  Therefore,
 * if perturbagen (pert) is specified, cell_line must also be specified.
 * Similarly, if dose and time are specified, perturbagen and cell line 
 * must also be specified.
 * @param {string} cell_line ID of cell lines. Can be null.
 * @param {string} pert Name of perturbagen. Can be null.
 * @param {numeric} dose Dose (numeric, unitless).  Can be null.
 * @param {numeric} time Duration of exposure (numeric, unitless). Can be null.
 * @param {boolean} gold Should query be restricted to gold instances. If null,
 *                       defaults to true.
 * @param {function} cb Call back to be called upon completion of async call.  
 *                       Optional--can use promises instead.
 */

LINCS.prototype.getZSVC = function(cell_line, 
                                     pert, 
                                     dose, 
                                     time, 
                                     skip, 
                                     limit, 
                                     gold,
                                     cb) {

  var query;
  var deferred = Q.defer();
  var startkey = [cell_line || "", pert || "", dose || -999, time || -999];
  var endkey = [cell_line || "\uefff", pert || "\uefff", dose || 9999, time || 9999];
  gold = gold || true;
  
  if(gold) {
    query = this.view.from('lincs_zscore', 'ZSVC_L1000_gold')
      .reduce(false).skip(skip || 0).limit(limit || 1000);
  } else {
    query = this.view.from('lincs_zscore', 'ZSVC_L1000')
      .reduce(false).skip(skip || 0).limit(limit || 1000);
  }
  
  query.options.startkey = JSON.stringify(startkey);  // query.range does not work 
                                                      // because it adds quotes
  query.options.endkey = JSON.stringify(endkey);  
  
  if(!cell_line && pert) {
    deferred.reject(new Error("Must specify cell line if perturbagen is specified"));
  } else if((dose || time) && (!pert || !cell_line)) {
    deferred.reject(new Error("Must specify cell line and perturbagen " +
                                "if dose and duration are specified"));
  }
  this.bucket.query(query, function(err, results) { 
      if(err) {
        deferred.reject(err);
      } else {
        deferred.resolve(results);
      }
    });

    deferred.promise.nodeify(cb);
    return deferred.promise;
};

/**
 * Insert a perturbation score (e.g. zscore) data document into the store.  
 * @param {string} doc Document in JSON including type, metadata, gene_ids, 
 *                 data, cell, dose, duration, type, gold, method.
 * @param {function} cb Call back to be called upon completion of async call.  
 *                       Optional--can use promises instead.
 */
LINCS.prototype.savePert = function(doc, cb){
  var deferred = Q.defer();
  if(!this._checkParams(doc, ['method', 'dose', 'perturbagen', 'duration', 'gene_ids', 'data'])){
    return(deferred.reject(new Error("document did not contain required parameters (saveInstance")));
  } else {
    if(doc.gene_ids.length != doc.data.length) {
      deferred.reject(new Error("Gene IDs length must match zscores length"));
    }
    doc.type = "pert";
    var id = doc.method + "_" + doc.cell + "_" +  doc.perturbagen +  
             "_" + doc.dose +  "_" + doc.duration;
    this.bucket.upsert(id, JSON.stringify(doc), function(err, res) 
    {
      if(err) {
        deferred.reject(err);
      } else {
        deferred.resolve(id);
      }
   });
  }
  deferred.promise.nodeify(cb);
  return deferred.promise;
};

/**
 * Insert an instance doc (e.g. level 2 data from LINCS) into the store.  
 * @param {string} id Desired document id (aka 'key')
 * @param {string} doc Document in JSON including type, metadata, gene_ids, 
 *                 expression.  Type should indicate what type of data 
 *                 this is, e.g. "q2norm"
 * @param {function} cb Call back to be called upon completion of async call.  
 *                       Optional--can use promises instead.
 */
LINCS.prototype.saveInstance = function(id, doc, cb){
  var deferred = Q.defer();
  

   if(!this._checkParams(doc, ['metadata', 'gene_ids', 'data', 'type'])){

    deferred.reject(new Error("document did not contain required parameters (saveInstance"));

  } else {
    
    if(doc.gene_ids.length != doc.data.length) {
          deferred.reject(new Error("Gene IDs length must match zscores length"));
    }
    this.bucket.upsert(String(id), doc, function(err, res) 
    {
      if(err) {
        deferred.reject(err);
      } else {
        deferred.resolve(id);
      }
   });
  }

  deferred.promise.nodeify(cb);
  return deferred.promise;
};


/* private function to verify parameters in object
*/
LINCS.prototype._checkParams = function(obj, vars) {
    var ok = true;
    vars.forEach(function(v) {
        if(typeof(obj[[v]]) == "undefined") {
            ok = false;
        } 
    });
    return(ok);
};


/* convert all members of an array to a string
*/
LINCS.prototype._arrayStringify = function(a) {
    var as = [];
    a.forEach(function(x) { as.push(String(x)) });
    return(as);
};


/* construct a well formed N1QL statement
*  Note, order of where statements is important for compound indices
*  Primary and secondary are objects.  Primary should have just one 
*  key (the name of the field to query) and value (the value to query), 
*  whereas secondary may have many keys to filter on.
*/
LINCS.prototype._prepare = function(select, primary, secondary, skip, limit) {

  var primary_field = Object.keys(primary)[0];
  var primary_value = primary[[primary_field]];
  
  select = select || "*";
  select = [].concat(select);
  select = select.map(function(f) {
      return("LINCS." + f);
  });

  var q = `SELECT META().id, ${select.join(", ")} FROM LINCS `+
          `WHERE metadata.${primary_field} = "${primary_value}" `;
  if(secondary) {
    Object.keys(secondary).forEach(function(k) {
      q += `AND metadata.${k} = "${secondary[[k]]}" `;
    });
  }
  q += `${skip ? 'OFFSET ' + skip : ''} ${limit ? ' LIMIT ' + limit : ''}`;

  // unquote numerics and booleans
  q = q.replace(/['"]true['"]/g, "true").replace(/['"]false['"]/g, "false")
       .replace(/['"]([\.\d]+)['"]/g, "$1");  

  return(this.n1ql.fromString(q));
};


 
module.exports = exports = new LINCS();
