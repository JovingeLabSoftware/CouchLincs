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
  // this seems to now have no effect.
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
 * Get data for a set of primary ids.
 * @param {string[]} ids List of ids. 
 * @param {string} fields Fields to return, defaults to '*'
 * @param {function} cb Call back to be called upon completion of async call.  Optional--can use promises instead.
 * @example lincs.get("1", "metadata.pert_desc").then(function(x) { console.log(x) });
 */
LINCS.prototype.get = function(ids, fields, cb) {
  fields = fields || "*";
  fields = [].concat(fields);
  var deferred = Q.defer();
  ids = [].concat(ids);
  fields = fields.map(function(f) {
      return("LINCS." + f);
  });

	var q = this.n1ql.fromString(`SELECT META().id, ${fields.join(", ")} ` + 
	  `FROM LINCS USE KEYS ` + JSON.stringify(this._arrayStringify(ids)));
	this._execute(q, deferred);
  deferred.promise.nodeify(cb);
  return deferred.promise;
};



/**
 * Retrieve instance data by query parameters.
 * @param {object} query JSON formatted field/value pairs.  Value can be an
 *                 array of possible values. 
 * @param {string[]} fields Fields to return, defaults to '*'
 * @param {number} skip how many records to skip (for paging)
 * @param {number} limit how many records to return
 * @param {function} cb Call back to be called upon completion of async call.  Optional--can use promises instead.
 * @example lincs.instanceQuery({"metadata.pert_desc": "Clindamycin"}).then(function(x) { console.log(x) });
 */
LINCS.prototype.instanceQuery = function(query, fields, skip, limit, cb) {
	var deferred = Q.defer();
	var q = this._prepare(fields, query, skip, limit);
  this._execute(q, deferred);
  deferred.promise.nodeify(cb);
  return deferred.promise;
};

/**
 * Retrieve instance COUNT  by query parameters.
 * @param {object} query JSON formatted field/value pairs.  Value can be an
 *                 array of possible values. 
 * @param {function} cb Call back to be called upon completion of async call.  Optional--can use promises instead.
 * @example lincs.instanceQuery({"metadata.pert_desc": "Clindamycin"}).then(function(x) { console.log(x) });
 */
LINCS.prototype.instanceCount = function(query, cb) {
	var deferred = Q.defer();
	var q = this._prepare(null, query, null, null, true);
  this.bucket.query(
    q,
    function(err, data) {
      if(err) {
        deferred.reject(err);
      } else {
        deferred.resolve(data[0].COUNT);
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
  

   if(!this._checkParams(doc, ['metadata', 'gene_ids', 'data', 'doctype'])){

    deferred.reject(new Error("document did not contain required parameters (saveInstance"));

  } else {
    
    if(doc.gene_ids.length != doc.data.length) {
          deferred.reject(new Error("Gene IDs length must match zscores length"));
    }
    doc.timestamp = new Date(Date.now());
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
*  Every field in "where" is prepended with metadata, although it will
*  cope gracefully if that qualifier is already there.  As a result you 
*  can only query based on metadata.
*/
LINCS.prototype._prepare = function(select, where, skip, limit, count) {
  var _self = this;
  count = count || false;
  var q;
  
  select = select || "*";
  select = [].concat(select);
  select = select.map(function(f) {
      return("LINCS." + f);
  });

  var c = 0;
  Object.keys(where).forEach(function(k) {
    var key = "metadata." + k.replace(/metadata\\./, "");
    if(c==0) {
      if(count) {
        q = "SELECT COUNT(META().id) AS COUNT FROM LINCS " +
              "WHERE " + _self._whereStmt(key, where[[k]]);
      } else {
        q = `SELECT META().id, ${select.join(", ")} FROM LINCS `+
              "WHERE " + _self._whereStmt(key, where[[k]]);
      }
      c++;
    } else {
      q += "AND " + _self._whereStmt(key, where[[k]]);
      c++;
    }
  });

  q += `${skip ? 'OFFSET ' + skip : ''} ${limit ? ' LIMIT ' + limit : ''}`;

  // unquote numerics and booleans
  q = q.replace(/['"]true['"]/g, "true").replace(/['"]false['"]/g, "false")
       .replace(/['"]([\.\d]+)['"]/g, "$1");  
  return(this.n1ql.fromString(q));
};




/* helper function to assist in formatting where statements
 * to address capitalization issues and = vs. IN
*/
LINCS.prototype._whereStmt = function(field, val) {
  var op, rightside, leftside;
  val = [].concat(val);

  // right side
  if(val.length > 1) {
    op = "IN";
    if(field == "pert_desc") {
      rightside = JSON.stringify(val.map(function(v) {
        return(v.toLowerCase());
      }));
    } else {
      rightside = JSON.stringify(val);
    }
  } else {
    val = val[0];
    op = "=";
    if(field == "metadata.pert_desc") {
      rightside = JSON.stringify(val.toLowerCase());
    } else {
      rightside = JSON.stringify(val);
    }
  }
 
  // left side 
  if(field == "metadata.pert_desc") {
    leftside = "lower(metadata.pert_desc)";
  } else {
    leftside = field;
  }

  return(`${leftside} ${op} ${rightside} `);
};



/* helper to execute N1QL queries with error handling
*  query is a N1QL query object, promise is the promise
*  to fulfill or reject upon completion.
*/
LINCS.prototype._execute = function(query, promise) {
  this.bucket.query(
    query,
    function(err, data) {
      if(err) {
        promise.reject(err);
      } else {
        promise.resolve(data);
      }
  });
};

module.exports = exports = new LINCS();
