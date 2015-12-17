define({ "api": [
  {
    "type": "POST",
    "url": "/LINCS/pert",
    "title": "Create a perturbation data document",
    "name": "createPerturbation",
    "group": "LINCS",
    "description": "<p>Creates a data document for a particular perturbation &amp; calculation method</p> ",
    "parameter": {
      "fields": {
        "Parameter": [
          {
            "group": "Parameter",
            "type": "<p>String</p> ",
            "optional": false,
            "field": "perturbagen",
            "description": "<p>name of perturbagen</p> "
          },
          {
            "group": "Parameter",
            "type": "<p>numeric</p> ",
            "optional": false,
            "field": "dose",
            "description": "<p>dose (unitless)</p> "
          },
          {
            "group": "Parameter",
            "type": "<p>numeric</p> ",
            "optional": false,
            "field": "duration",
            "description": "<p>duration (unitless)</p> "
          },
          {
            "group": "Parameter",
            "type": "<p>String</p> ",
            "optional": false,
            "field": "cell",
            "description": "<p>cell line used</p> "
          },
          {
            "group": "Parameter",
            "type": "<p>String</p> ",
            "optional": false,
            "field": "method",
            "description": "<p>calculation method used, e.g. &quot;zsvc_plate&quot;</p> "
          },
          {
            "group": "Parameter",
            "type": "<p>boolean</p> ",
            "optional": false,
            "field": "gold",
            "description": "<p>is this a gold signature score?</p> "
          },
          {
            "group": "Parameter",
            "type": "<p>[String]</p> ",
            "optional": false,
            "field": "gene_ids",
            "description": "<p>from lincs</p> "
          },
          {
            "group": "Parameter",
            "type": "<p>[Numeric]</p> ",
            "optional": false,
            "field": "data",
            "description": "<p>the scores (one per gene)</p> "
          }
        ]
      }
    },
    "success": {
      "fields": {
        "Success 200": [
          {
            "group": "Success 200",
            "type": "<p>string</p> ",
            "optional": false,
            "field": "Id",
            "description": "<p>of created object</p> "
          }
        ]
      },
      "examples": [
        {
          "title": "Success-Response: ",
          "content": "HTTP/1.1 201 Created\n{\n id: 1\n}",
          "type": "json"
        }
      ]
    },
    "version": "0.0.0",
    "filename": "bin/app.js",
    "groupTitle": "LINCS"
  },
  {
    "type": "GET",
    "url": "/LINCS/instances/:id/controls",
    "title": "Retrieve control data",
    "name": "getControls",
    "group": "LINCS",
    "description": "<p>Retrieves the full data document for each instance on the same plate as :id whose perturbagen is the appropriate control for that instance (including time of exposure and cell line).</p> ",
    "parameter": {
      "fields": {
        "Parameter": [
          {
            "group": "Parameter",
            "type": "<p>String</p> ",
            "optional": false,
            "field": "id",
            "description": "<p>of instance for which controls are desired.</p> "
          }
        ]
      }
    },
    "success": {
      "fields": {
        "Success 200": [
          {
            "group": "Success 200",
            "type": "<p>string</p> ",
            "optional": false,
            "field": "data",
            "description": "<p>Document in JSON format</p> "
          }
        ]
      },
      "examples": [
        {
          "title": "Success-Response (note array): ",
          "content": "HTTP/1.1 200 OK\n{\n[\"gene_ids\": [\n    \"200814_at\",\n    \"222103_at\",\n    \"...truncated...\"\n    ],\n\"metadata\": {\n    \"bead_batch\": \"b3\",\n    \"bead_revision\": \"r2\",\n    \"bead_set\": \"dp52,dp53\",\n    \"cell_id\": \"HCC515\",\n    \"...truncated...\"\n},\n\"data\": [\n    9.15469932556152,\n    9.05399990081787,\n    \"...truncated...\"\n    ]\n },\n \"...truncated...\"\n]\n}",
          "type": "json"
        }
      ]
    },
    "version": "0.0.0",
    "filename": "bin/app.js",
    "groupTitle": "LINCS"
  },
  {
    "type": "POST",
    "url": "/LINCS/instances",
    "title": "Retrieve multiple instances by id",
    "name": "getInstances",
    "group": "LINCS",
    "description": "<p>Retrieves instances by id (Couchbase document id, i.e. meta.id, not pert_id).  We use POST instead of GET to get around GET query string length requirements so you can send lots of IDs.</p> ",
    "parameter": {
      "fields": {
        "Parameter": [
          {
            "group": "Parameter",
            "type": "<p>String[]</p> ",
            "optional": false,
            "field": "ids",
            "description": "<p>Ids to retrieve as a legal JSON formatted array</p> "
          }
        ]
      }
    },
    "success": {
      "fields": {
        "Success 200": [
          {
            "group": "Success 200",
            "type": "<p>string</p> ",
            "optional": false,
            "field": "instances",
            "description": "<p>Full instance data in JSON format</p> "
          }
        ]
      },
      "examples": [
        {
          "title": "Success-Response: ",
          "content": "HTTP/1.1 200 OK\n{\n}",
          "type": "json"
        }
      ]
    },
    "version": "0.0.0",
    "filename": "bin/app.js",
    "groupTitle": "LINCS"
  },
  {
    "type": "GET",
    "url": "/LINCS/sh_controls",
    "title": "Retrieve control data for shRNA",
    "name": "getShControls",
    "group": "LINCS",
    "description": "<p>Retrieves the appropriate shRNA controls</p> ",
    "version": "0.0.0",
    "filename": "bin/app.js",
    "groupTitle": "LINCS"
  },
  {
    "type": "GET",
    "url": "/LINCS/instances/:id",
    "title": "Retrieve data document by id",
    "name": "getdata",
    "group": "LINCS",
    "description": "<p>Retrieves gene ids, Q2Norm data, and metadata</p> ",
    "parameter": {
      "fields": {
        "Parameter": [
          {
            "group": "Parameter",
            "type": "<p>String</p> ",
            "optional": false,
            "field": "id",
            "description": "<p>of desired instance.</p> "
          }
        ]
      }
    },
    "success": {
      "fields": {
        "Success 200": [
          {
            "group": "Success 200",
            "type": "<p>string</p> ",
            "optional": false,
            "field": "data",
            "description": "<p>Document in JSON format</p> "
          }
        ]
      },
      "examples": [
        {
          "title": "Success-Response: ",
          "content": "HTTP/1.1 200 OK\n{\n\"gene_ids\": [\n    \"200814_at\",\n    \"222103_at\",\n    \"...truncated...\"\n    ],\n\"metadata\": {\n    \"bead_batch\": \"b3\",\n    \"bead_revision\": \"r2\",\n    \"bead_set\": \"dp52,dp53\",\n    \"cell_id\": \"HCC515\",\n    \"...truncated...\"\n},\n\"data\": [\n    9.15469932556152,\n    9.05399990081787,\n    \"...truncated...\"\n    ]\n }",
          "type": "json"
        }
      ]
    },
    "version": "0.0.0",
    "filename": "bin/app.js",
    "groupTitle": "LINCS"
  },
  {
    "type": "GET",
    "url": "/LINCS/instances/:distil_id/metadata",
    "title": "Request metadata",
    "name": "instanceMetadata",
    "group": "LINCS",
    "description": "<p>Fetch metadata for a given instance by distil_id</p> ",
    "parameter": {
      "fields": {
        "Parameter": [
          {
            "group": "Parameter",
            "type": "<p>string</p> ",
            "optional": false,
            "field": "distil_id",
            "description": "<p>Id of instance</p> "
          }
        ]
      }
    },
    "examples": [
      {
        "title": "Example usage:",
        "content": "curl localhost:8080/LINCS/instances/\\\n   CPC014_VCAP_6H_X2_F1B3_DUO52HI53LO:P05/metadata",
        "type": "curl"
      }
    ],
    "success": {
      "fields": {
        "Success 200": [
          {
            "group": "Success 200",
            "type": "<p>Object</p> ",
            "optional": false,
            "field": "metadata",
            "description": "<p>Metadats docs in JSON format</p> "
          }
        ]
      },
      "examples": [
        {
          "title": "Success-Response: ",
          "content": "HTTP/1.1 200 OK\nContent-Type: application/json\n{\n  \"metadata\": {\n    \"det_plate\": \"AML001_CD34_24H_X1_F1B10\",\n    \"is_gold\": true,\n    \"pert_vehicle\": \"DMSO\",\n    \"pert_type\": \"trt_cp\",\n    \"distil_id\": \"AML001_CD34_24H_X1_F1B10:A03\",\n    \"pert_id\": \"BRD-K49071277\",\n    \"pert_desc\": \"SECURININE\",\n    \"cell_id\": \"CD34\",\n    \"pert_time\": 24,\n    \"pert_time_unit\": \"h\",\n    \"pert_dose\": 10,\n    \"pert_dose_unit\": \"um\"\n  },\n  \"id\": \"1230884\"\n}",
          "type": "json"
        }
      ]
    },
    "version": "0.0.0",
    "filename": "bin/app.js",
    "groupTitle": "LINCS"
  },
  {
    "type": "POST",
    "url": "/LINCS/instances/query/metadata",
    "title": "Request metadata (multi)",
    "name": "instancesMetadata",
    "group": "LINCS",
    "description": "<p>Fetch metadata for given instance by distil_id</p> ",
    "parameter": {
      "fields": {
        "Parameter": [
          {
            "group": "Parameter",
            "type": "<p>string[]</p> ",
            "optional": false,
            "field": "keys",
            "description": "<p>distilIds of instances</p> "
          }
        ]
      }
    },
    "examples": [
      {
        "title": "Example usage:",
        "content": "curl -d '{\"keys\": [\"CPC014_VCAP_6H_X2_F1B3_DUO52HI53LO:P05\", \\\n      \"CPC014_VCAP_6H_X2_F1B3_DUO52HI53LO:P05\"]}' \\\n      localhost:8080/LINCS/instances/metadata \\\n      -H \"Content-Type: application/json\"",
        "type": "curl"
      }
    ],
    "success": {
      "fields": {
        "Success 200": [
          {
            "group": "Success 200",
            "type": "<p>Object</p> ",
            "optional": false,
            "field": "metadata",
            "description": "<p>Metadats docs in JSON format</p> "
          }
        ]
      },
      "examples": [
        {
          "title": "Success-Response: ",
          "content": "HTTP/1.1 200 OK\nContent-Type: application/json\n[  \n{\n   \"key\": \"CPC014_VCAP_6H_X2_F1B3_DUO52HI53LO:P05\",\n   \"metadata\": {\n     \"det_plate\": \"CPC014_VCAP_6H_X2_F1B3_DUO52HI53LO\",\n     \"is_gold\": true,\n     \"pert_vehicle\": \"DMSO\",\n     \"pert_type\": \"trt_cp\",\n     \"distil_id\": \"CPC014_VCAP_6H_X2_F1B3_DUO52HI53LO:P05\",\n     \"pert_id\": \"BRD-A64228451\",\n     \"pert_desc\": \"EI-328\",\n     \"cell_id\": \"VCAP\",\n     \"pert_time\": 6,\n     \"pert_time_unit\": \"h\",\n     \"pert_dose\": 10,\n     \"pert_dose_unit\": \"um\"\n   },\n   \"id\": \"312240\"\n } \n... // truncated\n]",
          "type": "json"
        }
      ]
    },
    "version": "0.0.0",
    "filename": "bin/app.js",
    "groupTitle": "LINCS"
  },
  {
    "type": "POST",
    "url": "/LINCS/instances",
    "title": "Save instance data to server",
    "name": "postInstance",
    "group": "LINCS",
    "description": "<p>Stores instance (e.g. level 2 data) on the server. Do NOT use this endpoint to save calculated scores (use /LINCS/pert for that).</p> ",
    "parameter": {
      "fields": {
        "Parameter": [
          {
            "group": "Parameter",
            "type": "<p>String</p> ",
            "optional": false,
            "field": "id",
            "description": "<p>desired id (key) of document</p> "
          },
          {
            "group": "Parameter",
            "type": "<p>String</p> ",
            "optional": false,
            "field": "metadata",
            "description": "<p>JSON from lincs</p> "
          },
          {
            "group": "Parameter",
            "type": "<p>String</p> ",
            "optional": false,
            "field": "type",
            "description": "<p>Type of data, e.g. &quot;q2norm&quot;</p> "
          },
          {
            "group": "Parameter",
            "type": "<p>[String]</p> ",
            "optional": false,
            "field": "gene_ids",
            "description": "<p>from lincs</p> "
          },
          {
            "group": "Parameter",
            "type": "<p>[Numeric]</p> ",
            "optional": false,
            "field": "data",
            "description": "<p>the expression data</p> "
          }
        ]
      }
    },
    "success": {
      "fields": {
        "Success 200": [
          {
            "group": "Success 200",
            "type": "<p>string</p> ",
            "optional": false,
            "field": "response",
            "description": "<p>response</p> "
          }
        ]
      },
      "examples": [
        {
          "title": "Success-Response: ",
          "content": "HTTP/1.1 200 OK\n{\n}",
          "type": "json"
        }
      ]
    },
    "version": "0.0.0",
    "filename": "bin/app.js",
    "groupTitle": "LINCS"
  },
  {
    "type": "GET",
    "url": "/LINCS/instances",
    "title": "query the instances in various ways",
    "name": "queryInstances",
    "group": "LINCS",
    "description": "<p>submit query as query string (?q={}) in JSON format. Supported queries To Be Determined.</p> ",
    "parameter": {
      "fields": {
        "Parameter": [
          {
            "group": "Parameter",
            "type": "<p>String</p> ",
            "optional": false,
            "field": "q",
            "description": "<p>The query parameters in JSON format</p> "
          },
          {
            "group": "Parameter",
            "type": "<p>String</p> ",
            "optional": false,
            "field": "limit",
            "description": "<p>Number of docs to return, default 1000</p> "
          },
          {
            "group": "Parameter",
            "type": "<p>String</p> ",
            "optional": false,
            "field": "skip",
            "description": "<p>Number of docs to skip (for paging), default 0</p> "
          },
          {
            "group": "Parameter",
            "type": "<p>String</p> ",
            "optional": false,
            "field": "cell_line",
            "description": "<p>Cell line of interest (optional)</p> "
          },
          {
            "group": "Parameter",
            "type": "<p>String</p> ",
            "optional": false,
            "field": "pert",
            "description": "<p>perturbagen (pert_desc) of interest (optional)</p> "
          },
          {
            "group": "Parameter",
            "type": "<p>String</p> ",
            "optional": false,
            "field": "dose",
            "description": "<p>dose of interest (unitless, optional)</p> "
          },
          {
            "group": "Parameter",
            "type": "<p>String</p> ",
            "optional": false,
            "field": "duration",
            "description": "<p>duration of interest (unitless, optional)</p> "
          },
          {
            "group": "Parameter",
            "type": "<p>Logical</p> ",
            "optional": false,
            "field": "gold",
            "description": "<p>; * @apiParam {String}iSuccess {string} instances Instance ids  in JSON format</p> "
          }
        ]
      }
    },
    "success": {
      "examples": [
        {
          "title": "Success-Response: ",
          "content": "HTTP/1.1 200 OK\n{\n}",
          "type": "json"
        }
      ]
    },
    "version": "0.0.0",
    "filename": "bin/app.js",
    "groupTitle": "LINCS"
  },
  {
    "type": "POST",
    "url": "/LINCS/data Post ids (either primary or view) and retrieve",
    "title": "documents",
    "name": "retrieveData",
    "group": "LINCS",
    "description": "<p>Retrieves multiple docs by primary or view id</p> ",
    "parameter": {
      "fields": {
        "Parameter": [
          {
            "group": "Parameter",
            "type": "<p>[String]</p> ",
            "optional": false,
            "field": "keys",
            "description": "<p>List of primary or view keys</p> "
          },
          {
            "group": "Parameter",
            "type": "<p>String</p> ",
            "optional": false,
            "field": "view",
            "description": "<p>Name of view.  If missing will use primary index</p> "
          }
        ]
      }
    },
    "success": {
      "fields": {
        "Success 200": [
          {
            "group": "Success 200",
            "type": "<p>string</p> ",
            "optional": false,
            "field": "Id",
            "description": "<p>of created object</p> "
          }
        ]
      },
      "examples": [
        {
          "title": "Success-Response: ",
          "content": "HTTP/1.1 200 OK\n{\n ...\n}",
          "type": "json"
        }
      ]
    },
    "version": "0.0.0",
    "filename": "bin/app.js",
    "groupTitle": "LINCS"
  },
  {
    "type": "get",
    "url": "/LINCS",
    "title": "Get some documentation on endpoints.",
    "name": "Root",
    "group": "Root",
    "success": {
      "fields": {
        "Success 200": [
          {
            "group": "Success 200",
            "type": "<p>string</p> ",
            "optional": false,
            "field": "response",
            "description": "<p>Some useful information.</p> "
          }
        ]
      }
    },
    "version": "0.0.0",
    "filename": "bin/app.js",
    "groupTitle": "Root"
  },
  {
    "type": "get",
    "url": "/",
    "title": "Root.  Verify server up and receive useful advice.",
    "name": "Root",
    "group": "Root",
    "examples": [
      {
        "title": "Example usage:",
        "content": "curl http://localhost:8080/",
        "type": "curl"
      }
    ],
    "success": {
      "fields": {
        "Success 200": [
          {
            "group": "Success 200",
            "type": "<p>string</p> ",
            "optional": false,
            "field": "response",
            "description": ""
          }
        ]
      },
      "examples": [
        {
          "title": "Success-Response: ",
          "content": "HTTP/1.1 200 OK\nOK.  Send GET to /LINCS for further documentation.",
          "type": "json"
        }
      ]
    },
    "version": "0.0.0",
    "filename": "bin/app.js",
    "groupTitle": "Root"
  }
] });